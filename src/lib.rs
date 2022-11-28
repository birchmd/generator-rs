//! Provides functionality similar to
//! https://doc.rust-lang.org/beta/unstable-book/language-features/generators.html
//! built on top of the tokio library.
//! See the tests for example usage.

use std::fmt::Debug;
use std::future::Future;
use tokio::runtime::{self, Runtime};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::{self, JoinHandle};

/// Describes the result of executing a generator.
/// The computation can yield with an intermediate value and
/// a handle for continuing the computation;
/// or it can return a final value because the computation is complete.
pub enum GeneratorResult<Y, R> {
    Yielded {
        value: Y,
        continuation: JoinHandle<()>,
    },
    Returned(R),
}

/// An object implementing the `yield` and `return` functionality in generators.
pub struct GeneratorHelper<I, Y, R> {
    yield_channel: UnboundedSender<Y>,
    resume_channel: UnboundedReceiver<I>,
    return_channel: UnboundedSender<R>,
}

impl<I, Y: Debug, R: Debug> GeneratorHelper<I, Y, R> {
    /// Yields control from this generator.
    /// Resumes after receiving input `I` from the other process.
    pub async fn do_yield(&mut self, y: Y) -> I {
        self.yield_channel.send(y).unwrap();
        self.resume_channel.recv().await.unwrap()
    }

    pub fn do_return(self, r: R) {
        self.return_channel.send(r).unwrap()
    }
}

/// Object containing necessary state for running a generator.
pub struct Generator<I, Y, R> {
    yield_channel: UnboundedReceiver<Y>,
    resume_channel: UnboundedSender<I>,
    return_chanel: UnboundedReceiver<R>,
    local: task::LocalSet,
}

impl<I, Y, R> Generator<I, Y, R>
where
    I: Debug,
{
    pub fn new() -> (Self, GeneratorHelper<I, Y, R>) {
        let (yield_sender, yield_receiver) = mpsc::unbounded_channel();
        let (resume_sender, resume_receiver) = mpsc::unbounded_channel();
        let (return_sender, return_receiver) = mpsc::unbounded_channel();
        let helper = GeneratorHelper {
            yield_channel: yield_sender,
            resume_channel: resume_receiver,
            return_channel: return_sender,
        };
        let generator = Self {
            yield_channel: yield_receiver,
            resume_channel: resume_sender,
            return_chanel: return_receiver,
            local: task::LocalSet::new(),
        };
        (generator, helper)
    }

    /// Start the given logic as a generator.
    /// The closure contains the logic to execute, and has a helper in scope to allow
    /// yielding and retuning.
    pub async fn start<G, F>(
        &mut self,
        logic: G,
        helper: GeneratorHelper<I, Y, R>,
    ) -> GeneratorResult<Y, R>
    where
        G: FnOnce(GeneratorHelper<I, Y, R>) -> F,
        F: Future<Output = ()> + 'static,
    {
        let yield_channel = &mut self.yield_channel;
        let return_channel = &mut self.return_chanel;
        let local = &mut self.local;
        local
            .run_until(async move {
                let continuation = task::spawn_local((logic)(helper));
                Self::await_yield_or_return(yield_channel, return_channel, continuation).await
            })
            .await
    }

    /// Resume the generator using the given continuation (obtained from a prior yield result),
    /// and the given input.
    pub async fn resume(
        &mut self,
        continuation: JoinHandle<()>,
        input: I,
    ) -> GeneratorResult<Y, R> {
        self.resume_channel.send(input).unwrap();
        let yield_channel = &mut self.yield_channel;
        let return_channel = &mut self.return_chanel;
        let local = &mut self.local;
        local
            .run_until(async move {
                Self::await_yield_or_return(yield_channel, return_channel, continuation).await
            })
            .await
    }

    async fn await_yield_or_return(
        yield_channel: &mut UnboundedReceiver<Y>,
        return_channel: &mut UnboundedReceiver<R>,
        continuation: JoinHandle<()>,
    ) -> GeneratorResult<Y, R> {
        tokio::select! {
            biased;
            // Return channel needs to be polled first because the yield channel
            // will be dropped when the future completes.
            r = return_channel.recv() => {
                // Something on the return channel should mean the future is complete
                continuation.await.unwrap();
                GeneratorResult::Returned(r.unwrap())
            }
            y = yield_channel.recv() => {
                let value = y.unwrap();
                GeneratorResult::Yielded { value, continuation }
            }
        }
    }
}

/// Provides a synchronous interface for generators by having an internal `Runtime`
/// to execute the async code.
pub struct SyncGenerator {
    rt: Runtime,
}

impl Default for SyncGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncGenerator {
    pub fn new() -> Self {
        let rt = runtime::Builder::new_current_thread().build().unwrap();
        Self { rt }
    }

    pub fn start<I, Y, R, G, F>(&self, logic: G) -> (Generator<I, Y, R>, GeneratorResult<Y, R>)
    where
        G: FnOnce(GeneratorHelper<I, Y, R>) -> F,
        F: Future<Output = ()> + 'static,
        I: Debug,
        Y: Debug,
        R: Debug,
    {
        let (mut generator, helper) = Generator::new();
        let result = self
            .rt
            .block_on(async { generator.start(logic, helper).await });
        (generator, result)
    }

    pub fn resume<I, Y, R>(
        &self,
        generator: &mut Generator<I, Y, R>,
        continuation: JoinHandle<()>,
        input: I,
    ) -> GeneratorResult<Y, R>
    where
        I: Debug,
        Y: Debug,
        R: Debug,
    {
        self.rt
            .block_on(async { generator.resume(continuation, input).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_without_existing_runtime() {
        let sync_cr = SyncGenerator::new();
        println!("Hello from the main process");
        let (mut generator, result) = sync_cr.start(
            |mut helper: GeneratorHelper<u32, &'static str, u64>| async move {
                println!("Hello from the generator");
                let number = helper.do_yield("Started!").await;
                println!("Picking up where we left off.");
                let next_number = helper.do_yield("Continued!").await;
                helper.do_return((number + next_number) as u64);
            },
        );

        let continuation = match result {
            GeneratorResult::Yielded {
                value,
                continuation,
            } if value == "Started!" => continuation,
            _ => panic!("Unexpected result"),
        };
        println!("Hello again from the main process");
        let result = sync_cr.resume(&mut generator, continuation, 3);
        let continuation = match result {
            GeneratorResult::Yielded {
                value,
                continuation,
            } if value == "Continued!" => continuation,
            _ => panic!("Unexpected result"),
        };
        let result = sync_cr.resume(&mut generator, continuation, 5);
        match result {
            GeneratorResult::Returned(x) => assert_eq!(x, 8),
            _ => panic!("Unexpected result"),
        }
    }

    #[tokio::test]
    async fn test_with_existing_runtime() {
        println!("Hello from the main process");
        let (mut generator, helper) = Generator::new();
        let result = generator
            .start(
                |mut helper: GeneratorHelper<u32, &'static str, u64>| async move {
                    println!("Hello from the generator");
                    let number = helper.do_yield("Started!").await;
                    println!("Picking up where we left off.");
                    let next_number = helper.do_yield("Continued!").await;
                    helper.do_return((number + next_number) as u64);
                },
                helper,
            )
            .await;

        let continuation = match result {
            GeneratorResult::Yielded {
                value,
                continuation,
            } if value == "Started!" => continuation,
            _ => panic!("Unexpected result"),
        };
        println!("Hello again from the main process");
        let result = generator.resume(continuation, 3).await;
        let continuation = match result {
            GeneratorResult::Yielded {
                value,
                continuation,
            } if value == "Continued!" => continuation,
            _ => panic!("Unexpected result"),
        };
        let result = generator.resume(continuation, 5).await;
        match result {
            GeneratorResult::Returned(x) => assert_eq!(x, 8),
            _ => panic!("Unexpected result"),
        }
    }
}
