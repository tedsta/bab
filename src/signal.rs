#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "alloc")]
use alloc::sync::Arc;

use core::sync::atomic::{AtomicBool, Ordering};

use crate::waiter_queue::{Fulfillment, WaiterQueue};

pub struct Signal {
    is_notified: AtomicBool,
    waiter_queue: WaiterQueue<()>,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            is_notified: AtomicBool::new(false),
            waiter_queue: WaiterQueue::new(),
        }
    }

    pub fn is_notified(&self) -> bool {
        self.is_notified.load(Ordering::Relaxed)
    }

    pub fn notify(&self) {
        self.is_notified.store(true, Ordering::Relaxed);
        self.waiter_queue.lock().notify_all(());
    }

    pub async fn wait(&self) {
        let waiter = core::pin::pin!(self.waiter_queue.wait());
        core::future::poll_fn(move |cx| {
            waiter.as_ref().poll_fulfillment(cx, || {
                if self.is_notified() {
                    Some(Fulfillment { inner: (), count: 1 })
                } else {
                    None
                }
            })
        })
        .await;
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    pub async fn wait_arc(self: Arc<Self>) {
        self.wait().await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_signal() {
        use std::rc::Rc;

        let ex = async_executor::LocalExecutor::new();

        pollster::block_on(ex.run(async {
            let waiter_count = 4;
            let task_starts = Rc::new(async_unsync::semaphore::Semaphore::new(0));
            let task_completes = Rc::new(async_unsync::semaphore::Semaphore::new(0));
            let signal = Rc::new(Signal::new());

            for _ in 0..waiter_count {
                let task_starts = task_starts.clone();
                let task_completes = task_completes.clone();
                let signal = signal.clone();
                ex.spawn(async move {
                    task_starts.add_permits(1);
                    signal.wait().await;
                    task_completes.add_permits(1);
                })
                .detach();
            }

            for _ in 0..waiter_count {
                task_starts.acquire().await.unwrap().forget();
            }
            assert!(task_starts.try_acquire().is_err());

            signal.notify();

            for _ in 0..waiter_count {
                task_completes.acquire().await.unwrap().forget();
            }
            assert!(task_completes.try_acquire().is_err());
        }));
    }
}