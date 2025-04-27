use core::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "alloc")]
use alloc::sync::Arc;

use crossbeam_utils::CachePadded;
use spin::Mutex;

use crate::{
    buffer::BufferPtr,
    buffer_chain::BufferChain,
    thread_local::ThreadLocal,
};

pub fn buffer_queue() -> (BufferQueueSender, BufferQueueReceiver) {
    let shared = Arc::new(Mutex::new(BufferQueueShared {
        head_tail: None,
        waker: None,
    }));
    let sender = BufferQueueSender {
        shared: shared.clone(),
        local_chain: Arc::new(ThreadLocal::new()),
    };
    let receiver = BufferQueueReceiver::new(shared);

    (sender, receiver)
}

struct BufferQueueShared {
    head_tail: Option<(BufferPtr, BufferPtr)>,
    waker: Option<Waker>,
}

#[derive(Clone)]
pub struct BufferQueueSender {
    shared: Arc<Mutex<BufferQueueShared>>,
    local_chain: Arc<ThreadLocal<CachePadded<BufferChain>>>,
}

impl BufferQueueSender {
    pub fn push(&self, buffer: BufferPtr) {
        let local_chain = self.local_chain.get_or_default();
        local_chain.push(buffer);
    }

    pub fn flush(&self) {
        let local_chain = self.local_chain.get_or_default();

        let Some((head, tail)) = local_chain.take_all() else {
            return;
        };

        let mut shared = self.shared.lock();
        if let Some((_, prev_shared_tail)) = &mut shared.head_tail {
            unsafe { prev_shared_tail.set_next(Some(head)); }
            *prev_shared_tail = tail;
        } else {
            shared.head_tail = Some((head, tail));
            if let Some(waker) = &shared.waker {
                waker.wake_by_ref();
            }
        }
    }
}

impl Drop for BufferQueueSender {
    fn drop(&mut self) {
        self.flush();
    }
}

struct BufferQueueReceive<'a> {
    shared: &'a Mutex<BufferQueueShared>,
}

impl core::future::Future for BufferQueueReceive<'_> {
    type Output = BufferPtr;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.shared.lock();
        if let Some((head, _tail)) = shared.head_tail.take() {
            shared.waker = None;
            Poll::Ready(head)
        } else {
            let new_waker = cx.waker();
            if let Some(existing_waker) = &shared.waker {
                if !existing_waker.will_wake(new_waker) {
                    shared.waker = Some(new_waker.clone());
                }
            } else {
                shared.waker = Some(new_waker.clone());
            }

            Poll::Pending
        }
    }
}

impl Drop for BufferQueueReceive<'_> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock();
        shared.waker = None;
    }
}

#[derive(Clone)]
pub struct BufferQueueReceiver {
    shared: Arc<Mutex<BufferQueueShared>>,
}

impl BufferQueueReceiver {
    fn new(
        shared: Arc<Mutex<BufferQueueShared>>,
    ) -> Self {
        Self {
            shared,
        }
    }

    pub async fn recv(&self) -> BufferQueueReceiveIterator {
        let recv_head = BufferQueueReceive { shared: &self.shared }.await;
        BufferQueueReceiveIterator { head: Some(recv_head) }
    }
}

pub struct BufferQueueReceiveIterator {
    head: Option<BufferPtr>,
}

impl core::iter::Iterator for BufferQueueReceiveIterator {
    type Item = BufferPtr;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(buffer) = self.head {
            self.head = unsafe { buffer.swap_next(None) };
            Some(buffer)
        } else {
            None
        }
    }
}

impl Drop for BufferQueueReceiveIterator {
    fn drop(&mut self) {
        while self.next().is_some() { }
    }
}

