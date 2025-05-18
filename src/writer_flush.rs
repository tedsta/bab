// A niche concurrent linked-list datastructure to track write/flush progress on a set of buffers.
// - Multi producer, single consumer
// - Producers only append to the end of the list.
// - Single consumer consumes the entire list each time it receives.
//     - After unlinking a buffer from the list, the consumer sets a bit on the buffer's
//       `write_cursor` indicating that the buffer was flushed and should be re-added to the flush
//       list if any subsequent writes occur.

use core::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
};

#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "alloc")]
use alloc::sync::Arc;

use crossbeam_utils::{Backoff, CachePadded};
use spin::Mutex;

use crate::{
    buffer::BufferPtr,
    thread_local::ThreadLocal,
    BufferChain,
    Packet,
};

pub const WRITE_CURSOR_FLUSHED_FLAG: u32 = 0x8000_0000;
pub const WRITE_CURSOR_DONE:         u32 = 0x4000_0000;
pub const WRITE_CURSOR_MASK:         u32 = 0x3FFF_FFFF;

pub fn new_writer_flusher() -> (WriterFlushSender, WriterFlushReceiver) {
    let shared = Arc::new(Mutex::new(WriterFlushShared {
        head_tail: None,
        waker: None,
    }));
    let writer_flush_sender = WriterFlushSender {
        shared: shared.clone(),
        local_chain: Arc::new(ThreadLocal::new()),
    };
    let writer_flush_receiver = WriterFlushReceiver::new(shared);

    (writer_flush_sender, writer_flush_receiver)
}

struct WriterFlushShared {
    head_tail: Option<(BufferPtr, BufferPtr)>,
    waker: Option<Waker>,
}

impl Drop for WriterFlushShared {
    fn drop(&mut self) {
        let mut release_head = self.head_tail.take().map(|(head, _tail)| head);

        while let Some(buffer) = release_head {
            // SAFETY: we have exclusive access until we set `WRITE_CURSOR_FLUSHED_FLAG` on the
            // buffer's write_cursor, which we don't do in this case since no future flushes can
            // occur.
            release_head = unsafe { buffer.swap_next(None) };

            unsafe {
                *buffer.flush_cursor_mut() = 0;
                buffer.receive(1);
                buffer.release_ref(1);
            }
        }
    }
}

#[derive(Clone)]
pub struct WriterFlushSender {
    shared: Arc<Mutex<WriterFlushShared>>,
    local_chain: Arc<ThreadLocal<CachePadded<BufferChain>>>,
}

impl WriterFlushSender {
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

    pub(crate) fn advance_write_cursor(
        &self,
        buffer: BufferPtr,
        write_start: u32,
        new_write_cursor: u32,
    ) {
        let backoff = Backoff::new();
        let mut write_cursor = buffer.write_cursor().load(Ordering::Acquire);
        while write_cursor & WRITE_CURSOR_MASK != write_start {
            backoff.snooze();
            write_cursor = buffer.write_cursor().load(Ordering::Acquire);
        }
        loop {
            match buffer.write_cursor().compare_exchange(
                write_cursor,
                new_write_cursor,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(modified_write_cursor) => {
                    // Must've been modified the the flusher
                    write_cursor = modified_write_cursor;
                    assert_eq!(write_cursor & WRITE_CURSOR_MASK, write_start);
                }
            }
        }

        if write_start == 0 || write_cursor & WRITE_CURSOR_FLUSHED_FLAG != 0 {
            // This is the first write since the buffer was last flushed - add it to the flush
            // queue.
            let local_chain = self.local_chain.get_or_default();
            local_chain.push(buffer);
        }
    }

    /// Enqueue a complete buffer to be sent toward the WriterFlushReceiver upon the next call to
    /// `flush`.
    ///
    /// The buffer must previously have been been marked via `mark_complete_buffer`.
    pub fn send_complete_buffer(&self, buffer: BufferPtr) {
        let flush_cursor = unsafe { buffer.flush_cursor_mut() };
        let len = core::mem::replace(flush_cursor, 0);

        buffer.write_cursor().store(len | WRITE_CURSOR_DONE, Ordering::Release);

        let local_chain = self.local_chain.get_or_default();
        local_chain.push(buffer);
    }

    /// Prepare a buffer to be flushed without actually flushing it.
    ///
    /// This is a low-level operation that allows preparing written buffers externally (not
    /// using a `bab::Writer`) and sending them to the WriterFlushReceiver via
    /// `send_complete_buffer`.
    pub fn mark_complete_buffer(buffer: BufferPtr, len: u32) {
        // A bit of a hack - since we will only be sending this buffer to a flush receiver once, for
        // we have exclusive access to its flush_cursor and use it to store the buffer's wrtten
        // length so that users don't have to track it separately.
        // We just need to be sure to reset flush_cursor to 0 before it gets sent to the flush
        // receiver. We do this in `send_complete_buffer`.
        let flush_cursor = unsafe { buffer.flush_cursor_mut() };
        *flush_cursor = len;
    }

    /// Get the written length of a complete buffer previously marked via `mark_complete_buffer`.
    pub fn get_complete_buffer_len(buffer: BufferPtr) -> u32 {
        let flush_cursor = unsafe { buffer.flush_cursor_mut() };
        *flush_cursor
    }
}

struct WriterFlushQueueReceive<'a> {
    shared: &'a Mutex<WriterFlushShared>,
}

impl core::future::Future for WriterFlushQueueReceive<'_> {
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

impl Drop for WriterFlushQueueReceive<'_> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock();
        shared.waker = None;
    }
}

impl Drop for WriterFlushSender {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct WriterFlushReceiver {
    shared: Arc<Mutex<WriterFlushShared>>,
}

pub struct Flush {
    buffer: BufferPtr,
    writer_id: usize,
    offset: usize,
    len: usize,
    release_buffer: bool,
    _not_send: core::marker::PhantomData<*const ()>,
}

impl WriterFlushReceiver {
    fn new(
        shared: Arc<Mutex<WriterFlushShared>>,
    ) -> Self {
        Self {
            shared,
        }
    }

    pub async fn flush(&mut self) -> FlushIterator {
        let recv_head = WriterFlushQueueReceive { shared: &self.shared }.await;
        FlushIterator { head: Some(recv_head) }
    }
}

pub struct FlushIterator {
    head: Option<BufferPtr>,
}

impl core::iter::Iterator for FlushIterator {
    type Item = Flush;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(buffer) = self.head {
            // Note: it's important that this is done *before* fetch_or'ing the write_cursor while
            // we still have exclusive access.
            self.head = unsafe { buffer.swap_next(None) };

            let write_cursor = buffer.write_cursor()
                .fetch_or(WRITE_CURSOR_FLUSHED_FLAG, Ordering::AcqRel);
            let writer_id = unsafe { buffer.writer_id() };

            let flush_cursor = unsafe { buffer.flush_cursor_mut() };
            let buffer_is_done = (write_cursor & WRITE_CURSOR_DONE) != 0;
            let write_cursor = write_cursor & WRITE_CURSOR_MASK;

            debug_assert!(write_cursor > 0);

            if *flush_cursor < write_cursor {
                let offset = *flush_cursor as usize;
                let len = (write_cursor - *flush_cursor) as usize;
                *flush_cursor = write_cursor;

                return Some(Flush {
                    buffer,
                    writer_id,
                    offset,
                    len,
                    release_buffer: buffer_is_done,
                    _not_send: core::marker::PhantomData,
                });
            } else if buffer_is_done {
                debug_assert_eq!(*flush_cursor, write_cursor);
                *flush_cursor = 0;
                unsafe { buffer.receive(1); }
                unsafe { buffer.release_ref(1); }
            }
        }

        None
    }
}

impl Drop for FlushIterator {
    fn drop(&mut self) {
        while self.next().is_some() { }
    }
}

impl Flush {
    pub fn len(&self) -> usize { self.len }

    pub fn writer_id(&self) -> usize { self.writer_id }
}

impl core::ops::Deref for Flush {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.buffer.data().add(self.offset),
                self.len,
            )
        }
    }
}

impl Drop for Flush {
    fn drop(&mut self) {
        if self.release_buffer {
            unsafe {
                *self.buffer.flush_cursor_mut() = 0;
                self.buffer.receive(1);
                self.buffer.release_ref(1);
            }
        }
    }
}

impl From<Flush> for Packet {
    fn from(flush: Flush) -> Self {
        if flush.release_buffer {
            unsafe {
                *flush.buffer.flush_cursor_mut() = 0;
                flush.buffer.receive(1);
            }
        } else {
            // In theory we could convert this to a regular `take_ref` if we `receive`'d the buffer
            // the first time the flush receiver encounters the buffer rather than just before
            // releasing its buffer reference. But doing that greatly complicates the shutdown
            // logic, (mainly releasing unflushed buffers at the senders) and I haven't been able to
            // find a reasonable solution yet.
            unsafe { flush.buffer.take_shared_ref(1); }
        }

        let packet = unsafe {
            Self::new(
                flush.buffer,
                flush.offset as usize,
                flush.len as usize,
            )
        };

        core::mem::forget(flush);

        packet
    }
}
