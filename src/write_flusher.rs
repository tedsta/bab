use core::sync::atomic::Ordering;

use crate::{
    buffer::BufferPtr,
    writer::WriterFlushQueue,
};

pub struct WriteFlusher {
    flush_queue: WriterFlushQueue,
}

pub struct Flush {
    buffer: BufferPtr,
    writer_id: usize,
    offset: usize,
    len: usize,
    release_buffer: bool,
    _not_send: core::marker::PhantomData<*const ()>,
}

impl WriteFlusher {
    pub fn new(
        flush_queue: WriterFlushQueue,
    ) -> Self {
        Self {
            flush_queue,
        }
    }

    pub async fn flush(&mut self) -> FlushIterator {
        let recv_head = self.flush_queue.receive().await;
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
                .fetch_or(crate::writer::WRITE_CURSOR_FLUSHED_FLAG, Ordering::AcqRel);
            let writer_id = buffer.writer_id().load(Ordering::Relaxed);

            let flush_cursor = unsafe { buffer.flush_cursor_mut() };
            let buffer_is_done = (write_cursor & crate::writer::WRITE_CURSOR_DONE) != 0;
            let write_cursor = write_cursor & crate::writer::WRITE_CURSOR_MASK;

            debug_assert!(write_cursor > 0);

            if *flush_cursor == 0 {
                unsafe { buffer.receive(1); }
            }

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
                self.buffer.release_ref(1);
            }
        }
    }
}
