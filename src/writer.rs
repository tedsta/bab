use core::{
    cell::Cell,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
};
use std::sync::Arc;

use crossbeam_utils::{Backoff, CachePadded};
use spin::Mutex;

use crate::{
    buffer::BufferPtr,
    thread_local::ThreadLocal,
    waiter_queue::{Fulfillment, WaiterQueue},
    HeapBufferPool,
};

pub const WRITE_CURSOR_FLUSHED_FLAG: u32 = 0x8000_0000;
pub const WRITE_CURSOR_DONE:         u32 = 0x4000_0000;
pub const WRITE_CURSOR_MASK:         u32 = 0x3FFF_FFFF;


#[derive(Default)]
struct WriterFlusherLocal {
    head_tail: Cell<Option<(BufferPtr, BufferPtr)>>,
}

struct WriterFlushQueueInner {
    head_tail: Option<(BufferPtr, BufferPtr)>,
    waker: Option<Waker>,
}

#[derive(Clone)]
pub struct WriterFlushQueue {
    inner: Arc<Mutex<WriterFlushQueueInner>>,
}

impl WriterFlushQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(WriterFlushQueueInner {
                head_tail: None,
                waker: None,
            })),
        }
    }

    pub fn send(&self, append_head: BufferPtr, append_tail: BufferPtr) {
        let mut flush_queue = self.inner.lock();
        if let Some((_, prev_shared_tail)) = &mut flush_queue.head_tail {
            unsafe { prev_shared_tail.set_next(Some(append_head)); }
            *prev_shared_tail = append_tail;
        } else {
            flush_queue.head_tail = Some((append_head, append_tail));
            if let Some(waker) = &flush_queue.waker {
                waker.wake_by_ref();
            }
        }
    }

    pub async fn receive(&self) -> BufferPtr {
        WriterFlushQueueReceive { flush_queue: self }.await
    }

    pub fn try_receive(&self) -> Option<BufferPtr> {
        let mut flush_queue = self.inner.lock();
        flush_queue.try_receive()
    }
}

impl WriterFlushQueueInner {
    pub fn try_receive(&mut self) -> Option<BufferPtr> {
        self.head_tail.take().map(|(head, _tail)| head)
    }
}

impl Drop for WriterFlushQueueInner {
    fn drop(&mut self) {
        let mut recv_head = self.try_receive();

        while let Some(buffer) = recv_head {
            // SAFETY: we have exclusive access until we set `WRITE_CURSOR_FLUSHED_FLAG` on the
            // buffer's write_cursor, which we don't do in this case since no future flushes can
            // occur.
            recv_head = unsafe { buffer.swap_next(None) };

            let flush_cursor = unsafe { buffer.flush_cursor_mut() };

            if *flush_cursor == 0 {
                unsafe { buffer.receive(1); }
            }

            *flush_cursor = 0;
            unsafe { buffer.release_ref(1); }
        }
    }
}

struct WriterFlushQueueReceive<'a> {
    flush_queue: &'a WriterFlushQueue,
}

impl core::future::Future for WriterFlushQueueReceive<'_> {
    type Output = BufferPtr;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut flush_queue = self.flush_queue.inner.lock();
        if let Some((head, _tail)) = flush_queue.head_tail.take() {
            flush_queue.waker = None;
            Poll::Ready(head)
        } else {
            let new_waker = cx.waker();
            if let Some(existing_waker) = &flush_queue.waker {
                if !existing_waker.will_wake(new_waker) {
                    flush_queue.waker = Some(new_waker.clone());
                }
            } else {
                flush_queue.waker = Some(new_waker.clone());
            }

            Poll::Pending
        }
    }
}

impl Drop for WriterFlushQueueReceive<'_> {
    fn drop(&mut self) {
        let mut flush_queue = self.flush_queue.inner.lock();
        flush_queue.waker = None;
    }
}

// A niche concurrent linked-list datastructure to track write/flush progress on a set of buffers.
// - Multi producer, single consumer
// - Writers only append to the end of the list.
// - Single consumer is free to remove items anywhere in the list.
//     - After unlinking a buffer from the list, the consumer sets a bit on the buffer's
//       `write_cursor` indicating that the buffer was flushed and should be re-added to the flush
//       list if any subsequent writes occur.
pub struct WriterFlusher {
    flush_queue: WriterFlushQueue,
    local: ThreadLocal<CachePadded<WriterFlusherLocal>>,
}

impl WriterFlusher {
    pub fn new(flush_queue: WriterFlushQueue) -> Self {
        Self {
            flush_queue,
            local: ThreadLocal::new(),
        }
    }

    pub fn flush(&self) {
        let local = self.local.get_or_default();

        let Some((local_head, local_tail)) = local.head_tail.replace(None) else {
            return;
        };

        self.flush_queue.send(local_head, local_tail);
    }

    pub fn advance_write_cursor(&self, buffer: BufferPtr, write_start: u32, new_write_cursor: u32) {
        // Wait for previous writes to finish.
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

        if write_start == 0 {
            assert!(write_cursor & WRITE_CURSOR_FLUSHED_FLAG == 0);
        }

        if write_start == 0 || write_cursor & WRITE_CURSOR_FLUSHED_FLAG != 0 {
            // This is the first write since the buffer was last flushed - add it to the flush
            // queue.
            let local = self.local.get_or_default();
            if let Some((prev_head, prev_tail)) = local.head_tail.get() {
                unsafe { prev_tail.set_next(Some(buffer)); }
                local.head_tail.set(Some((prev_head, buffer)));
            } else {
                local.head_tail.set(Some((buffer, buffer)));
            }
        }
    }
}

impl Drop for WriterFlusher {
    fn drop(&mut self) {
        self.flush();
    }
}

pub type DynWriter = Writer<dyn sealed::WriterCursor>;
pub type SharedWriter = Writer<SharedCursor>;
pub type LocalWriter = Writer<LocalCursor>;

pub struct WriterInner<Cursor: sealed::WriterCursor + ?Sized> {
    buffer_pool: HeapBufferPool,
    flusher: WriterFlusher,
    switch_buffer_waiters: WaiterQueue<()>,
    cursor: Cursor,
}

pub struct Writer<Cursor: sealed::WriterCursor + ?Sized> {
    inner: Arc<WriterInner<Cursor>>,
    writer_id: usize,
}

const CURSOR_INIT: u64        = 0x8000_0000_0000_0000;
const CLAIM_CURSOR_INIT: u64  = 0x4000_0000_0000_0000;
const CURSOR_BUF_MASK: u64    = 0x0FFF_FFFF_0000_0000;
const CURSOR_OFFSET_MASK: u64 = 0x0000_0000_0000_FFFF;

const CURSOR_BUF_SHIFT: u64   = 32;

impl<Cursor: sealed::WriterCursor + ?Sized> Clone for Writer<Cursor> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            writer_id: self.writer_id,
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> WriterInner<Cursor> {
    async fn switch_buffer(&self, initial_offset: u32) -> u32 {
        let next_buffer = self.buffer_pool.acquire().await;
        next_buffer.write_cursor().store(0, Ordering::Relaxed);

        let wanted_cursor =
            ((unsafe { next_buffer.id() } as u64) << CURSOR_BUF_SHIFT) |
            initial_offset as u64;

        self.cursor.start_buffer(wanted_cursor, next_buffer);
        self.switch_buffer_waiters.lock().notify_all(());

        unsafe { next_buffer.id() }
    }

    async fn wait_for_buffer(&self, cursor: u64) {
        let prev_buf_index = cursor & CURSOR_BUF_MASK;

        let mut waiter = core::pin::pin!(self.switch_buffer_waiters.wait());
        core::future::poll_fn(move |cx| {
            waiter.as_mut().poll_fulfillment(cx, || {
                let cursor = self.cursor.get();
                let buf_index = cursor & CURSOR_BUF_MASK;
                let offset = (cursor & CURSOR_OFFSET_MASK) as u32;

                if buf_index != prev_buf_index || offset < self.buffer_pool.buffer_size() as u32 {
                    Some(Fulfillment { inner: (), count: 1 })
                } else {
                    None
                }
            })
        })
        .await;
    }

    fn release_buffer(&self) {
        let buffer_size = self.buffer_pool.buffer_size();

        let cursor = self.cursor.release_buffer();
        let buffer_id = ((cursor & CURSOR_BUF_MASK) >> CURSOR_BUF_SHIFT) as u32;
        let offset = (cursor & CURSOR_OFFSET_MASK) as u32;
        let is_initialized = (cursor & CURSOR_INIT) == 0;

        if is_initialized && offset < buffer_size as u32 {
            let buffer = self.buffer_pool.buffer_by_id(buffer_id);
            self.cursor.finish_buffer(buffer);
            self.flusher.advance_write_cursor(
                buffer,
                offset,
                offset | WRITE_CURSOR_DONE,
            );
        }
    }
}

mod sealed {
    use crate::BufferPtr;

    pub trait WriterCursor {
        fn get(&self) -> u64;

        fn start_buffer(&self, v: u64, next_buffer: BufferPtr);

        fn finish_buffer(&self, prev_buffer: BufferPtr);

        fn try_reserve(&self, len: u64) -> u64;

        fn try_init(&self) -> u64;

        fn take_ref(&self, buffer: BufferPtr);

        fn release_buffer(&self) -> u64;
    }
}

pub struct SharedCursor {
    cursor: CachePadded<AtomicU64>,
}

impl sealed::WriterCursor for SharedCursor {
    fn get(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    fn start_buffer(&self, v: u64, next_buffer: BufferPtr) {
        // 1 local ref + 1 shared ref for the first packet
        // another shared ref for the flusher
        unsafe { next_buffer.initialize_rc(1, 1, 2); }
        self.cursor.store(v, Ordering::Relaxed);
    }

    fn finish_buffer(&self, _prev_buffer: BufferPtr) { }

    fn try_reserve(&self, len: u64) -> u64 {
        self.cursor.fetch_add(len, Ordering::AcqRel)
    }

    fn try_init(&self) -> u64 {
        self.cursor.fetch_or(CLAIM_CURSOR_INIT, Ordering::AcqRel)
    }

    fn take_ref(&self, buffer: BufferPtr) {
        if buffer.get_local_rc() > 0 {
            unsafe { buffer.take_ref(1); }
        } else {
            // This ensures that a buffer won't be released without checking the shared
            // reference count.
            unsafe { buffer.take_shared_ref(1); }
        }
    }

    fn release_buffer(&self) -> u64 {
        self.cursor.swap(CURSOR_INIT, Ordering::Relaxed)
    }
}

impl Default for SharedCursor {
    fn default() -> Self {
        Self {
            cursor: CachePadded::new(AtomicU64::new(CURSOR_INIT)),
        }
    }
}

pub struct LocalCursor {
    cursor: Cell<u64>,
}

impl sealed::WriterCursor for LocalCursor {
    fn get(&self) -> u64 {
        self.cursor.get()
    }

    fn start_buffer(&self, v: u64, next_buffer: BufferPtr) {
        // 2 local refs: 1 for the writer, one for the first packet
        // 2 shared refs: 1 for the writer, one for the flusher
        unsafe { next_buffer.initialize_rc(2, 1, 2); }
        self.cursor.set(v);
    }

    fn finish_buffer(&self, prev_buffer: BufferPtr) {
        unsafe {
            prev_buffer.release_ref(1);
        }
    }

    fn try_reserve(&self, len: u64) -> u64 {
        let prev = self.cursor.get();
        self.cursor.set(prev + len);
        prev
    }

    fn try_init(&self) -> u64 {
        let prev = self.cursor.get();
        self.cursor.set(prev | CLAIM_CURSOR_INIT);
        prev
    }

    fn take_ref(&self, buffer: BufferPtr) {
        unsafe { buffer.take_ref(1); }
    }

    fn release_buffer(&self) -> u64 {
        self.cursor.replace(CURSOR_INIT)
    }
}

impl Default for LocalCursor {
    fn default() -> Self {
        Self {
            cursor: Cell::new(CURSOR_INIT),
        }
    }
}

impl<Cursor: sealed::WriterCursor + Default> Writer<Cursor> {
    pub fn new(
        buffer_pool: HeapBufferPool,
        flush_queue: WriterFlushQueue,
        writer_id: usize,
    ) -> Self {
        Self {
            inner: Arc::new(WriterInner {
                buffer_pool: buffer_pool,
                cursor: Cursor::default(),
                flusher: WriterFlusher::new(flush_queue),
                switch_buffer_waiters: WaiterQueue::new(),
            }),
            writer_id,
        }
    }
}

impl<Cursor: sealed::WriterCursor + 'static> Writer<Cursor> {
    pub fn to_dyn(self) -> DynWriter {
        Writer {
            inner: self.inner.clone(),
            writer_id: self.writer_id,
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> Writer<Cursor> {
    /// Reserve space on a buffer to write bytes to.
    /// 
    /// The returned `Write` handle can be dereferenced into a mutable `[u8]`. When finished
    /// writing to the buffer, you can convert the `Write` handle to a `Packet` if desired:
    /// 
    /// ```notrust
    /// let mut write_buf = writer.reserve(5).await;
    /// write_buf[..].copy_from_slice(b"hello");
    /// let packet: bab::Packet = write_buf.into();
    /// ```
    /// 
    /// ## Footgun alert
    ///
    /// This method is unfortunately a bit of a footgun. The returned `Write` handles busy-wait
    /// until all previously reserved `Write` handles have been dropped, and so all `Write` handles
    /// on the same thread must be dropped in the order that they were acquired, otherwise a
    /// deadlock will occur.
    /// 
    /// You should make the returned `Write` as shortlived as possible and especially avoid keeping
    /// one alive across an await point. Besides avoiding deadlocks, keeping a `Write` handle alive
    /// longer than it needs to be is a performance concern because it can hold up other threads
    /// that are using the writer.
    pub async fn reserve(&self, len: usize) -> Write<Cursor> {
        let buffer_size = self.inner.buffer_pool.buffer_size();
        if len > buffer_size {
            panic!("packet too big! len={} max={}", len, buffer_size);
        }

        if len > buffer_size / 2 {
            // Big reservation - just grab a dedicated buffer for this one.
            let buffer = self.inner.buffer_pool.acquire().await;
            buffer.write_cursor().store(0, Ordering::Relaxed);

            unsafe { buffer.initialize_rc(1, 1, 2); }

            return Write {
                writer: self,
                buffer,
                offset: 0,
                len: len as u32,
                is_buffer_done: true,
            };
        }

        loop {
            // Try to allocate in current buffer
            let cursor = self.inner.cursor.try_reserve(len as u64);
            let is_uninitialized = (cursor & CURSOR_INIT) != 0;
            let buf_index = ((cursor & CURSOR_BUF_MASK) >> CURSOR_BUF_SHIFT) as u32;
            let offset = (cursor & CURSOR_OFFSET_MASK) as u32;

            let use_buf_index: u32;
            let use_offset: u32;

            assert!(offset + len as u32 <= CURSOR_OFFSET_MASK as u32);

            if is_uninitialized {
                let prev_cursor = self.inner.cursor.try_init();
                let prev_buf_index = ((prev_cursor & CURSOR_BUF_MASK) >> CURSOR_BUF_SHIFT) as u32;
                let latest_cursor = prev_cursor | CLAIM_CURSOR_INIT;

                if prev_cursor & CLAIM_CURSOR_INIT == 0 {
                    // This task is designated to acquire the initial buffer.
                    let next_buf_index = self.inner.switch_buffer(len as u32).await;

                    use_buf_index = next_buf_index;
                    use_offset = 0;
                } else {
                    assert_eq!(prev_buf_index, 0);
                    // Wait for initial buffer.
                    self.inner.wait_for_buffer(latest_cursor).await;
                    continue;
                }
            } else {
                let latest_cursor = cursor + len as u64;

                if (offset as usize) < buffer_size &&
                    offset as usize + len >= buffer_size
                {
                    // This task tipped the buffer over the limit, so a new buffer needs to be
                    // swapped in. The task that tips the buffer over the limit is the designated
                    // task to notify the flusher and switch the buffer.

                    let prev_buffer = self.inner.buffer_pool.buffer_by_id(buf_index);
                    self.inner.cursor.finish_buffer(prev_buffer);
                    self.inner.flusher.advance_write_cursor(
                        prev_buffer,
                        offset,
                        offset | WRITE_CURSOR_DONE,
                    );

                    // When we swap the buffer we allocate space on the new buffer simultaneously.
                    let next_buf_index = self.inner.switch_buffer(len as u32).await;
                    use_buf_index = next_buf_index;
                    use_offset = 0;
                } else if offset as usize + len >= buffer_size {
                    // Wait for buffer to be swapped.
                    self.inner.wait_for_buffer(latest_cursor).await;
                    continue;
                } else {
                    // Allocation on current buffer successful.
                    use_buf_index = buf_index;
                    use_offset = offset;
                    assert!(use_offset > 0);
                }
            }

            let buffer = self.inner.buffer_pool.buffer_by_id(use_buf_index);
            return Write {
                writer: self,
                buffer,
                offset: use_offset,
                len: len as u32,
                is_buffer_done: false,
            };
        }
    }

    pub fn flush(&self) {
        self.inner.flusher.flush();
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> Drop for WriterInner<Cursor> {
    fn drop(&mut self) {
        self.release_buffer();
    }
}

pub struct Write<'a, Cursor: sealed::WriterCursor + ?Sized> {
    writer: &'a Writer<Cursor>,
    buffer: BufferPtr,
    offset: u32,
    len: u32,
    is_buffer_done: bool,
}

impl<Cursor: sealed::WriterCursor + ?Sized> Write<'_, Cursor> {
    pub fn len(&self) -> usize { self.len as usize }
}

impl<Cursor: sealed::WriterCursor + ?Sized> core::ops::Deref for Write<'_, Cursor> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.buffer.data().add(self.offset as usize),
                self.len() as usize,
            )
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> core::ops::DerefMut for Write<'_, Cursor> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            core::slice::from_raw_parts_mut(
                self.buffer.data().add(self.offset as usize),
                self.len() as usize,
            )
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> AsRef<[u8]> for Write<'_, Cursor> {
    fn as_ref(&self) -> &[u8] { core::ops::Deref::deref(self) }
}

impl<Cursor: sealed::WriterCursor + ?Sized> Drop for Write<'_, Cursor> {
    fn drop(&mut self) {
        self.writer.inner.flusher.advance_write_cursor(
            self.buffer,
            self.offset,
            (self.offset + self.len) | if self.is_buffer_done { WRITE_CURSOR_DONE } else { 0 },
        );

        if self.offset == 0 {
            // Since a Packet isn't being created from this write, release the buffer ref that was
            // added in WriteCursor::start_buffer.
            unsafe { self.buffer.release_ref(1); }
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> From<Write<'_, Cursor>> for crate::Packet {
    fn from(write: Write<'_, Cursor>) -> Self {
        if write.offset > 0 {
            // Only take a ref if it's not the first write to the buffer - the ref for the first
            // buffer write is handled in WriteCursor::start_buffer.
            write.writer.inner.cursor.take_ref(write.buffer);
        }

        write.writer.inner.flusher.advance_write_cursor(
            write.buffer,
            write.offset,
            (write.offset + write.len) | if write.is_buffer_done { WRITE_CURSOR_DONE } else { 0 },
        );

        let packet = Self::new(
            write.buffer,
            write.offset as usize,
            write.len as usize,
        );

        core::mem::forget(write);

        packet
    }
}
