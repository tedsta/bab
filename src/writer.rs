use core::{
    cell::Cell,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(feature = "alloc")]
use alloc::sync::Arc;
#[cfg(feature = "std")]
use std::sync::Arc;

use crossbeam_utils::CachePadded;
use waitq::WaiterQueue;

use crate::{
    HeapBufferPool, Packet,
    buffer::BufferPtr,
    writer_flush::{WRITE_CURSOR_DONE, WRITE_CURSOR_MASK, WriterFlushSender},
};

pub type DynWriter = Writer<dyn sealed::WriterCursor>;
pub type SharedWriter = Writer<SharedCursor>;
pub type LocalWriter = Writer<LocalCursor<WriterFlushSender>>;
pub type LocalWriterNoFlush = Writer<LocalCursor<NoopFlusher>>;

pub struct Writer<Cursor: sealed::WriterCursor + ?Sized> {
    inner: Arc<WriterInner<Cursor>>,
}

struct WriterInner<Cursor: sealed::WriterCursor + ?Sized> {
    writer_id: usize,
    max_buffer_size: usize,
    buffer_pool: HeapBufferPool,
    switch_buffer_waiters: WaiterQueue<()>,
    cursor: Cursor,
}

const CURSOR_INIT: u64 = 0x8000_0000_0000_0000;
const CLAIM_CURSOR_INIT: u64 = 0x4000_0000_0000_0000;
const CURSOR_BUF_MASK: u64 = 0x0FFF_FFFF_0000_0000;
const CURSOR_OFFSET_MASK: u64 = 0x0000_0000_0FFF_FFFF;

const CURSOR_BUF_SHIFT: u64 = 32;

impl<Cursor: sealed::WriterCursor + ?Sized> Clone for Writer<Cursor> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> WriterInner<Cursor> {
    async fn switch_buffer(&self, initial_offset: u32) -> u32 {
        let next_buffer = self.buffer_pool.acquire().await;
        unsafe {
            next_buffer.set_writer_id(self.writer_id);
        }
        next_buffer.write_cursor().store(0, Ordering::Release);

        let wanted_cursor =
            ((unsafe { next_buffer.id() } as u64) << CURSOR_BUF_SHIFT) | initial_offset as u64;

        self.cursor.start_buffer(wanted_cursor, next_buffer);
        self.switch_buffer_waiters.lock().notify_all(());

        unsafe { next_buffer.id() }
    }

    fn try_switch_buffer(&self, initial_offset: u32) -> Option<u32> {
        let next_buffer = self.buffer_pool.try_acquire()?;
        unsafe {
            next_buffer.set_writer_id(self.writer_id);
        }
        next_buffer.write_cursor().store(0, Ordering::Release);

        let wanted_cursor =
            ((unsafe { next_buffer.id() } as u64) << CURSOR_BUF_SHIFT) | initial_offset as u64;

        self.cursor.start_buffer(wanted_cursor, next_buffer);
        self.switch_buffer_waiters.lock().notify_all(());

        Some(unsafe { next_buffer.id() })
    }

    async fn wait_for_buffer(&self, cursor: u64) {
        let prev_buf_index = cursor & CURSOR_BUF_MASK;

        self.switch_buffer_waiters
            .wait_until(|| {
                let cursor = self.cursor.get();
                let buf_index = cursor & CURSOR_BUF_MASK;
                let offset = (cursor & CURSOR_OFFSET_MASK) as u32;
                buf_index != prev_buf_index || offset < self.max_buffer_size as u32
            })
            .await;
    }

    fn release_buffer(&self) {
        let buffer_size = self.max_buffer_size;

        let cursor = self.cursor.release_buffer();
        let buffer_id = ((cursor & CURSOR_BUF_MASK) >> CURSOR_BUF_SHIFT) as u32;
        let offset = (cursor & CURSOR_OFFSET_MASK) as u32;
        let is_initialized = (cursor & CURSOR_INIT) == 0;

        if is_initialized && offset < buffer_size as u32 {
            let buffer = self.buffer_pool.buffer_by_id(buffer_id);
            self.cursor
                .advance_write_cursor(buffer, offset, offset | WRITE_CURSOR_DONE);
            self.cursor.finish_buffer(buffer);
        }

        self.switch_buffer_waiters.lock().notify_all(());
    }
}

mod sealed {
    use crate::BufferPtr;

    pub trait WriterCursor {
        fn get(&self) -> u64;

        fn initialize_dedicated_buffer(&self, buffer: BufferPtr);

        fn start_buffer(&self, v: u64, next_buffer: BufferPtr);

        fn finish_buffer(&self, prev_buffer: BufferPtr);

        fn try_reserve(&self, len: u64) -> u64;

        fn try_init(&self) -> u64;

        fn take_ref(&self, buffer: BufferPtr);

        fn release_buffer(&self) -> u64;

        fn flush_local(&self);

        fn flush_full(&self);

        fn advance_write_cursor(&self, buffer: BufferPtr, write_start: u32, new_write_cursor: u32);

        fn send_complete_buffer(&self, buffer: BufferPtr);

        fn unflushed_bytes(&self) -> usize;
    }
}

pub struct NoopFlusher;

impl Default for NoopFlusher {
    fn default() -> Self {
        Self
    }
}

pub struct SharedCursor {
    cursor: CachePadded<AtomicU64>,
    flusher: WriterFlushSender,
}

impl sealed::WriterCursor for SharedCursor {
    fn get(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    fn initialize_dedicated_buffer(&self, buffer: BufferPtr) {
        // 1 local ref for the returned packet
        // 2 shared refs: 1 for the returned packet, 1 for the flusher
        unsafe {
            buffer.initialize_rc(1, 1, 2);
        }
    }

    fn start_buffer(&self, v: u64, next_buffer: BufferPtr) {
        // 1 local ref + 1 shared ref for the first packet
        // another shared ref for the flusher
        unsafe {
            next_buffer.initialize_rc(1, 1, 2);
        }
        self.cursor.store(v, Ordering::Relaxed);
    }

    fn finish_buffer(&self, _prev_buffer: BufferPtr) {}

    fn try_reserve(&self, len: u64) -> u64 {
        self.cursor.fetch_add(len, Ordering::AcqRel)
    }

    fn try_init(&self) -> u64 {
        self.cursor.fetch_or(CLAIM_CURSOR_INIT, Ordering::AcqRel)
    }

    fn take_ref(&self, buffer: BufferPtr) {
        if buffer.get_local_rc() > 0 {
            unsafe {
                buffer.take_ref(1);
            }
        } else {
            // This ensures that a buffer won't be released without checking the shared
            // reference count.
            unsafe {
                buffer.take_shared_ref(1);
            }
        }
    }

    fn release_buffer(&self) -> u64 {
        self.cursor.swap(CURSOR_INIT, Ordering::Relaxed)
    }

    fn flush_local(&self) {}

    fn flush_full(&self) {
        self.flusher.flush();
    }

    fn advance_write_cursor(&self, buffer: BufferPtr, write_start: u32, new_write_cursor: u32) {
        self.flusher
            .advance_write_cursor(buffer, write_start, new_write_cursor);
    }

    fn send_complete_buffer(&self, buffer: BufferPtr) {
        self.flusher.send_complete_buffer(buffer);
    }

    fn unflushed_bytes(&self) -> usize {
        self.flusher.unflushed_bytes()
    }
}

impl SharedCursor {
    fn new(flusher: WriterFlushSender) -> Self {
        Self {
            cursor: CachePadded::new(AtomicU64::new(CURSOR_INIT)),
            flusher,
        }
    }
}

pub struct LocalCursor<Flusher> {
    last_flush_cursor: Cell<u32>,
    advance_cursor: Cell<u32>,
    current_buffer: Cell<Option<BufferPtr>>,

    cursor: Cell<u64>,
    flusher: Flusher,
}

impl sealed::WriterCursor for LocalCursor<WriterFlushSender> {
    fn get(&self) -> u64 {
        self.cursor.get()
    }

    fn initialize_dedicated_buffer(&self, buffer: BufferPtr) {
        // 1 local ref for the returned packet
        // 2 shared refs: 1 for the returned packet, 1 for the flusher
        unsafe {
            buffer.initialize_rc(1, 1, 2);
        }
    }

    fn start_buffer(&self, v: u64, next_buffer: BufferPtr) {
        // 2 local refs: 1 for the writer, one for the first packet
        // 2 shared refs: 1 for the writer, one for the flusher
        unsafe {
            next_buffer.initialize_rc(2, 1, 2);
        }
        self.cursor.set(v);

        self.last_flush_cursor.set(0);
        self.advance_cursor.set(0);
        debug_assert!(self.current_buffer.get().is_none());
        self.current_buffer.set(Some(next_buffer));
    }

    fn finish_buffer(&self, prev_buffer: BufferPtr) {
        self.flusher.advance_write_cursor(
            prev_buffer,
            self.last_flush_cursor.get(),
            self.advance_cursor.get(),
        );
        self.current_buffer.set(None);

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
        unsafe {
            buffer.take_ref(1);
        }
    }

    fn release_buffer(&self) -> u64 {
        self.cursor.replace(CURSOR_INIT)
    }

    fn flush_local(&self) {
        if let Some(current_buffer) = self.current_buffer.get() {
            if self.advance_cursor.get() != self.last_flush_cursor.get() {
                self.flusher.advance_write_cursor(
                    current_buffer,
                    self.last_flush_cursor.get(),
                    self.advance_cursor.get(),
                );
                self.last_flush_cursor.set(self.advance_cursor.get());
            }
        }
    }

    fn flush_full(&self) {
        self.flush_local();
        self.flusher.flush();
    }

    fn advance_write_cursor(&self, buffer: BufferPtr, write_start: u32, new_write_cursor: u32) {
        if Some(buffer) == self.current_buffer.get() {
            debug_assert_eq!(self.advance_cursor.get() & WRITE_CURSOR_MASK, write_start);

            // Defer advancing the write cursor until we flush.
            self.advance_cursor.set(new_write_cursor);
        } else {
            // It's not the buffer tracked by this cursor
            self.flusher
                .advance_write_cursor(buffer, write_start, new_write_cursor);
        }
    }

    fn send_complete_buffer(&self, buffer: BufferPtr) {
        self.flusher.send_complete_buffer(buffer);
    }

    fn unflushed_bytes(&self) -> usize {
        self.flusher.unflushed_bytes()
    }
}

impl sealed::WriterCursor for LocalCursor<NoopFlusher> {
    fn get(&self) -> u64 {
        self.cursor.get()
    }

    fn initialize_dedicated_buffer(&self, buffer: BufferPtr) {
        // 1 local ref for the returned packet
        unsafe {
            buffer.initialize_rc(1, 0, 0);
        }
    }
    fn start_buffer(&self, v: u64, next_buffer: BufferPtr) {
        // 2 local refs: 1 for the writer, one for the first packet
        unsafe {
            next_buffer.initialize_rc(2, 0, 0);
        }
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
        unsafe {
            buffer.take_ref(1);
        }
    }

    fn release_buffer(&self) -> u64 {
        self.cursor.replace(CURSOR_INIT)
    }

    fn flush_local(&self) {}

    fn flush_full(&self) {}

    fn advance_write_cursor(&self, _buffer: BufferPtr, _write_start: u32, _new_write_cursor: u32) {}

    fn send_complete_buffer(&self, _buffer: BufferPtr) {}

    fn unflushed_bytes(&self) -> usize {
        0
    }
}

impl<Flusher> LocalCursor<Flusher> {
    fn new(flusher: Flusher) -> Self {
        Self {
            advance_cursor: Cell::new(0),
            last_flush_cursor: Cell::new(0),
            current_buffer: Cell::new(None),
            cursor: Cell::new(CURSOR_INIT),
            flusher,
        }
    }
}

impl Writer<SharedCursor> {
    pub fn new_shared(
        buffer_pool: HeapBufferPool,
        buffer_tailroom: usize,
        flusher: WriterFlushSender,
        writer_id: usize,
    ) -> Self {
        Self::new(
            buffer_pool,
            buffer_tailroom,
            SharedCursor::new(flusher),
            writer_id,
        )
    }
}

impl Writer<LocalCursor<WriterFlushSender>> {
    pub fn new_local_flush(
        buffer_pool: HeapBufferPool,
        buffer_tailroom: usize,
        flusher: WriterFlushSender,
        writer_id: usize,
    ) -> Self {
        Self::new(
            buffer_pool,
            buffer_tailroom,
            LocalCursor::new(flusher),
            writer_id,
        )
    }
}

impl Writer<LocalCursor<NoopFlusher>> {
    pub fn new_local_noflush(
        buffer_pool: HeapBufferPool,
        buffer_tailroom: usize,
        writer_id: usize,
    ) -> Self {
        Self::new(
            buffer_pool,
            buffer_tailroom,
            LocalCursor::new(NoopFlusher),
            writer_id,
        )
    }
}

impl<Cursor: sealed::WriterCursor> Writer<Cursor> {
    fn new(
        buffer_pool: HeapBufferPool,
        buffer_tailroom: usize,
        cursor: Cursor,
        writer_id: usize,
    ) -> Self {
        let mut max_buffer_size = buffer_pool.buffer_size();
        if max_buffer_size as u64 > CURSOR_OFFSET_MASK + 1 {
            panic!(
                "Writers do not support buffers larger than {} bytes",
                CURSOR_OFFSET_MASK + 1
            );
        }

        if buffer_tailroom >= max_buffer_size {
            panic!(
                "bab::Writer buffer_tailroom can't be larger than the buffers themselves - tailroom={} buffer_size={}",
                buffer_tailroom, max_buffer_size,
            );
        }
        max_buffer_size -= buffer_tailroom;

        Self {
            inner: Arc::new(WriterInner {
                writer_id,
                buffer_pool: buffer_pool,
                cursor,
                switch_buffer_waiters: WaiterQueue::new(),
                max_buffer_size,
            }),
        }
    }
}

impl<Cursor: sealed::WriterCursor + 'static> Writer<Cursor> {
    pub fn to_dyn(self) -> DynWriter {
        Writer { inner: self.inner }
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
    pub async fn reserve(&self, len: usize) -> Write<'_, Cursor> {
        let buffer_size = self.inner.max_buffer_size;
        if len > buffer_size {
            panic!("packet too big! len={} max={}", len, buffer_size);
        }

        if len > buffer_size / 2 {
            // Big reservation - just grab a dedicated buffer for this one.
            let buffer = self.inner.buffer_pool.acquire().await;
            unsafe {
                buffer.set_writer_id(self.inner.writer_id);
            }
            buffer.write_cursor().store(0, Ordering::Release);

            self.inner.cursor.initialize_dedicated_buffer(buffer);

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

                if offset as usize + len < buffer_size {
                    // Allocation on current buffer successful.
                    use_buf_index = buf_index;
                    use_offset = offset;
                    debug_assert!(use_offset > 0);
                } else if (offset as usize) < buffer_size {
                    // This task tipped the buffer over the limit, so a new buffer needs to be
                    // swapped in. The task that tips the buffer over the limit is the designated
                    // task to notify the flusher and switch the buffer.

                    let prev_buffer = self.inner.buffer_pool.buffer_by_id(buf_index);
                    self.inner.cursor.advance_write_cursor(
                        prev_buffer,
                        offset,
                        offset | WRITE_CURSOR_DONE,
                    );
                    self.inner.cursor.finish_buffer(prev_buffer);

                    // When we swap the buffer we allocate space on the new buffer simultaneously.
                    let next_buf_index = self.inner.switch_buffer(len as u32).await;
                    use_buf_index = next_buf_index;
                    use_offset = 0;
                } else {
                    // Wait for buffer to be swapped.
                    self.inner.wait_for_buffer(latest_cursor).await;
                    continue;
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

    pub fn try_reserve(&self, len: usize) -> Option<Write<'_, Cursor>> {
        let buffer_size = self.inner.max_buffer_size;
        if len > buffer_size {
            panic!("packet too big! len={} max={}", len, buffer_size);
        }

        if len > buffer_size / 2 {
            // Big reservation - just grab a dedicated buffer for this one.
            let buffer = self.inner.buffer_pool.try_acquire()?;
            unsafe {
                buffer.set_writer_id(self.inner.writer_id);
            }
            buffer.write_cursor().store(0, Ordering::Release);

            unsafe {
                buffer.initialize_rc(1, 1, 2);
            }

            return Some(Write {
                writer: self,
                buffer,
                offset: 0,
                len: len as u32,
                is_buffer_done: true,
            });
        }

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

            if prev_cursor & CLAIM_CURSOR_INIT == 0 {
                // This task is designated to acquire the initial buffer.
                let Some(next_buf_index) = self.inner.try_switch_buffer(len as u32) else {
                    // Force next reserver to re-initialize the writer.
                    self.inner.release_buffer();
                    return None;
                };

                use_buf_index = next_buf_index;
                use_offset = 0;
            } else {
                assert_eq!(prev_buf_index, 0);
                // Wait for initial buffer.
                return None;
            }
        } else {
            if offset as usize + len < buffer_size {
                // Allocation on current buffer successful.
                use_buf_index = buf_index;
                use_offset = offset;
                debug_assert!(use_offset > 0);
            } else if (offset as usize) < buffer_size {
                // This task tipped the buffer over the limit, so a new buffer needs to be
                // swapped in. The task that tips the buffer over the limit is the designated
                // task to notify the flusher and switch the buffer.

                let prev_buffer = self.inner.buffer_pool.buffer_by_id(buf_index);
                self.inner.cursor.advance_write_cursor(
                    prev_buffer,
                    offset,
                    offset | WRITE_CURSOR_DONE,
                );
                self.inner.cursor.finish_buffer(prev_buffer);

                // When we swap the buffer we allocate space on the new buffer simultaneously.
                let Some(next_buf_index) = self.inner.try_switch_buffer(len as u32) else {
                    // Force next reserver to re-initialize the writer.
                    self.inner.release_buffer();
                    return None;
                };
                use_buf_index = next_buf_index;
                use_offset = 0;
            } else {
                // Wait for buffer to be swapped.
                return None;
            }
        }

        let buffer = self.inner.buffer_pool.buffer_by_id(use_buf_index);
        Some(Write {
            writer: self,
            buffer,
            offset: use_offset,
            len: len as u32,
            is_buffer_done: false,
        })
    }

    pub fn ingest_complete_buffer(&self, buffer: BufferPtr, len: usize) -> Packet {
        unsafe {
            buffer.set_writer_id(self.inner.writer_id);
        }

        if buffer.get_local_rc() == 0 {
            // 1 local ref + local shared contribution for the returned packet
            // 2 shared refs: 1 for packet + 1 for the flush receiver
            unsafe {
                buffer.initialize_rc(1, 1, 2);
            }
        } else {
            // Buffer is already being reference counted. Tack on new references for the returned
            // Packet and the flush receiver.
            unsafe {
                buffer.take_ref(2); // 1 for the packet, 1 for the flush receiver
                buffer.send(); // send 1 ref to the flush receiver
            }
        }

        let packet = unsafe { Packet::new(buffer, 0, len) };

        self.inner.cursor.send_complete_buffer(buffer);

        packet
    }

    pub fn flush_local(&self) {
        self.inner.cursor.flush_local();
    }

    pub fn flush(&self) {
        self.inner.cursor.flush_full();
    }

    pub fn unflushed_bytes(&self) -> usize {
        self.inner.cursor.unflushed_bytes()
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
    pub fn len(&self) -> usize {
        self.len as usize
    }
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
    fn as_ref(&self) -> &[u8] {
        core::ops::Deref::deref(self)
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> Drop for Write<'_, Cursor> {
    fn drop(&mut self) {
        self.writer.inner.cursor.advance_write_cursor(
            self.buffer,
            self.offset,
            (self.offset + self.len)
                | if self.is_buffer_done {
                    WRITE_CURSOR_DONE
                } else {
                    0
                },
        );

        if self.offset == 0 {
            // Since a Packet isn't being created from this write, release the buffer ref that was
            // added in WriteCursor::start_buffer.
            unsafe {
                self.buffer.release_ref(1);
            }
        }
    }
}

impl<Cursor: sealed::WriterCursor + ?Sized> From<Write<'_, Cursor>> for Packet {
    fn from(write: Write<'_, Cursor>) -> Self {
        if write.offset > 0 {
            // Only take a ref if it's not the first write to the buffer - the ref for the first
            // buffer write is handled in WriteCursor::start_buffer.
            write.writer.inner.cursor.take_ref(write.buffer);
        }

        write.writer.inner.cursor.advance_write_cursor(
            write.buffer,
            write.offset,
            (write.offset + write.len)
                | if write.is_buffer_done {
                    WRITE_CURSOR_DONE
                } else {
                    0
                },
        );

        let packet = unsafe { Self::new(write.buffer, write.offset as usize, write.len as usize) };

        core::mem::forget(write);

        packet
    }
}
