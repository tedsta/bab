use core::{
    alloc::Layout,
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
    task::{Context, Poll},
};

#[cfg(feature = "alloc")]
use alloc::{alloc::{alloc, dealloc}, boxed::Box, vec::Vec};
#[cfg(feature = "std")]
use std::alloc::{alloc, dealloc};

use crossbeam_utils::CachePadded;

use crate::{
    buffer::{Buffer, BufferPtr},
    free_stack::FreeStack,
    thread_local::ThreadLocal,
    waiter_queue::{IFulfillment, Fulfillment, Waiter, WaiterQueue},
};

pub(crate) struct Local {
    head: Cell<Option<BufferPtr>>,
    watermark: Cell<Option<BufferPtr>>,
    count: Cell<u32>,
    buffers_in_use: Cell<u32>,
    local_buffer_state: *const [LocalBufferState],
}

unsafe impl Send for Local { }

impl Local {
    #[cfg(any(feature = "std", feature = "alloc"))]
    fn new_heap(total_buffer_count: usize) -> Self {
        let local_buffer_state = Box::into_raw(
            (0..total_buffer_count).map(|_| {
                LocalBufferState {
                    ref_count: Cell::new(0),
                    shared_rc_contribution: Cell::new(0),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
        );

        Self {
            head: Cell::new(None),
            watermark: Cell::new(None),
            count: Cell::new(0),
            buffers_in_use: Cell::new(0),
            local_buffer_state,
        }
    }
}

impl Local {
    fn try_acquire(&self) -> Option<BufferPtr> {
        self.head.get().map(|head_ptr| {
            // We can take a buffer from the local batch - advance the local head and return.
            debug_assert!(self.count.get() > 0);
            self.count.set(self.count.get() - 1);
            self.head.set(unsafe { head_ptr.get_next() });
            unsafe { head_ptr.set_next(None); }

            head_ptr
        })
    }

    pub(crate) fn local_buffer_state(&self, buffer_id: usize) -> &LocalBufferState {
        unsafe {
            &*core::ptr::addr_of!((*self.local_buffer_state)[buffer_id])
        }
    }
}

pub(crate) struct LocalBufferState {
    pub(crate) ref_count: Cell<u32>,
    pub(crate) shared_rc_contribution: Cell<u32>,
}

pub struct BufferPool {
    pub(crate) alloc: *mut Buffer,
    alloc_layout: Layout,
    // Size of a single buffer, including padding for alignment, in self.alloc
    buffer_padded_size: usize,
    total_buffer_count: usize,
    buffer_size: usize,
    batch_size: u32,
    free_stack: FreeStack,
    waiter_queue: WaiterQueue<BufferPtr>,
    local: ThreadLocal<CachePadded<Local>>,
    ref_count: AtomicUsize,
    shutdown_released_buffers: AtomicU32,
    handle_drop_fn: fn(*mut Self),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BufferPoolShutdownStatus {
    NotShutdown,
    ShutdownNow,
    AlreadyShutdown,
}

/// Guard object returned by BufferPool::register_thread - see there for more details.
pub struct BufferPoolThreadGuard<'a> {
    buffer_pool: &'a BufferPool,
}

impl Drop for BufferPoolThreadGuard<'_> {
    fn drop(&mut self) {
        self.buffer_pool.decrement_local_buffers_in_use(self.buffer_pool.local());
    }
}

unsafe impl Send for BufferPool { }
unsafe impl Sync for BufferPool { }

impl BufferPool {
    /// Get the total number of buffers from this pool that are in circulation.
    pub fn total_buffer_count(&self) -> usize {
        self.total_buffer_count
    }

    /// Get the size in bytes of each buffer in this pool.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    #[inline]
    pub fn buffer_by_id(&self, id: u32) -> BufferPtr {
        let buffer_raw = unsafe {
            self.alloc.byte_add(id as usize * self.buffer_padded_size)
        };
        BufferPtr::from_ptr(buffer_raw).unwrap()
    }

    /// Register the current thread as a known user of buffers until the returned guard object is
    /// dropped. This is purely an optimization and is optional to do. It prevents an atomic
    /// reference count from being unnecessarily incremented and decremented when buffers are
    /// received and released by this thread.
    pub fn register_thread(&self) -> BufferPoolThreadGuard {
        self.increment_local_buffers_in_use(self.local());
        BufferPoolThreadGuard {
            buffer_pool: self,
        }
    }

    pub(crate) fn local(&self) -> &Local {
        self.local.get_or(|| CachePadded::new(Local::new_heap(self.total_buffer_count as usize)))
    }

    pub(crate) fn increment_local_buffers_in_use(&self, local: &Local) {
        let prev = local.buffers_in_use.replace(local.buffers_in_use.get() + 1);
        if prev == 0 {
            if !self.is_shutting_down() {
                self.ref_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns true if the buffer pool is shutting down.
    pub(crate) fn decrement_local_buffers_in_use(&self, local: &Local) -> BufferPoolShutdownStatus {
        let prev = local.buffers_in_use.replace(local.buffers_in_use.get() - 1);
        if prev == 1 {
            if !self.is_shutting_down() {
                let prev_ref_count = self.ref_count.fetch_sub(1, Ordering::AcqRel);
                if prev_ref_count == 1 {
                    // This is was the last active buffer on the last active thread. There may still
                    // be some unreleased buffers, but they will all be buffers sent from one thread
                    // and not yet received by their destination thread.
                    //
                    // It's important that after the fetch_sub above, self.local.count won't be
                    // written to ever again by any thread, and any thread that reads the
                    // ref_count == 0 value sees the latest self.local.count values. This is why we
                    // use AcqRel ordering in the fetch_sub above, and Acquire ordering in
                    // BufferPool::is_shutting_down.
                    BufferPoolShutdownStatus::ShutdownNow
                } else {
                    BufferPoolShutdownStatus::NotShutdown
                }
            } else {
                self.shutdown_released_buffers.fetch_add(1, Ordering::Relaxed);
                BufferPoolShutdownStatus::AlreadyShutdown
            }
        } else {
            BufferPoolShutdownStatus::NotShutdown
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        match self.ref_count.compare_exchange(0, 0, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub async fn acquire(&self) -> BufferPtr {
        if let Some(buffer) = self.try_acquire() {
            return buffer;
        }

        // Need to wait for a buffer to become available.
        let buffer = Acquire {
            buffer_pool: self,
            waiter: Waiter::new(&self.waiter_queue),
        }
        .await;

        buffer
    }

    pub fn try_acquire(&self) -> Option<BufferPtr> {
        let local = self.local();
        if let Some(local_buffer) = local.try_acquire() {
            return Some(local_buffer);
        }

        self.try_acquire_batch(local)
    }

    fn try_acquire_batch(&self, local: &Local) -> Option<BufferPtr> {
        debug_assert!(local.head.get().is_none());
        debug_assert_eq!(local.count.get(), 0);

        if let Some(batch_head) = self.try_take_batch(local) {
            local.head.set(unsafe { batch_head.swap_next(None) });
            local.count.set(self.batch_size as u32 - 1);
            Some(batch_head)
        } else {
            None
        }
    }

    fn try_take_batch(&self, local: &Local) -> Option<BufferPtr> {
        debug_assert!(local.head.get().is_none());
        debug_assert_eq!(local.count.get(), 0);

        self.free_stack.pop()
    }

    pub unsafe fn release(&self, buffer: BufferPtr) {
        debug_assert_eq!(unsafe { buffer.get_next() }, None);

        if self.waiter_queue.notify_one_local(buffer).is_none() {
            return;
        }

        let local = self.local();

        // Release the buffer into local stock
        if local.count.get() == self.batch_size {
            // Store the local batch watermark
            local.watermark.set(Some(buffer));
        }
        unsafe {
            buffer.set_next(local.head.get());
        }
        local.head.set(Some(buffer));
        local.count.set(local.count.get() + 1);

        self.release_overflow(local);
    }

    fn release_overflow(&self, local: &Local) {
        if local.count.get() < (self.batch_size as u32 * 3) / 2 {
            return;
        }

        // Release batch

        let Some(watermark) = local.watermark.take() else {
            unreachable!();
        };

        let release_head = unsafe { watermark.swap_next(None) }.unwrap();
        let release_count = self.batch_size;
        local.count.set(local.count.get() - self.batch_size as u32);

        debug_assert!(local.count.get() < (self.batch_size as u32 * 3) / 2);
        debug_assert_eq!(release_count, self.batch_size);

        let mut waiter_queue_guard = None;
        self.free_stack.push_if(release_head, |free_count| {
            if free_count == 0 {
                // Only need to try to notify a waiter if the stock was empty.
                let guard = waiter_queue_guard.get_or_insert_with(|| self.waiter_queue.lock());

                let waiter_count = guard.waiter_count();
                if waiter_count > 0 {
                    Self::notify_waiters(
                        release_head, release_count as usize, waiter_count,
                        |fulfillment| {
                            guard.notify(fulfillment.inner, fulfillment.count);
                        },
                    );
                    // Don't push onto free stack since we've used the released buffers to
                    // fulfill waiters.
                    return false;
                }
            }

            true
        });
    }

    fn notify_waiters(
        release_head: BufferPtr,
        release_count: usize,
        waiter_count: usize,
        mut notify_fn: impl FnMut(Fulfillment<BufferPtr>),
    ) {
        let mut fulfillment = Fulfillment {
            inner: release_head,
            count: release_count,
        };

        let buffers_per_waiter: u32;
        let remainder: u32;
        if release_count >= waiter_count {
            buffers_per_waiter = (release_count / waiter_count) as u32;
            remainder = (release_count % waiter_count) as u32;
        } else {
            buffers_per_waiter = 1;
            remainder = 0;
        }

        let next_fulfillment = fulfillment.take(buffers_per_waiter + remainder);
        notify_fn(next_fulfillment);

        for _ in 1..core::cmp::min(waiter_count, release_count) {
            let next_fulfillment = fulfillment.take(buffers_per_waiter);
            notify_fn(next_fulfillment);
        }

        debug_assert_eq!(fulfillment.count, 0);
    }

    fn release_many(&self, release_head: BufferPtr, release_count: usize) {
        let local = self.local();

        // Find the local tail so we can add the extra buffers.
        let prev_local_count = local.count.get();
        let mut tail = local.head.get();
        while let Some(next) = tail {
            let new_tail = unsafe { next.get_next() };
            if new_tail.is_none() {
                break;
            }
            tail = new_tail;
        }

        if let Some(tail) = tail {
            // Append newly released buffers to end of local stockpile.
            unsafe { tail.set_next(Some(release_head)); }
            local.count.set(local.count.get() + release_count as u32);
        } else {
            // Local stockpile is empty.
            debug_assert_eq!(local.head.get(), None);
            debug_assert_eq!(local.count.get(), 0);
            local.head.set(Some(release_head));
            local.count.set(release_count as u32);
        }

        let overflow_threshold = self.batch_size * 3 / 2;
        debug_assert!(prev_local_count < overflow_threshold as u32);

        self.find_watermark(local);
        self.release_overflow(local);

        debug_assert!(local.count.get() < self.batch_size as u32 * 3 / 2);

        self.find_watermark(local);
    }

    fn find_watermark(&self, local: &Local) {
        if local.count.get() > self.batch_size as u32 {
            let mut watermark = local.head.get().unwrap();
            for _ in 0..local.count.get() - self.batch_size as u32 - 1 {
                watermark = unsafe { watermark.get_next() }.unwrap();
            }
            local.watermark.set(Some(watermark));
        }
    }

    pub(crate) fn shutdown_now_try_drop(buffer_pool: *mut BufferPool) {
        let this = unsafe { &mut *buffer_pool };

        let mut released_buffers = 0;
        for local in this.local.iter_mut() {
            assert_eq!(local.buffers_in_use.get(), 0);
            released_buffers += local.count.get();
        }
        while let Some(_) = this.free_stack.pop() {
            released_buffers += this.batch_size;
        }
        let prev_released_buffers =
            this.shutdown_released_buffers.fetch_add(released_buffers, Ordering::Relaxed);

        if prev_released_buffers + released_buffers == this.total_buffer_count as u32 {
            // All buffers have been released - time to drop
            let handle_drop_fn = unsafe { (*buffer_pool).handle_drop_fn };
            handle_drop_fn(buffer_pool as *mut BufferPool);
        }
    }

    pub(crate) fn already_shutdown_try_drop(buffer_pool: *mut BufferPool) {
        let this = unsafe { &*buffer_pool };
        if this.shutdown_released_buffers.compare_exchange(
            this.total_buffer_count as u32,
            this.total_buffer_count as u32,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).is_ok() {
            // All buffers have been released - time to drop
            let handle_drop_fn = unsafe { (*buffer_pool).handle_drop_fn };
            handle_drop_fn(buffer_pool as *mut BufferPool);
        }
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // Drop all local buffer state arrays
        for local in self.local.iter_mut() {
            let _ = unsafe { Box::from_raw(local.local_buffer_state as *mut [LocalBufferState]) };
        }

        let _ = unsafe { dealloc(self.alloc as *mut u8, self.alloc_layout) };
    }
}

pub struct HeapBufferPool {
    ptr: *const BufferPool,
}

impl HeapBufferPool {
    pub fn new(
        buffer_size: usize,
        batch_count: usize,
        batch_size: usize,
    ) -> Self {
        // Helpers adapted from core::alloc::Layout till they're stable.
        fn padding_needed_for_layout(layout: Layout) -> usize {
            let len = layout.size();
            let align = layout.align();

            (len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1))
                .wrapping_sub(len)
        }
        fn repeat_layout(layout: Layout, n: usize) -> (Layout, usize) {
            let padded_size = layout.size() + padding_needed_for_layout(layout);
            let alloc_size = padded_size.checked_mul(n).unwrap();

            let layout = Layout::from_size_align(alloc_size, layout.align()).unwrap();
            (layout, padded_size)
        }

        let total_buffer_count = batch_count * batch_size;
        let buffer_layout = Buffer::layout_with_data(buffer_size);
        let (alloc_layout, buffer_padded_size) = repeat_layout(buffer_layout, total_buffer_count);
        let alloc = unsafe { alloc(alloc_layout) } as *mut Buffer;

        let buffer_pool = Box::new(BufferPool {
            alloc,
            alloc_layout,
            buffer_padded_size,
            free_stack: FreeStack::new(batch_count),
            waiter_queue: WaiterQueue::new(),
            total_buffer_count,
            buffer_size,
            batch_size: batch_size as u32,
            local: ThreadLocal::new(),
            ref_count: AtomicUsize::new(1),
            shutdown_released_buffers: AtomicU32::new(0),
            handle_drop_fn: |buffer_pool| {
                let _ = unsafe { Box::from_raw(buffer_pool) };
            },
        });
        let buffer_pool_ptr = Box::into_raw(buffer_pool);
        let buffer_pool = unsafe { &*buffer_pool_ptr };

        // Initialize buffer descriptors
        for id in 0..total_buffer_count {
            let buffer = buffer_pool.buffer_by_id(id as u32);
            unsafe {
                Buffer::initialize(buffer.as_ptr_mut(), buffer_pool_ptr, id as usize, buffer_size);
            }
        }

        let mut next_buffer_id = 0;
        for _ in 0..batch_count {
            let new_batch_head = buffer_pool.buffer_by_id(next_buffer_id);
            next_buffer_id += 1;

            let mut head = None;
            for _ in 1..batch_size {
                let next = head;
                let new_head = buffer_pool.buffer_by_id(next_buffer_id);
                head = Some(new_head);
                next_buffer_id += 1;
                unsafe { new_head.set_next(next); }
            }
            unsafe { new_batch_head.set_next(head); }

            buffer_pool.free_stack.push_if(new_batch_head, |_|  true);
        }

        Self { ptr: buffer_pool_ptr }
    }
}

unsafe impl Send for HeapBufferPool { }
unsafe impl Sync for HeapBufferPool { }

impl core::ops::Deref for HeapBufferPool {
    type Target = BufferPool;

    fn deref(&self) -> &Self::Target {
        // SAFETY: BufferPool is never dereferenced mutably except on drop.
        unsafe { &*self.ptr }
    }
}

impl Clone for HeapBufferPool {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        Self { ptr: self.ptr }
    }
}

impl Drop for HeapBufferPool {
    fn drop(&mut self) {
        let prev_rc = self.ref_count.fetch_sub(1, Ordering::Relaxed);
        if prev_rc == 1 {
            // All HeapBufferPool handles have been dropped.
            BufferPool::shutdown_now_try_drop(self.ptr as *mut _);
        }
    }
}

impl Fulfillment<BufferPtr> {
    // Note this can leave self in an invalid state if self.count == n, where afterwards self.count
    // is 0. In that case, self must never be used again.
    fn take(&mut self, n: u32) -> Self {
        let head = self.inner;
        let mut tail = head;
        for _ in 1..n {
            tail = unsafe { tail.get_next() }.unwrap();
        }

        self.inner = unsafe { tail.swap_next(None) }.unwrap_or(tail);
        self.count -= n as usize;

        Self {
            inner: head,
            count: n as usize,
        }
    }
}

impl IFulfillment for BufferPtr {
    fn take_one(&mut self) -> Self {
        let ptr = *self;
        *self = unsafe { ptr.swap_next(None) }.unwrap();
        ptr
    }

    fn append(&mut self, other: Self, _other_count: usize) {
        let mut tail = *self;
        while let Some(next) = unsafe { tail.get_next() } {
            tail = next;
        }

        unsafe { tail.set_next(Some(other)); }
    }
}

pub struct Acquire<'a> {
    buffer_pool: &'a BufferPool,
    waiter: Waiter<'a, BufferPtr>,
}

impl<'a> Acquire<'a> {
    fn waiter(self: Pin<&'_ mut Self>) -> Pin<&'_ mut Waiter<'a, BufferPtr>> {
        // SAFETY: `waiter` is pinned when `self` is.
        unsafe { self.map_unchecked_mut(|s| &mut s.waiter) }
    }
}

impl Future for Acquire<'_> {
    type Output = BufferPtr;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let buffer_pool = self.buffer_pool;
        let local = self.buffer_pool.local();
        let Poll::Ready(fulfillment) =
            self.as_mut().waiter().poll_fulfillment(
                context,
                || {
                    if let Some(local_head) = local.head.replace(None) {
                        Some(Fulfillment {
                            inner: local_head,
                            count: local.count.replace(0) as usize,
                        })
                    } else {
                        buffer_pool.try_take_batch(local)
                            .map(|ptr| {
                                Fulfillment { inner: ptr, count: buffer_pool.batch_size as usize }
                            })
                    }
                }
            )
        else {
            return Poll::Pending;
        };

        let extra_head = unsafe { fulfillment.inner.swap_next(None) };
        let extra_count = fulfillment.count as usize - 1;

        if let Some(extra_head) = extra_head {
            debug_assert!(extra_count > 0);
            self.buffer_pool.release_many(extra_head, extra_count);
        }

        Poll::Ready(fulfillment.inner)
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if let Some(fulfillment) = self.waiter.cancel() {
            self.buffer_pool.release_many(fulfillment.inner, fulfillment.count as usize);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_buffer_fulfillment_append_and_take_one() {
        let batch_count = 16;
        let batch_size = 16;
        let buffer_pool = HeapBufferPool::new(16, batch_count, batch_size);

        let a = buffer_pool.try_acquire().unwrap();
        let b = buffer_pool.try_acquire().unwrap();
        let c = buffer_pool.try_acquire().unwrap();
        for buffer in [a, b, c] {
            unsafe { buffer.initialize_rc(1, 0, 0); }
        }

        assert_eq!(a.count(), 1);
        assert_eq!(b.count(), 1);
        assert_eq!(c.count(), 1);

        let mut f = Fulfillment { inner: a, count: 1 };

        f.append(Fulfillment { inner: b, count: 1});
        assert_eq!((f.inner, f.count), (a, 2));
        assert_eq!(a.count(), 2);
        assert_eq!(b.count(), 1);
        assert_eq!(c.count(), 1);
        f.append(Fulfillment { inner: c, count: 1});
        assert_eq!((f.inner, f.count), (a, 3));
        assert_eq!(a.count(), 3);
        assert_eq!(b.count(), 2);
        assert_eq!(c.count(), 1);

        let taken = f.take_one();
        assert_eq!(taken, a);
        assert_eq!((f.inner, f.count), (b, 2));
        assert_eq!(a.count(), 1);
        assert_eq!(b.count(), 2);
        assert_eq!(c.count(), 1);

        let taken = f.take_one();
        assert_eq!(taken, b);
        assert_eq!((f.inner, f.count), (c, 1));
        assert_eq!(a.count(), 1);
        assert_eq!(b.count(), 1);
        assert_eq!(c.count(), 1);

        for buffer in [a, b, c] {
            unsafe { buffer.release_ref(1); }
        }
    }

    #[test]
    fn test_buffer_fulfillment_take() {
        let batch_count = 16;
        let batch_size = 16;
        let buffer_pool = HeapBufferPool::new(16, batch_count, batch_size);

        let a = buffer_pool.try_acquire().unwrap();
        let b = buffer_pool.try_acquire().unwrap();
        let c = buffer_pool.try_acquire().unwrap();

        let mut f = Fulfillment { inner: a, count: 1 };
        f.append(Fulfillment { inner: b, count: 1});
        f.append(Fulfillment { inner: c, count: 1});

        let taken = f.take(2);
        assert_eq!((taken.inner, taken.count), (a, 2));
        assert_eq!((f.inner, f.count), (c, 1));
        assert_eq!(unsafe { a.swap_next(None) }, Some(b));
        assert_eq!(unsafe { b.get_next() }, None);
        assert_eq!(unsafe { c.get_next() }, None);

        unsafe {
            buffer_pool.release(a);
            buffer_pool.release(b);
            buffer_pool.release(c);
        }
    }

    #[test]
    fn test_buffer_pool_shutdown_send_packet() {
        let batch_count = 16;
        let batch_size = 16;
        let buffer_pool = HeapBufferPool::new(16, batch_count, batch_size);

        let a = buffer_pool.try_acquire().unwrap();
        let b = buffer_pool.try_acquire().unwrap();

        unsafe {
            a.initialize_rc(1, 0, 0);
            b.initialize_rc(1, 1, 1);
        }

        drop(buffer_pool);

        unsafe {
            a.release_ref(1);
            assert_eq!(b.send_bulk(1), 1);
            b.receive(1);
            b.release_ref(1);
        }
    }

    #[test]
    fn test_notify_waiters() {
        let batch_count = 16;
        let batch_size = 16;
        let buffer_pool = HeapBufferPool::new(16, batch_count, batch_size);

        let a = buffer_pool.try_acquire().unwrap();
        let b = buffer_pool.try_acquire().unwrap();
        let c = buffer_pool.try_acquire().unwrap();
        let d = buffer_pool.try_acquire().unwrap();
        let e = buffer_pool.try_acquire().unwrap();

        let mut f = Fulfillment { inner: a, count: 1 };
        f.append(Fulfillment { inner: b, count: 1});
        f.append(Fulfillment { inner: c, count: 1});
        f.append(Fulfillment { inner: d, count: 1});
        f.append(Fulfillment { inner: e, count: 1});

        let mut notified_count = 0;
        let calls = [
            (a, 3),
            (d, 1),
            (e, 1),
        ];
        BufferPool::notify_waiters(f.inner, f.count as usize, 3, |f| {
            assert_eq!((f.inner, f.count), calls[notified_count]);
            assert_eq!(f.inner.count(), f.count as usize);
            notified_count += 1;
        });
        assert_eq!(notified_count, calls.len());

        // Take apart the fulfillment batches created by notify_waiters to avoid debug sanity check
        // panic in BufferPool::release.
        unsafe {
            a.set_next(None);
            b.set_next(None);
            c.set_next(None);
            d.set_next(None);
            e.set_next(None);
        };

        unsafe {
            buffer_pool.release(a);
            buffer_pool.release(b);
            buffer_pool.release(c);
            buffer_pool.release(d);
            buffer_pool.release(e);
        }
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_buffer_pool_local_acquire_waiter() {
        use std::rc::Rc;

        let batch_count = 16;
        let batch_size = 2;
        let waiter_count = 8;
        let buffer_pool = HeapBufferPool::new(64, batch_count, batch_size);

        let ex = async_executor::LocalExecutor::new();

        pollster::block_on(ex.run(async {
            // Acquire all the buffers
            let channel = Rc::new(async_unsync::unbounded::channel());
            let acquire_starts = Rc::new(async_unsync::semaphore::Semaphore::new(0));

            for _ in 0..batch_count * batch_size {
                let buf: BufferPtr = buffer_pool.acquire().await;
                let data = unsafe {
                    core::slice::from_raw_parts_mut(buf.data(), buffer_pool.buffer_size())
                };
                unsafe { buf.initialize_rc(1, 0, 0); }
                data[..4].copy_from_slice(&[1, 2, 3, 4]);
                channel.send(buf).unwrap();
            }

            // Spawn some acquires to force local acquire waiters to get created.
            for _ in 0..waiter_count {
                let buffer_pool = buffer_pool.clone();
                let channel = channel.clone();
                let acquire_starts = acquire_starts.clone();
                ex.spawn(async move {
                    acquire_starts.add_permits(1);
                    let buf: BufferPtr = buffer_pool.acquire().await;
                    let data = unsafe {
                        core::slice::from_raw_parts_mut(buf.data(), buffer_pool.buffer_size())
                    };
                    unsafe { buf.initialize_rc(1, 0, 0); }
                    data[..4].copy_from_slice(&[1, 2, 3, 4]);
                    channel.send(buf).unwrap();
                })
                .detach();
            }

            acquire_starts.acquire_many(waiter_count).await.unwrap();

            // Release all the buffers
            for _ in 0..batch_count * batch_size + waiter_count {
                let buf = channel.recv().await.unwrap();
                let data = unsafe {
                    core::slice::from_raw_parts_mut(buf.data(), buffer_pool.buffer_size())
                };
                assert_eq!(&data[..4], &[1, 2, 3, 4]);
                unsafe { buf.release_ref(1); }
            }

            assert!(channel.try_recv().is_err());
        }));
    }
}
