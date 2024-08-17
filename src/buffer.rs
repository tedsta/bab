use core::{
    alloc::Layout,
    cell::UnsafeCell,
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
};

use crossbeam_utils::CachePadded;

use crate::{
    buffer_pool::BufferPoolShutdownStatus,
    BufferPool,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufferPtr {
    ptr: NonNull<Buffer>,
}

unsafe impl Send for BufferPtr { }
unsafe impl Sync for BufferPtr { }

impl BufferPtr {
    pub unsafe fn id(&self) -> u32 {
        self.as_ref().buffer_id as u32
    }

    #[inline]
    pub unsafe fn data(&self) -> *mut u8 {
        Buffer::data(self.ptr.as_ptr())
    }

    /// SAFETY: this method must not be called concurrently from multiple threads.
    pub unsafe fn set_next(&self, next: Option<Self>) {
        unsafe {
            *self.as_ref().next.get() = next;
        }
    }

    /// SAFETY: this method must not be called concurrently from multiple threads.
    pub unsafe fn swap_next(&self, new_next: Option<Self>) -> Option<Self> {
        let next_mut = unsafe { &mut *self.as_ref().next.get() };
        core::mem::replace(next_mut, new_next)
    }

    /// SAFETY: this method must not be called concurrently with set_next or swap_next from
    /// multiple threads.
    pub unsafe fn get_next(&self) -> Option<Self> {
        unsafe {
            *self.as_ref().next.get()
        }
    }

    /// You must ensure that you have exclusive access to the reference count of the buffer - that
    /// no other threads try to take or release references to this buffer between this buffer being
    /// acquired from the pool and the call to this method.
    pub unsafe fn initialize_rc(&self, count: u32, local_shared: u32, shared: u32) {
        // SAFETY: BufferPool and Buffer are never mutably referenced.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        debug_assert_eq!(buffer_local.ref_count.get(), 0);
        debug_assert_eq!(buffer_local.shared_rc_contribution.get(), 0);

        buffer_local.ref_count.set(count);
        buffer_local.shared_rc_contribution.set(local_shared);
        if shared > 0 {
            buffer.ref_count.store(shared, Ordering::Relaxed);
        }

        if count > 0 {
            unsafe { &*buffer.buffer_pool }.increment_local_buffers_in_use(local);
        }
    }

    /// Used when converting a LocalPacket into a sendable Packet. Increments the shared reference
    /// count and decrements the thread-local reference count.
    ///
    /// Returns the `shared_rc_contribution` value that the receiving thread should add to its own.
    pub unsafe fn send(&self) -> u32 {
        self.send_bulk(1)
    }

    /// Used when converting a LocalPacket into a sendable Packet. Increments the shared reference
    /// count and decrements the thread-local reference count.
    ///
    /// Returns the `shared_rc_contribution` value that the receiving thread should add to its own.
    pub unsafe fn send_bulk(&self, count: u32) -> u32 {
        // SAFETY: memory behind buffer_pool is never mutably accessed.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        debug_assert!(buffer_local.ref_count.get() > 0);

        buffer_local.ref_count.set(buffer_local.ref_count.get() - count);

        if buffer_local.ref_count.get() == 0 {
            // There are no remaining references on the current thread, so the shared reference
            // count needs to decrease by 1. This cancels out the need to increment the shared
            // reference count to refer to this buffer from the new thread.

            let buffer_pool = unsafe { &*buffer.buffer_pool };
            let shutdown_status = buffer_pool.decrement_local_buffers_in_use(local);
            if shutdown_status == BufferPoolShutdownStatus::ShutdownNow {
                // Note this definitely won't drop the buffer pool since this buffer isn't released
                // yet, but shutdown_now_try_drop still needs to be called to start the buffer pool
                // shutdown procedure.
                BufferPool::shutdown_now_try_drop(buffer.buffer_pool as *mut _);
            }

            // This thread's shared_rc_contribution is transferred to the receiving thread.
            return buffer_local.shared_rc_contribution.replace(0);
        } else {
            // There are other active references on the current thread.

            if buffer_local.shared_rc_contribution.get() == 0 {
                // This thread is the owner, so the shared reference count was never set -
                // initialize it here since this buffer is going to be referenced by another
                // thread.
                buffer.ref_count.store(1 + count, Ordering::Relaxed);
                buffer_local.shared_rc_contribution.set(1);
                return count;
            } else {
                buffer.ref_count.fetch_add(count, Ordering::Relaxed);
                return count;
            }
        }
    }

    /// Used when converting a LocalPacket into a sendable Packet. Increments the shared reference
    /// count and decrements the thread-local reference count.
    pub unsafe fn receive(&self, shared_rc_contribution: u32) {
        // SAFETY: BufferPool and Buffer are never mutably referenced.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        let prev_local_rc= buffer_local.ref_count.replace(buffer_local.ref_count.get() + 1);
        buffer_local.shared_rc_contribution.set(
            buffer_local.shared_rc_contribution.get() + shared_rc_contribution
        );

        if prev_local_rc == 0 {
            unsafe { &*buffer.buffer_pool }.increment_local_buffers_in_use(local);
        }
    }

    pub unsafe fn take_ref(&self, count: u32) -> u32 {
        // SAFETY: BufferPool and Buffer are never mutably referenced.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        let prev_rc = buffer_local.ref_count.get();
        buffer_local.ref_count.set(prev_rc + count);
        assert!(prev_rc > 0);

        prev_rc
    }

    pub unsafe fn take_shared_ref(&self, count: u32) {
        // SAFETY: BufferPool and Buffer are never mutably referenced.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        let prev_rc = buffer_local.ref_count.get();
        buffer_local.ref_count.set(prev_rc + count);
        buffer_local.shared_rc_contribution.set(buffer_local.shared_rc_contribution.get() + count);
        buffer.ref_count.fetch_add(count, Ordering::Relaxed);

        if prev_rc == 0 {
            unsafe { &*buffer.buffer_pool }.increment_local_buffers_in_use(local);
        }
    }

    pub unsafe fn release_ref(&self, count: u32) {
        // SAFETY: BufferPool and Buffer are never mutably referenced.
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        let buffer_local = local.local_buffer_state(buffer.buffer_id);

        debug_assert!(buffer_local.ref_count.get() >= count);

        buffer_local.ref_count.set(buffer_local.ref_count.get() - count);
        if buffer_local.ref_count.get() > 0 {
            return;
        }

        // All local references have been released.

        let buffer_pool_ptr = buffer.buffer_pool;
        let buffer_pool = unsafe { &*buffer_pool_ptr };
        let shutdown_status = buffer_pool.decrement_local_buffers_in_use(local);

        // Release this thread's contribution to the shared reference count if applicable.
        let shared_rc_contribution = buffer_local.shared_rc_contribution.replace(0);
        if shared_rc_contribution > 0 {
            // time to release the buffer
            let prev_rc = buffer.ref_count.fetch_sub(shared_rc_contribution, Ordering::Relaxed);
            if prev_rc > shared_rc_contribution {
                // There are still active references, don't release the buffer.
                return;
            }
            assert_eq!(shared_rc_contribution, prev_rc);
        }

        // If we've made it this far, it's time to release the buffer back into the pool.

        // We don't release the buffer if the pool is already shutting down. We do this so that
        // BufferPool::shutdown_now_try_drop has unique access to all threads' local buffers cache.
        if shutdown_status != BufferPoolShutdownStatus::AlreadyShutdown {
            unsafe { buffer_pool.release(*self); }
        }

        match shutdown_status {
            BufferPoolShutdownStatus::ShutdownNow => {
                BufferPool::shutdown_now_try_drop(buffer_pool_ptr as *mut _);
            }
            BufferPoolShutdownStatus::AlreadyShutdown => {
                BufferPool::already_shutdown_try_drop(buffer_pool_ptr as *mut _);
            }
            BufferPoolShutdownStatus::NotShutdown => { }
        }
    }

    pub(crate) fn from_ptr(ptr: *mut Buffer) -> Option<BufferPtr> {
        NonNull::new(ptr).map(|ptr| BufferPtr { ptr })
    }

    pub(crate) fn as_ptr_mut(&self) -> *mut Buffer {
        self.ptr.as_ptr()
    }

    // Technically unsafe but it's only used internally and we can eventually make it safe if we
    // make BufferPtr a proper owned handle.
    pub(crate) fn as_ref(&self) -> &Buffer {
        unsafe { self.ptr.as_ref() }
    }

    // Technically unsafe but it's only used internally and we can eventually make it safe if we
    // make BufferPtr a proper owned handle.
    pub(crate) fn get_local_rc(&self) -> u32 {
        let buffer = self.as_ref();
        let local = unsafe { &*buffer.buffer_pool }.local();
        local.local_buffer_state(buffer.buffer_id).ref_count.get()
    }

    // Technically unsafe but it's only used internally and we can eventually make it safe if we
    // make BufferPtr a proper owned handle.
    pub(crate) fn write_cursor(&self) -> &AtomicU32 {
        &self.as_ref().write_cursor
    }

    // Technically unsafe but it's only used internally and we can eventually make it safe if we
    // make BufferPtr a proper owned handle.
    pub(crate) fn writer_id(&self) -> &AtomicUsize {
        &self.as_ref().writer_id
    }

    pub(crate) unsafe fn flush_cursor_mut(&self) -> &mut u32 {
        &mut *self.as_ref().flush_cursor.get()
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        let mut count = 1;
        let mut tail = *self;
        while let Some(next) = unsafe { tail.get_next() } {
            tail = next;
            count += 1;
        }
        count
    }
}

pub struct Buffer {
    buffer_pool: *const BufferPool,
    buffer_id: usize,
    next: UnsafeCell<Option<BufferPtr>>,
    writer_id: AtomicUsize,
    write_cursor: AtomicU32,
    flush_cursor: UnsafeCell<u32>,
    ref_count: AtomicU32,
}

impl Buffer {
    pub(crate) fn layout_with_data(capacity: usize) -> Layout {
        let layout = Layout::new::<CachePadded<Buffer>>();
        // Note that Buffer's data comes after the `Buffer` alignment padding.
        let (layout, _) = layout.extend(Layout::array::<u8>(capacity).unwrap()).unwrap();
        layout
    }

    #[inline]
    pub(crate) fn data(buffer: *const Self) -> *mut u8 {
        unsafe { (buffer as *mut u8).add(core::mem::size_of::<CachePadded<Self>>()) }
    }

    pub(crate) unsafe fn initialize(
        buffer: *mut Self,
        buffer_pool: *const BufferPool,
        buffer_id: usize,
        capacity: usize,
    ) {
        use core::ptr::addr_of_mut;

        addr_of_mut!((*buffer).buffer_pool).write(buffer_pool);
        addr_of_mut!((*buffer).buffer_id).write(buffer_id);
        addr_of_mut!((*buffer).next).write(UnsafeCell::new(None));
        addr_of_mut!((*buffer).writer_id).write(AtomicUsize::new(usize::MAX));
        addr_of_mut!((*buffer).write_cursor).write(AtomicU32::new(0));
        addr_of_mut!((*buffer).flush_cursor).write(UnsafeCell::new(0));
        addr_of_mut!((*buffer).ref_count).write(AtomicU32::new(0));
        Self::data(buffer).write_bytes(0, capacity);
    }
}
