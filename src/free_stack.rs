use core::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use crossbeam_utils::{atomic::AtomicCell, Backoff};

use crate::buffer::{Buffer, BufferPtr};

// Utility MPMC concurrent stack for BufferPool to store freed batches of buffers.
pub struct FreeStack {
    slots: Box<[AtomicPtr<Buffer>]>,
    count: AtomicUsize,
}

impl FreeStack {
    pub fn new(capacity: usize) -> Self {
        assert!(AtomicCell::<Option<BufferPtr>>::is_lock_free());
        Self {
            slots: (0..capacity).map(|_| AtomicPtr::new(core::ptr::null_mut()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            count: AtomicUsize::new(0),
        }
    }

    pub fn pop(&self) -> Option<BufferPtr> {
        let backoff = Backoff::new();
        loop {
            let count = self.count.load(Ordering::Relaxed);
            match self.count.compare_exchange(
                count,
                count.saturating_sub(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                // There are no available slots.
                Ok(_) if count == 0 => break,
                Ok(_) => {
                    // Successfully acquired a slot.
                    backoff.reset();
                    loop {
                        if let Some(taken) = BufferPtr::from_ptr(
                            self.slots[count - 1].swap(core::ptr::null_mut(), Ordering::AcqRel)
                        ) {
                            return Some(taken);
                        }
                        backoff.snooze();
                    }
                }
                Err(_) => { }
            }
            backoff.spin();
        }

        None
    }

    pub fn push_if(&self, buffer: BufferPtr, mut condition: impl FnMut(usize) -> bool) -> bool {
        let backoff = Backoff::new();
        loop {
            let count = self.count.load(Ordering::Relaxed);
            if !condition(count) {
                return false;
            }
            match self.count.compare_exchange(
                count,
                count + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully reserved a slot.
                    backoff.reset();
                    loop {
                        match self.slots[count].compare_exchange(
                            core::ptr::null_mut(),
                            buffer.as_ptr_mut(),
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(_) => { }
                        }
                        backoff.snooze();
                    }
                    return true;
                }
                Err(_) => { }
            }
            backoff.spin();
        }
    }
}