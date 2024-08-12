use core::cell::UnsafeCell;

use crate::thread_id;

const DEFAULT_MAX_THREADS: usize = usize::BITS as usize;

/// Thread-local per-object value.
pub struct ThreadLocal<T: Send, const MAX_THREADS: usize = DEFAULT_MAX_THREADS> {
    entries: [UnsafeCell<Option<T>>; MAX_THREADS],
}

unsafe impl<const MAX_THREADS: usize, T: Send> Send for ThreadLocal<T, MAX_THREADS> { }
unsafe impl<const MAX_THREADS: usize, T: Send> Sync for ThreadLocal<T, MAX_THREADS> { }

impl<const MAX_THREADS: usize, T: Send> ThreadLocal<T, MAX_THREADS> {
    const DEFAULT_ENTRY: UnsafeCell<Option<T>> = UnsafeCell::new(None);

    pub fn new() -> Self {
        Self {
            entries: [Self::DEFAULT_ENTRY; MAX_THREADS],
        }
    }

    /// Returns the element for the current thread, if it exists.
    #[inline]
    pub fn get(&self) -> Option<&T> {
        self.get_inner(thread_id::current().as_usize())
    }

    /// Returns the element for the current thread, or creates a default one if it doesn’t exist.
    #[inline]
    pub fn get_or_default(&self) -> &T
    where T: Default
    {
        self.get_or(|| Default::default())
    }

    /// Returns the element for the current thread, or creates it if it doesn't exist.
    #[inline]
    pub fn get_or<F>(&self, create: F) -> &T
    where
        F: FnOnce() -> T,
    {
        let thread_id = thread_id::current().as_usize();
        if let Some(val) = self.get_inner(thread_id) {
            return val;
        }
        self.insert(thread_id, create())
    }

    /// Returns the element for the current thread, or creates it if it doesn't exist. If `create`
    /// fails, that error is returned and no element is added.
    #[inline]
    pub fn get_or_try<F, E>(&self, create: F) -> Result<&T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        let thread_id = thread_id::current().as_usize();
        if let Some(val) = self.get_inner(thread_id) {
            return Ok(val);
        }
        Ok(self.insert(thread_id, create()?))
    }

    /// Returns a mutable iterator over the local values of all threads in unspecified order.
    ///
    /// Since this call borrows the ThreadLocal mutably, this operation can be done safely - the
    /// mutable borrow statically guarantees no other threads are currently accessing their
    /// associated values.
    pub fn iter_mut(&mut self) -> IterMut<'_, MAX_THREADS, T> {
        IterMut {
            thread_local: self,
            cursor: 0,
        }
    }

    #[inline]
    fn get_inner(&self, thread_id: usize) -> Option<&T> {
        unsafe { &*self.entries[thread_id].get() }.as_ref()
    }

    fn get_inner_ptr(&self, thread_id: usize) -> Option<*mut T> {
        unsafe { &mut *self.entries[thread_id].get() }
            .as_mut()
            .map(|entry| entry as *mut _)
    }

    fn insert(&self, thread_id: usize, value: T) -> &T {
        unsafe {
            let entry = &mut *self.entries[thread_id].get();
            *entry = Some(value);
            entry.as_ref().unwrap_unchecked()
        }
    }
}

pub struct IterMut<'a, const MAX_THREADS: usize, T: Send> {
    thread_local: &'a ThreadLocal<T, MAX_THREADS>,
    cursor: usize,
}

impl<'a, const MAX_THREADS: usize, T: Send> Iterator for IterMut<'a, MAX_THREADS, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < MAX_THREADS {
            if let Some(entry_ptr) = self.thread_local.get_inner_ptr(self.cursor) {
                self.cursor += 1;
                return Some(unsafe { &mut *entry_ptr });
            }
            self.cursor += 1;
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_thread_local_noncopy() {
        let t = ThreadLocal::<Vec<u8>>::new();
        assert_eq!(t.get(), None);
        let v = t.get_or(|| vec![1, 2, 3]);
        assert_eq!(v, &[1, 2, 3]);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_thread_local_end_to_end() {
        use std::cell::Cell;
        use std::sync::Arc;

        let t = Arc::new(ThreadLocal::<Vec<Cell<u32>>>::new());

        let threads = (0..4).map(|x| {
            let t = t.clone();
            std::thread::spawn(move || {
                let v = t.get_or(|| vec![Cell::new(x * 10), Cell::new(x * 10 + 5)]);
                assert_eq!(v[0].get(), x * 10);
                assert_eq!(v[1].get(), x * 10 + 5);
                t.get().unwrap()[0].set(t.get().unwrap()[0].get() + 1);
                t.get().unwrap()[1].set(t.get().unwrap()[1].get() + 1);
                assert_eq!(v[0].get(), x * 10 + 1);
                assert_eq!(v[1].get(), x * 10 + 5 + 1);
            })
        })
        .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap();
        }

        let mut thread_local_owned = Arc::into_inner(t).unwrap();
        let mut entries = thread_local_owned.iter_mut().collect::<Vec<_>>();
        entries.sort_by_key(|x| x[0].get());
        assert_eq!(
            entries,
            vec![
                &mut vec![Cell::new(1), Cell::new(6)],
                &mut vec![Cell::new(11), Cell::new(16)],
                &mut vec![Cell::new(21), Cell::new(26)],
                &mut vec![Cell::new(31), Cell::new(36)],
            ],
        );
    }
}
