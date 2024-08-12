use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering};

static NEXT_THREAD_ID: AtomicU32 = AtomicU32::new(0);
thread_local! {
    static THREAD_ID: Cell<ThreadId> = Cell::new(ThreadId::uninitialized());
}
// Hopefully this is stabilized one day, it is a bit faster.
//#[thread_local]
//static THREAD_ID: Cell<ThreadId> = Cell::new(ThreadId::uninitialized());

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ThreadId(u32);

impl ThreadId {
    const fn uninitialized() -> Self { Self(u32::MAX) }

    fn next() -> Self {
        ThreadId(NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed))
    }

    fn is_uninitialized(&self) -> bool { self.0 == u32::MAX }

    pub fn as_u32(&self) -> u32 { self.0 }
    pub fn as_usize(&self) -> usize { self.0 as usize }
}

pub fn current() -> ThreadId {
    let mut thread_id = THREAD_ID.get();

    if thread_id.is_uninitialized() {
        thread_id = ThreadId::next();
        THREAD_ID.set(thread_id);
    }

    thread_id
}

pub fn clear() {
    THREAD_ID.set(ThreadId::uninitialized());
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "std")]
    #[test]
    fn different_threads_have_different_ids() {
        use std::sync::mpsc;
        use std::thread;

        let (tx, rx) = mpsc::channel();
        thread::spawn(move || tx.send(current()).unwrap()).join().unwrap();

        let main_id = current();
        let other_id = rx.recv().unwrap();
        assert!(main_id != other_id);
    }
}
