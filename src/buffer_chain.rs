use core::cell::Cell;

use crate::BufferPtr;

/// A linked list of `BufferPtr`s.
///
/// The user must ensure that a BufferPtr can appear at most once in at most one `BufferChain` at
/// any given time. Failing to do so in a way that breaks any `BufferChain`'s behavior will result
/// in a panic.
#[derive(Default)]
pub struct BufferChain {
    head_tail: Cell<Option<(BufferPtr, BufferPtr)>>,
}

impl BufferChain {
    pub fn new() -> Self {
        Self {
            head_tail: Cell::new(None),
        }
    }

    /// Insert a single buffer at the end of this chain.
    ///
    /// A buffer can only appear once in a single BufferChain. This method will panic if the
    /// previously inserted buffer is found to have been inserted into another BufferChain.
    pub fn push(&self, buffer: BufferPtr) {
        if let Some((prev_head, prev_tail)) = self.head_tail.get() {
            let prev_tail_next = unsafe { prev_tail.swap_next(Some(buffer)) };
            assert!(prev_tail_next.is_none());
            self.head_tail.set(Some((prev_head, buffer)));
        } else {
            self.head_tail.set(Some((buffer, buffer)));
        }
    }

    pub fn drain(&self) -> BufferChainDrain {
        let head = self.head_tail.take().map(|(head, _)| head);
        BufferChainDrain { head }
    }

    /// Returns Some((head, tail)) if the chain contains at least one buffer.
    /// `head == tail` if the chain contains exactly one buffer.
    /// Returns None if the chain is empty.
    pub(crate) fn take_all(&self) -> Option<(BufferPtr, BufferPtr)> {
        self.head_tail.take()
    }
}

pub struct BufferChainDrain {
    head: Option<BufferPtr>,
}

impl core::iter::Iterator for BufferChainDrain {
    type Item = BufferPtr;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(buffer) = self.head {
            self.head = unsafe { buffer.swap_next(None) };
            return Some(buffer);
        }

        None
    }
}

