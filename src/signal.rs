#[cfg(feature = "std")]
use std::sync::{Arc, Weak};
#[cfg(feature = "alloc")]
use alloc::sync::{Arc, Weak};

use core::sync::atomic::{AtomicBool, Ordering};

#[cfg(any(feature = "std", feature = "alloc"))]
use crossbeam_utils::atomic::AtomicCell;
use waitq::WaiterQueue;

pub struct Signal {
    is_notified: AtomicBool,
    waiter_queue: WaiterQueue<()>,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            is_notified: AtomicBool::new(false),
            waiter_queue: WaiterQueue::new(),
        }
    }

    pub fn is_notified(&self) -> bool {
        self.is_notified.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.is_notified.store(false, Ordering::Relaxed);
    }

    pub fn notify(&self) {
        self.is_notified.store(true, Ordering::Relaxed);
        self.waiter_queue.lock().notify_all(());
    }

    pub async fn wait(&self) {
        self.waiter_queue.wait_until(|| self.is_notified()).await;
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    pub async fn wait_arc(self: Arc<Self>) {
        self.wait().await;
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
struct SignalTreeNode {
    signal: Signal,
    children_head_tail: spin::Mutex<Option<(Arc<Self>, Arc<Self>)>>,
    // TODO - we always lock the parent's children_head_tail when accessing next_sibling,
    // so with some care we can probably switch this to an UnsafeCell instead of AtomicCell.
    next_sibling: AtomicCell<Option<Arc<Self>>>,
    parent: spin::Mutex<SignalTreeNodeParent>,
}

#[cfg(any(feature = "std", feature = "alloc"))]
struct SignalTreeNodeParent {
    parent: Weak<SignalTreeNode>,
    previous_sibling: Weak<SignalTreeNode>,
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl SignalTreeNode {
    fn notify(&self) {
        self.signal.notify();

        // Notify children
        let mut next_child = self.children_head_tail.lock().take().map(|(h, _)| h);
        while let Some(child) = next_child {
            child.parent.lock().parent = Weak::new();
            child.notify();
            next_child = child.next_sibling.take();
        }
    }

    fn remove_from_parent(&self) {
        let parent = self.parent.lock();
        let Some(parent_node) = parent.parent.upgrade() else {
            return;
        };

        // Remove this node from the chain of siblings
        let next_sibling = self.next_sibling.take();
        let previous_sibling = parent.previous_sibling.upgrade();
        if let Some(previous_sibling) = &previous_sibling {
            previous_sibling.next_sibling.store(next_sibling.clone());
        }

        // If this node is the head or tail sibling, update the parent's head/tail children
        // pointers.
        let mut parent_children_head_tail = parent_node.children_head_tail.lock();
        if let Some((head, tail)) = parent_children_head_tail.as_mut() {
            let is_head = Arc::as_ptr(head) == self as *const Self;
            let is_tail = Arc::as_ptr(tail) == self as *const Self;

            if is_head && is_tail {
                *parent_children_head_tail = None;
            } else if is_head {
                *head = next_sibling.unwrap();
            } else if is_tail {
                *tail = previous_sibling.unwrap();
            }
        }
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl Drop for SignalTreeNode {
    fn drop(&mut self) {
        self.remove_from_parent();
    }
}

#[cfg(any(feature = "std", feature = "alloc"))]
#[derive(Clone)]
pub struct SignalTree {
    node: Arc<SignalTreeNode>,
}

#[cfg(any(feature = "std", feature = "alloc"))]
impl SignalTree {
    pub fn new() -> Self {
        Self {
            node: Arc::new(SignalTreeNode {
                signal: Signal::new(),
                children_head_tail: spin::Mutex::new(None),
                next_sibling: AtomicCell::new(None),
                parent: spin::Mutex::new(SignalTreeNodeParent {
                    parent: Weak::new(),
                    previous_sibling: Weak::new(),
                }),
            }),
        }
    }

    pub fn add_child(&self, child: Self) {
        match self.node.signal.is_notified.compare_exchange(
            false,
            false,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => { }
            Err(_) => {
                // Already notified - immediately notify the new child and return early.
                child.notify();
                return;
            }
        }

        let mut child_parent = child.node.parent.lock();
        if child_parent.parent.upgrade().is_some() {
            panic!("A bab::Signal cannot have multiple parents");
        }

        let mut children_head_tail = self.node.children_head_tail.lock();
        if let Some((_, tail)) = children_head_tail.as_mut() {
            child_parent.parent = Arc::downgrade(&self.node);
            child_parent.previous_sibling = Arc::downgrade(&tail);
            drop(child_parent);

            assert!(
                tail.next_sibling.swap(Some(child.node.clone()))
                    .is_none()
            );
            *tail = child.node;
        } else {
            child_parent.parent = Arc::downgrade(&self.node);
            child_parent.previous_sibling = Weak::new();
            drop(child_parent);

            *children_head_tail = Some((child.node.clone(), child.node));
        }
    }

    pub fn is_notified(&self) -> bool {
        self.node.signal.is_notified()
    }

    pub fn notify(&self) {
        self.node.remove_from_parent();
        self.node.notify();
    }

    pub fn remove_from_parent(&self) {
        self.node.remove_from_parent();
    }

    pub async fn wait(&self) {
        self.node.signal.wait().await;
    }

    pub async fn wait_owned(self) {
        self.node.signal.wait().await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_signal() {
        use std::rc::Rc;

        let ex = async_executor::LocalExecutor::new();

        pollster::block_on(ex.run(async {
            let waiter_count = 4;
            let task_starts = Rc::new(async_unsync::semaphore::Semaphore::new(0));
            let task_completes = Rc::new(async_unsync::semaphore::Semaphore::new(0));
            let signal = Rc::new(Signal::new());

            for _ in 0..waiter_count {
                let task_starts = task_starts.clone();
                let task_completes = task_completes.clone();
                let signal = signal.clone();
                ex.spawn(async move {
                    task_starts.add_permits(1);
                    signal.wait().await;
                    task_completes.add_permits(1);
                })
                .detach();
            }

            for _ in 0..waiter_count {
                task_starts.acquire().await.unwrap().forget();
            }
            assert!(task_starts.try_acquire().is_err());

            signal.notify();

            for _ in 0..waiter_count {
                task_completes.acquire().await.unwrap().forget();
            }
            assert!(task_completes.try_acquire().is_err());
        }));
    }

    #[test]
    fn test_signal_tree() {
        let root = SignalTree::new();

        let level1_a = SignalTree::new();
        root.add_child(level1_a.clone());

        let level1_b = SignalTree::new();
        root.add_child(level1_b.clone());

        let level1_c = SignalTree::new();
        root.add_child(level1_c.clone());

        let level2_a = SignalTree::new();
        level1_b.add_child(level2_a.clone());

        let level2_b = SignalTree::new();
        level1_b.add_child(level2_b.clone());

        let level2_c = SignalTree::new();
        level1_b.add_child(level2_c.clone());

        let level2_d = SignalTree::new();
        level1_a.add_child(level2_d.clone());

        level1_a.notify();

        assert!(!root.is_notified());
        assert!(level1_a.is_notified());
        assert!(!level1_b.is_notified());
        assert!(!level1_c.is_notified());
        assert!(!level2_a.is_notified());
        assert!(!level2_b.is_notified());
        assert!(!level2_c.is_notified());
        assert!(level2_d.is_notified());

        level1_c.remove_from_parent();
        root.notify();

        assert!(root.is_notified());
        assert!(level1_a.is_notified());
        assert!(level1_b.is_notified());
        assert!(!level1_c.is_notified());
        assert!(level2_a.is_notified());
        assert!(level2_b.is_notified());
        assert!(level2_c.is_notified());
        assert!(level2_d.is_notified());
    }
}
