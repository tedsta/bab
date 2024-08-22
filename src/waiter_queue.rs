use core::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::thread_local::ThreadLocal;

pub trait IFulfillment {
    fn take_one(&mut self) -> Self;

    fn append(&mut self, other: Self, other_count: usize);
}

impl IFulfillment for () {
    fn take_one(&mut self) -> Self { () }

    fn append(&mut self, _other: Self, _other_count: usize) { }
}

pub struct Fulfillment<T> {
    pub count: usize,
    pub inner: T,
}

impl<T: IFulfillment> Fulfillment<T> {
    pub fn take_one(&mut self) -> T {
        self.count -= 1;
        self.inner.take_one()
    }

    pub fn append(&mut self, other: Self) {
        self.inner.append(other.inner, other.count);
        self.count += other.count;
    }
}

struct WaiterQueueState<T> {
    front: Option<NonNull<WaiterNode<T>>>,
    back: Option<NonNull<WaiterNode<T>>>,
    count: usize,
}

pub struct WaiterQueue<T> {
    state: spin::Mutex<WaiterQueueState<T>>,
    local: ThreadLocal<Local<T>>,
}

pub struct WaiterNode<T> {
    state: spin::Mutex<WaiterNodeState<T>>,

    // SAFETY: These can only be accessed while the WaiterQueue lock is held.
    previous: UnsafeCell<Option<NonNull<Self>>>,
    next: UnsafeCell<Option<NonNull<Self>>>,

    // SAFETY: These can only be accessed from the awaiting task's thread, and that task cannot be
    // `Send`.
    local_lifecycle: Cell<WaiterLifecycle>,
    local_state: UnsafeCell<WaiterNodeState<T>>,
    local_next: Cell<Option<NonNull<Self>>>,
    local_prev: Cell<Option<NonNull<Self>>>,
}

pub enum WaiterNodeState<T> {
    Pending,
    Polled {
        waker: Waker,
    },
    Notified {
        fulfillment: Fulfillment<T>,
    },
    Releasing,
}

unsafe impl<T> Send for WaiterQueue<T> { }
unsafe impl<T> Sync for WaiterQueue<T> { }

impl<T> WaiterNode<T> {
    pub fn new() -> Self {
        Self {
            previous: UnsafeCell::new(None),
            next: UnsafeCell::new(None),
            state: spin::Mutex::new(WaiterNodeState::Pending),
            local_lifecycle: Cell::new(WaiterLifecycle::Unregistered),
            local_state: UnsafeCell::new(WaiterNodeState::Pending),
            local_next: Cell::new(None),
            local_prev: Cell::new(None),
        }
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut WaiterNodeState<T>) -> R) -> R {
        f(&mut self.state.lock())
    }

    fn fulfill(&self, fulfillment: Fulfillment<T>) -> Option<Waker> {
        self.with_state(|state| {
            Self::fulfill_common(state, fulfillment)
        })
    }

    fn fulfill_local(&self, fulfillment: Fulfillment<T>) -> Option<Waker> {
        let state = unsafe { &mut *self.local_state.get() };
        Self::fulfill_common(state, fulfillment)
    }

    fn fulfill_common(state: &mut WaiterNodeState<T>, fulfillment: Fulfillment<T>) -> Option<Waker> {
        match state {
            WaiterNodeState::Pending => {
                *state = WaiterNodeState::Notified { fulfillment };
                None
            }
            WaiterNodeState::Polled { .. } => {
                let WaiterNodeState::Polled { waker } =
                    core::mem::replace(&mut *state, WaiterNodeState::Notified { fulfillment })
                else {
                    unreachable!();
                };
                Some(waker)
            }
            // A WaiterNode can be notified exactly once.
            WaiterNodeState::Notified { .. } => unreachable!(),
            // A WaiterNode shouldn't be reachable after it's been dropped.
            WaiterNodeState::Releasing => unreachable!(),
        }
    }
}

struct Local<T> {
    nodes: Cell<Option<NonNull<WaiterNode<T>>>>,
    node_count: Cell<usize>,
}

unsafe impl<T> Send for Local<T> { }

impl<T> Default for Local<T> {
    fn default() -> Self {
        Self {
            nodes: Cell::new(None),
            node_count: Cell::new(0),
        }
    }
}

impl<T> Local<T> {
    fn add_node(&self, new_node: NonNull<WaiterNode<T>>) {
        let next = self.nodes.replace(Some(new_node));
        let new_node_ref = unsafe { new_node.as_ref() };
        new_node_ref.local_next.set(next);
        if let Some(next) = next {
            unsafe { next.as_ref() }.local_prev.set(Some(new_node));
        }

        self.node_count.set(self.node_count.get() + 1);
    }

    fn remove_node(&self, to_remove: &WaiterNode<T>) {
        let prev = to_remove.local_prev.replace(None);
        let next = to_remove.local_next.replace(None);

        if prev.is_none() && next.is_none() && self.nodes.get() != Some(NonNull::from(to_remove)) {
            return;
        }

        self.node_count.set(self.node_count.get() - 1);

        if let Some(next) = next {
            unsafe { next.as_ref() }.local_prev.set(prev);
        }
        if let Some(prev) = prev {
            unsafe { prev.as_ref() }.local_next.set(next);
        } else {
            // This is the head node.
            let prev_head = self.nodes.replace(next);
            debug_assert_eq!(
                prev_head,
                Some(NonNull::from(to_remove)),
            );
        }
    }
}

pub struct WaiterQueueGuard<'a, T> {
    state: spin::MutexGuard<'a, WaiterQueueState<T>>,
}

impl<T> WaiterQueueGuard<'_, T> {
    pub fn waiter_count(&self) -> usize {
        self.state.count
    }
}

impl<T: IFulfillment> WaiterQueue<T> {
    pub fn new() -> Self {
        Self {
            state: spin::Mutex::new(WaiterQueueState {
                front: None,
                back: None,
                count: 0,
            }),
            local: ThreadLocal::new(),
        }
    }

    pub fn lock(&self) -> WaiterQueueGuard<T> {
        WaiterQueueGuard {
            state: self.state.lock(),
        }
    }

    pub fn wait(&self) -> Waiter<T> {
        Waiter::new(&self)
    }

    pub fn notify_one_local(&self, fulfillment: T) -> Option<T> {
        let local = self.local.get_or_default();

        if let Some(local_head) = local.nodes.get() {
            debug_assert!(unsafe { local_head.as_ref() }.local_prev.get().is_none());

            let local_next = unsafe { local_head.as_ref() }.local_next.replace(None);
            local.nodes.set(local_next);
            local.node_count.set(local.node_count.get() - 1);

            if let Some(local_next) = local_next {
                unsafe { local_next.as_ref() }.local_prev.set(None);

                // This node is only registered locally.
                let fulfillment = Fulfillment { inner: fulfillment, count: 1 };
                if let Some(waker) = unsafe { local_head.as_ref() }.fulfill_local(fulfillment) {
                    waker.wake();
                }

                None
            } else {
                // This was the last node in the local stack, so it is registered in the shared
                // queue.
                debug_assert_eq!(local.node_count.get(), 0);
                debug_assert_eq!(unsafe { local_head.as_ref() }.local_lifecycle.get(), WaiterLifecycle::Registered);
                let mut guard = self.lock();
                if guard.remove_waiter(local_head) {
                    // This waiter hasn't been notified yet. Convert it to a local registration and fulfill it.
                    unsafe { local_head.as_ref() }.local_lifecycle.set(WaiterLifecycle::RegisteredLocal);
                    let fulfillment = Fulfillment { inner: fulfillment, count: 1 };
                    if let Some(waker) = unsafe { local_head.as_ref() }.fulfill_local(fulfillment) {
                        waker.wake();
                    }

                    None
                } else {
                    Some(fulfillment)
                }
            }
        } else {
            Some(fulfillment)
        }
    }

    fn remove_local_waiter(&self, to_remove: &WaiterNode<T>) {
        let local = self.local.get_or_default();
        local.remove_node(to_remove);
    }
}

impl<T> WaiterQueueGuard<'_, T> {
    pub fn notify(&mut self, fulfillment: T, count: usize) -> bool {
        let Some(front_ptr) = self.state.front else {
            // There are currently no waiters.
            return false;
        };

        self.state.count -= 1;

        // Advance the front cursor
        let next_ptr = core::mem::replace(
            unsafe { &mut *front_ptr.as_ref().next.get() },
            None,
        );
        self.state.front = next_ptr;

        if let Some(new_front_ptr) = self.state.front {
            unsafe { *new_front_ptr.as_ref().previous.get() = None };
        } else {
            debug_assert_eq!(Some(front_ptr), self.state.back);
            debug_assert!(unsafe { *front_ptr.as_ref().previous.get() }.is_none());

            // We've reached the end of the waiter list - clear the `back` pointer.
            self.state.back = None;
        }

        let maybe_waker = unsafe { front_ptr.as_ref() }.fulfill(Fulfillment {
            inner: fulfillment,
            count,
        });
        if let Some(waker) = maybe_waker {
            waker.wake();
        }

        true
    }

    /// Returns true if the waiter was removed.
    /// Returns false if the waiter had already been removed before this call.
    fn remove_waiter(
        &mut self,
        node: NonNull<WaiterNode<T>>,
    ) -> bool {
        let prev = unsafe { *node.as_ref().previous.get() };
        let next = unsafe { *node.as_ref().next.get() };

        if prev.is_none() && next.is_none() && self.state.front != Some(node) {
            // This waiter has already been removed.
            return false;
        }

        self.state.count -= 1;

        unsafe { *node.as_ref().next.get() = None; }
        unsafe { *node.as_ref().previous.get() = None; }

        // Check if we are removing the back node, move `back` pointer to earlier waiter in line.
        if Some(node) == self.state.back {
            self.state.back = prev;
            debug_assert!(next.is_none());
        }

        if Some(node) == self.state.front {
            // We are removing the front node
            self.state.front = next;
            if let Some(next) = next {
                // SAFETY: the shared lock protects access to all `previous` values.
                unsafe { *next.as_ref().previous.get() = None; }
            } else {
                debug_assert!(self.state.back.is_none());
            }
        } else if let Some(prev) = prev {
            // Previous node is guaranteed not to be the back node, and we have the shared lock, so
            // we have exclusive access to `next`.
            unsafe { *prev.as_ref().next.get() = next };
            if let Some(next) = next {
                // SAFETY: the shared lock protects access to all `previous` values.
                unsafe { *next.as_ref().previous.get() = Some(prev); }
            }
        }

        true
    }

    fn add_waiter(
        &mut self,
        new_node: NonNull<WaiterNode<T>>,
    ) {
        let state = &mut self.state;
        state.count += 1;

        debug_assert!(unsafe { (*new_node.as_ref().next.get()).is_none() });
        debug_assert!(unsafe { (*new_node.as_ref().previous.get()).is_none() });

        let prev_back = core::mem::replace(
            &mut state.back,
            Some(new_node),
        );
        if let Some(prev_back) = prev_back {
            unsafe {
                // Set my node's previous node.
                *new_node.as_ref().previous.get() = Some(prev_back);
                // Link my node as next after the previous `back`.
                *prev_back.as_ref().next.get() = Some(new_node);
            }
        } else {
            // We are the first in line - set the queue's front.
            state.front = Some(new_node);
            debug_assert!(unsafe { &*new_node.as_ref().next.get() }.is_none());
            debug_assert!(unsafe { &*new_node.as_ref().previous.get() }.is_none());
        }
    }
}

impl<T: Copy> WaiterQueueGuard<'_, T> {
    pub fn notify_all(&mut self, fulfillment: T) -> usize {
        let mut notified_count = 0;

        while let Some(front_ptr) = self.state.front {
            notified_count += 1;
            self.state.count -= 1;

            // Advance the front cursor
            let next_ptr = core::mem::replace(
                unsafe { &mut *front_ptr.as_ref().next.get() },
                None,
            );
            self.state.front = next_ptr;

            if let Some(new_front_ptr) = self.state.front {
                unsafe { *new_front_ptr.as_ref().previous.get() = None };
            } else {
                debug_assert_eq!(Some(front_ptr), self.state.back);
                debug_assert!(unsafe { *front_ptr.as_ref().previous.get() }.is_none());

                // We've reached the end of the waiter list - clear the `back` pointer.
                self.state.back = None;
            }

            let maybe_waker = unsafe { front_ptr.as_ref() }.fulfill(Fulfillment {
                inner: fulfillment,
                count: usize::MAX,
            });
            if let Some(waker) = maybe_waker {
                waker.wake();
            }
        }

        notified_count
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WaiterLifecycle {
    Unregistered,
    Registered,
    RegisteredLocal,
    Releasing,
}

pub struct Waiter<'a, T: IFulfillment> {
    waiter_queue: &'a WaiterQueue<T>,
    waiter_node: UnsafeCell<WaiterNode<T>>,
}

impl<'a, T: IFulfillment> Waiter<'a, T> {
    pub fn new(waiter_queue: &'a WaiterQueue<T>) -> Self {
        Self {
            waiter_queue,
            waiter_node: UnsafeCell::new(WaiterNode::new()),
        }
    }

    fn lifecycle(&self) -> WaiterLifecycle {
        unsafe { &*self.waiter_node.get() }.local_lifecycle.get()
    }

    fn set_lifecycle(&self, new_value: WaiterLifecycle) {
        unsafe { &*self.waiter_node.get() }.local_lifecycle.set(new_value);
    }

    fn register(
        self: Pin<&Self>,
        mut try_fulfill: impl FnMut() -> Option<Fulfillment<T>>,
    ) -> Option<Fulfillment<T>> {
        if self.lifecycle() != WaiterLifecycle::Unregistered {
            return None;
        }

        let local = self.waiter_queue.local.get_or_default();
        let waiter_node_ptr = NonNull::from(unsafe { &*self.waiter_node.get() });

        if local.nodes.get().is_some() {
            self.set_lifecycle(WaiterLifecycle::RegisteredLocal);
            local.add_node(waiter_node_ptr);
            None
        } else {
            let mut guard = self.waiter_queue.lock();
            if let Some(fulfillment) = try_fulfill() {
                drop(guard);
                self.set_lifecycle(WaiterLifecycle::Releasing);
                Some(fulfillment)
            } else {
                self.set_lifecycle(WaiterLifecycle::Registered);
                guard.add_waiter(waiter_node_ptr);
                local.add_node(waiter_node_ptr);
                None
            }
        }
    }

    pub fn cancel(&self) -> Option<Fulfillment<T>> {
        match self.lifecycle() {
            WaiterLifecycle::Registered => {
                self.set_lifecycle(WaiterLifecycle::Releasing);

                // Lock waker queue to prevent this node from being notified if it wasn't already notified.
                let mut waiter_queue_guard = self.waiter_queue.lock();

                let waiter_node = unsafe { &*self.waiter_node.get() };
                let mut state = waiter_node.state.lock();
                match core::mem::replace(&mut *state, WaiterNodeState::Releasing) {
                    WaiterNodeState::Notified { fulfillment } => {
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        Some(fulfillment)
                    }
                    // Fulfillment was already processed, so this waiter has already been deregistered.
                    WaiterNodeState::Releasing => None,
                    _ => {
                        // Deregister the waiter.
                        waiter_queue_guard.remove_waiter(NonNull::from(waiter_node));
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        None
                    }
                }
            }
            WaiterLifecycle::RegisteredLocal => {
                self.set_lifecycle(WaiterLifecycle::Releasing);

                let waiter_node = unsafe { &*self.waiter_node.get() };
                let state = unsafe { &mut *waiter_node.local_state.get() };
                match core::mem::replace(&mut *state, WaiterNodeState::Releasing) {
                    WaiterNodeState::Notified { fulfillment } => {
                        // Local waiters are deregistered immediately when fulfilled, so don't need to deregister here.
                        Some(fulfillment)
                    }
                    // Fulfillment was already processed, so this waiter has already been deregistered.
                    WaiterNodeState::Releasing => None,
                    _ => {
                        // Deregister the waiter.
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        None
                    }
                }
            }
            _ => {
                None
            }
        }
    }

    pub fn poll_fulfillment(
        self: Pin<&'_ Self>,
        context: &'_ mut Context<'_>,
        mut try_fulfill: impl FnMut() -> Option<Fulfillment<T>>,
    ) -> Poll<Fulfillment<T>> {
        if let Some(fulfillment) = self.as_ref().register(&mut try_fulfill) {
            return Poll::Ready(fulfillment);
        }

        let waiter_node = unsafe { &*self.waiter_node.get() };

        let update_state = |state: &mut WaiterNodeState<T>| {
            let mut maybe_fulfillment = None;
            let state_ptr = &mut *state as *mut WaiterNodeState<T>;
            let taken_state = unsafe { core::ptr::read(state_ptr) };

            // SAFETY: the match block below must not panic.
            let new_state = match taken_state {
                WaiterNodeState::Pending => {
                    WaiterNodeState::Polled { waker: context.waker().clone() }
                }
                WaiterNodeState::Polled { waker } => {
                    let new_waker = context.waker();
                    if !waker.will_wake(new_waker) {
                        WaiterNodeState::Polled { waker: new_waker.clone() }
                    } else {
                        WaiterNodeState::Polled { waker: waker.clone() }
                    }
                }
                WaiterNodeState::Notified { fulfillment } => {
                    maybe_fulfillment = Some(fulfillment);
                    WaiterNodeState::Releasing
                }
                WaiterNodeState::Releasing => unreachable!(),
            };

            unsafe { state_ptr.write(new_state); }

            maybe_fulfillment
        };

        // Always poll the local state - a waiter can be notified locally even if it is registered
        // in the shared queue.
        let local_state = unsafe { &mut *waiter_node.local_state.get() };
        if let Some(fulfillment) = update_state(local_state) {
            debug_assert_eq!(fulfillment.count, 1);
            debug_assert_eq!(self.lifecycle(), WaiterLifecycle::RegisteredLocal);
            self.set_lifecycle(WaiterLifecycle::Releasing);
            return Poll::Ready(fulfillment);
        }

        if self.as_ref().lifecycle() == WaiterLifecycle::Registered {
            // This waiter is registered in the shared queue.

            let mut state = waiter_node.state.lock();
            if let Some(mut fulfillment) = update_state(&mut state) {
                self.set_lifecycle(WaiterLifecycle::Releasing);

                // Try to fulfill local waiters.
                while fulfillment.count > 1 && waiter_node.local_prev.get().is_some() {
                    if self.as_ref().waiter_queue.notify_one_local(fulfillment.take_one()).is_some() {
                        panic!("failed to notify local waiter unexpectedly");
                    }
                }

                // An invariant to uphold is: if there are any local waiters, the oldest of them
                // must be registered in the shared queue. So here we must upgrade the next oldest
                // local waiter to be registered in the shared queue if such a waiter exists.

                let next_oldest_local_waiter = waiter_node.local_prev.get();
                self.waiter_queue.remove_local_waiter(waiter_node);

                if let Some(upgrade_waiter_node) = next_oldest_local_waiter {
                    // Before registering the waiter in the shared queue, set its shared state from
                    // its local state in case it was already polled (so that the waker is properly set).
                    let upgrade_waiter_node_ref = unsafe { upgrade_waiter_node.as_ref() };
                    *upgrade_waiter_node_ref.state.lock() = match unsafe { &*upgrade_waiter_node_ref.local_state.get() } {
                        WaiterNodeState::Pending => WaiterNodeState::Pending,
                        WaiterNodeState::Polled { waker } => WaiterNodeState::Polled { waker: waker.clone() },
                        WaiterNodeState::Notified { .. } => unreachable!(),
                        WaiterNodeState::Releasing => unreachable!(),
                    };

                    self.as_ref().waiter_queue.lock().add_waiter(upgrade_waiter_node);
                    unsafe { upgrade_waiter_node.as_ref() }.local_lifecycle.set(WaiterLifecycle::Registered);

                    // Try to fulfill the waiter after registering it.
                    'try_fulfill: while let Some(new_fulfillment) = try_fulfill() {
                        fulfillment.append(new_fulfillment);

                        // Try to fulfill local waiters.
                        while fulfillment.count > 1 {
                            if let Some(unused_fulfillment) =
                                self.as_ref().waiter_queue.notify_one_local(fulfillment.take_one())
                            {
                                // All local waiters have been fulfilled.
                                fulfillment.append(Fulfillment { inner: unused_fulfillment, count: 1 });
                                break 'try_fulfill;
                            }
                        }
                    }
                }

                return Poll::Ready(fulfillment);
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add_local_node() {
        let a = WaiterNode::new();
        let b = WaiterNode::new();
        let c = WaiterNode::new();

        let local = Local::<()>::default();

        local.add_node(NonNull::from(&c));
        local.add_node(NonNull::from(&b));
        local.add_node(NonNull::from(&a));

        assert_eq!(local.nodes.get(), Some(NonNull::from(&a)));
        assert_eq!(a.local_prev.get(), None);
        assert_eq!(a.local_next.get(), Some(NonNull::from(&b)));
        assert_eq!(b.local_prev.get(), Some(NonNull::from(&a)));
        assert_eq!(b.local_next.get(), Some(NonNull::from(&c)));
        assert_eq!(c.local_prev.get(), Some(NonNull::from(&b)));
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&b);

        assert_eq!(local.nodes.get(), Some(NonNull::from(&a)));
        assert_eq!(a.local_prev.get(), None);
        assert_eq!(a.local_next.get(), Some(NonNull::from(&c)));
        assert_eq!(c.local_prev.get(), Some(NonNull::from(&a)));
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&a);

        assert_eq!(local.nodes.get(), Some(NonNull::from(&c)));
        assert_eq!(c.local_prev.get(), None);
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&c);

        assert_eq!(local.nodes.get(), None);
    }

    #[test]
    fn test_add_waiter() {
        let waiter_queue = WaiterQueue::<()>::new();

        let a = WaiterNode::new();
        let b = WaiterNode::new();
        let c = WaiterNode::new();

        let mut guard = waiter_queue.lock();

        guard.add_waiter(NonNull::from(&a));
        guard.add_waiter(NonNull::from(&b));
        guard.add_waiter(NonNull::from(&c));

        assert!(guard.remove_waiter(NonNull::from(&b)));
        assert!(guard.remove_waiter(NonNull::from(&a)));
        assert!(guard.remove_waiter(NonNull::from(&c)));

        assert!(!guard.remove_waiter(NonNull::from(&a)));
        assert!(!guard.remove_waiter(NonNull::from(&b)));
        assert!(!guard.remove_waiter(NonNull::from(&c)));
    }

    #[test]
    fn test_register_waiter() {
        let waiter_queue = WaiterQueue::<()>::new();

        let a = core::pin::pin!(Waiter::new(&waiter_queue));
        let b = core::pin::pin!(Waiter::new(&waiter_queue));
        let c = core::pin::pin!(Waiter::new(&waiter_queue));

        a.as_ref().register(|| { None });
        b.as_ref().register(|| { None });
        c.as_ref().register(|| { None });

        assert!(b.cancel().is_none());
        assert!(a.cancel().is_none());
        assert!(c.cancel().is_none());
    }
}