
//! generic support for turning any non-blocking queue into a blocking one.

use std::cell::UnsafeCell;
use std::thread::{self, Thread};
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicIsize, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::time::Instant;

use ::{Sends, Receive, GetConsumerAddition, GetProducerAddition};


pub struct Blocking<Q>(Q);

impl<Q> Blocking<Q> {
     pub fn new(q: Q) -> Self {
         Blocking(q)
     }
}

//TODO make newtype so queues cannot mess up the signal token.
pub type AtomicSignalToken = AtomicUsize;

impl<T, Q> Receive<T> for Blocking<Q>
where Q: Receive<T> + GetProducerAddition<AtomicSignalToken> {
    fn try_receive(&mut self) -> Option<T> {
        self.0.try_receive()
    }

    fn receive(&mut self) -> T {
        match self.try_receive() {
            None => {}
            Some(data) => return data,
        }
        'recv: loop {
            // Welp, our channel has no data. Deschedule the current thread and
            // initiate the blocking protocol.
            let (wait_token, signal_token) = tokens();

            // Store the signal token a sender will use to wake us.
            let ptr = unsafe { signal_token.cast_to_usize() };

            self.0.get_producer_addition().store(ptr, Ordering::SeqCst);

            // A sender may have sent while we were storing the token,
            // so we need to double check that the queue is empty.
            match self.try_receive() {
                None => {},
                Some(data) => {
                    let tok = self.0.get_producer_addition().swap(0, Ordering::Relaxed);
                    if tok != 0 { unsafe { mem::drop(SignalToken::cast_from_usize(tok)) } };
                    return data
                }
            }

            // All sends that complete are now guaranteed to wake us,
            // so it is safe to sleep.
            wait_token.wait();

            match self.try_receive() {
                // We get get spurious wakeups under the correct interleaving
                // (send starts for t1, we recv t1, we call receive and sleep, send for t1 wakes us)
                // so if we don't receive data here go back to sleep
                None => continue 'recv,
                Some(data) => return data,
            }
        }
    }
}

impl<T, Q> Sends<T> for Blocking<Q>
where Q: Sends<T> + GetProducerAddition<AtomicSignalToken> {
    fn unconditional_send(&mut self, t: T) {
        self.0.unconditional_send(t);

        if self.0.get_producer_addition().load(Ordering::SeqCst) == 0 {
            return;
        }
        //TODO since we sync above, can this be Relaxed?
        let waker = self.0.get_producer_addition().swap(0, Ordering::Relaxed);
        if waker != 0 {
            let waker = unsafe { SignalToken::cast_from_usize(waker) };
            waker.signal();
        }
    }
}

impl<'queue, T, Q> Receive<T> for &'queue Blocking<Q>
where for<'a> &'a Q: Receive<T>, Q: GetProducerAddition<AtomicSignalToken> {
    fn try_receive(&mut self) -> Option<T> {
        (&self.0).try_receive()
    }

    fn receive(&mut self) -> T {
        match self.try_receive() {
            None => {}
            Some(data) => return data,
        }
        'recv: loop {
            // Welp, our channel has no data. Deschedule the current thread and
            // initiate the blocking protocol.
            let (wait_token, signal_token) = tokens();

            // Store the signal token a sender will use to wake us.
            let ptr = unsafe { signal_token.cast_to_usize() };
            self.0.get_producer_addition().store(ptr, Ordering::SeqCst);

            // A sender may have sent while we were storing the token,
            // so we need to double check that the queue is empty.
            match self.try_receive() {
                None => {},
                Some(data) => {
                    let tok = self.0.get_producer_addition().swap(0, Ordering::Relaxed);
                    if tok != 0 { unsafe { mem::drop(SignalToken::cast_from_usize(tok)) } };
                    return data
                }
            }

            // All sends that complete are now guaranteed to wake us,
            // so it is safe to sleep.
            wait_token.wait();

            match self.try_receive() {
                // We get get spurious wakeups under the correct interleaving
                // (send starts for t1, we recv t1, we call receive and sleep, send for t1 wakes us)
                // so if we don't receive data here go back to sleep
                None => continue 'recv,
                Some(data) => return data,
            }
        }
    }
}

impl<'queue, T, Q> Sends<T> for &'queue Blocking<Q>
where for<'a> &'a Q: Sends<T>, Q: GetProducerAddition<AtomicSignalToken> {
    fn unconditional_send(&mut self, t: T) {
        (&self.0).unconditional_send(t);

        if self.0.get_producer_addition().load(Ordering::SeqCst) == 0 {
            return;
        }
        //TODO since we sync above, can this be Relaxed?
        let waker = self.0.get_producer_addition().swap(0, Ordering::Relaxed);
        if waker != 0 {
            let waker = unsafe { SignalToken::cast_from_usize(waker) };
            waker.signal();
        }
    }
}

////////////////////////////////////////

// two versions of the counter based blocker from https://github.com/rust-lang/rust/blob/d88736905e403ce3d0a68648fe1cd77dffad3641/src/libstd/sync/mpsc/stream.rs

pub struct CompactCounterBlocking<Q>(Q);

impl<Q> CompactCounterBlocking<Q> {
     pub fn new(q: Q) -> Self {
         CompactCounterBlocking(q)
     }
}

impl<'queue, T, Q> Receive<T> for &'queue CompactCounterBlocking<Q>
where
    for<'a> &'a Q: Receive<T>,
    Q: GetProducerAddition<(AtomicSignalToken, AtomicIsize)> + GetConsumerAddition<UnsafeCell<isize>> {

    fn receive(&mut self) -> T {
        match self.try_receive() {
            None => {}
            Some(data) => return data,
        }
        // Welp, our channel has no data. Deschedule the current thread and
        // initiate the blocking protocol.
        let (wait_token, signal_token) = tokens();
        assert_eq!(self.0.get_producer_addition().0.load(Ordering::SeqCst), 0);
        // Store the signal token a sender will use to wake us.
        let ptr = unsafe { signal_token.cast_to_usize() };
        self.0.get_producer_addition().0.store(ptr, Ordering::SeqCst);

        let steals = unsafe { ptr::replace(self.0.get_consumer_addition().get(), 0) };

        match self.0.get_producer_addition().1.fetch_sub(1 + steals, Ordering::SeqCst) {
            n if n - steals <= 0 => {
                // All sends that complete are now guaranteed to wake us,
                // so it is safe to sleep.
                wait_token.wait();
            }
            _ => {
                self.0.get_producer_addition().0.store(0, Ordering::SeqCst);
                unsafe { SignalToken::cast_from_usize(ptr) };
            }
        }

        match self.try_receive() {
            // We get get spurious wakeups under the correct interleaving
            // (send starts for t1, we recv t1, we call receive and sleep, send for t1 wakes us)
            // so if we don't receive data here go back to sleep
            None => unreachable!(),
            Some(data) => unsafe {
                *self.0.get_consumer_addition().get() -= 1;
                data
            },
        }
    }

    fn try_receive(&mut self) -> Option<T> {
        match (&self.0).try_receive() {
            None => None,
            Some(data) => unsafe {
                const MAX_STEALS: isize = 1 << 20;
                if *self.0.get_consumer_addition().get() > MAX_STEALS {
                    let n = self.0.get_producer_addition().1.swap(0, Ordering::SeqCst);
                    let m = ::std::cmp::min(n, *self.0.get_consumer_addition().get());
                    *self.0.get_consumer_addition().get() -= m;
                    self.0.get_producer_addition().1.fetch_add(n - m, Ordering::SeqCst);
                    assert!(*self.0.get_consumer_addition().get() >= 0);
                }
                *self.0.get_consumer_addition().get() += 1;
                Some(data)
            }
        }
    }
}

impl<'queue, T, Q> Sends<T> for &'queue CompactCounterBlocking<Q>
where for<'a> &'a Q: Sends<T>, Q: GetProducerAddition<(AtomicSignalToken, AtomicIsize)> {
    fn unconditional_send(&mut self, t: T) {
        (&self.0).unconditional_send(t);

        match self.0.get_producer_addition().1.fetch_add(1, Ordering::SeqCst) {
            n if n >= 0 => return, // success!
            -1 => {
                let waker = self.0.get_producer_addition().0.load(Ordering::SeqCst);
                self.0.get_producer_addition().0.store(0, Ordering::SeqCst);
                assert!(waker != 0);
                unsafe { SignalToken::cast_from_usize(waker).signal() };
            }
            n => return, //FIXME check mpsc::shared for this logic
        }
    }
}

pub struct SeparateCounterBlocking<Q>(Q, ::CacheAligned<AtomicIsize>);

impl<Q> SeparateCounterBlocking<Q> {
     pub fn new(q: Q) -> Self {
         SeparateCounterBlocking(q, ::CacheAligned::new(AtomicIsize::new(0)))
     }
}

impl<'queue, T, Q> Receive<T> for &'queue SeparateCounterBlocking<Q>
where
    for<'a> &'a Q: Receive<T>,
    Q: GetProducerAddition<AtomicSignalToken> + GetConsumerAddition<UnsafeCell<isize>> {

    fn receive(&mut self) -> T {
        match self.try_receive() {
            None => {}
            Some(data) => return data,
        }
        // Welp, our channel has no data. Deschedule the current thread and
        // initiate the blocking protocol.
        let (wait_token, signal_token) = tokens();
        assert_eq!(self.0.get_producer_addition().load(Ordering::SeqCst), 0);
        // Store the signal token a sender will use to wake us.
        let ptr = unsafe { signal_token.cast_to_usize() };
        self.0.get_producer_addition().store(ptr, Ordering::SeqCst);

        let steals = unsafe { ptr::replace(self.0.get_consumer_addition().get(), 0) };

        match self.1.fetch_sub(1 + steals, Ordering::SeqCst) {
            n if n - steals <= 0 => {
                // All sends that complete are now guaranteed to wake us,
                // so it is safe to sleep.
                wait_token.wait();
            }
            _ => {
                self.0.get_producer_addition().store(0, Ordering::SeqCst);
                unsafe { SignalToken::cast_from_usize(ptr) };
            }
        }

        match self.try_receive() {
            // We get get spurious wakeups under the correct interleaving
            // (send starts for t1, we recv t1, we call receive and sleep, send for t1 wakes us)
            // so if we don't receive data here go back to sleep
            None => unreachable!(),
            Some(data) => unsafe {
                *self.0.get_consumer_addition().get() -= 1;
                data
            },
        }
    }

    fn try_receive(&mut self) -> Option<T> {
        match (&self.0).try_receive() {
            None => None,
            Some(data) => unsafe {
                const MAX_STEALS: isize = 1 << 20;
                if *self.0.get_consumer_addition().get() > MAX_STEALS {
                    let n = self.1.swap(0, Ordering::SeqCst);
                    let m = ::std::cmp::min(n, *self.0.get_consumer_addition().get());
                    *self.0.get_consumer_addition().get() -= m;
                    self.1.fetch_add(n - m, Ordering::SeqCst);
                    assert!(*self.0.get_consumer_addition().get() >= 0);
                }
                *self.0.get_consumer_addition().get() += 1;
                Some(data)
            }
        }
    }
}

impl<'queue, T, Q> Sends<T> for &'queue SeparateCounterBlocking<Q>
where for<'a> &'a Q: Sends<T>, Q: GetProducerAddition<AtomicSignalToken> {
    fn unconditional_send(&mut self, t: T) {
        (&self.0).unconditional_send(t);

        match self.1.fetch_add(1, Ordering::SeqCst) {
            n if n >= 0 => return, // success!
            -1 => {
                let waker = self.0.get_producer_addition().load(Ordering::SeqCst);
                self.0.get_producer_addition().store(0, Ordering::SeqCst);
                assert!(waker != 0);
                unsafe { SignalToken::cast_from_usize(waker).signal() };
            }
            n => return, //FIXME check mpsc::shared for this logic
        }
    }
}

////////////////////////////////////////

// the following based on
//   https://github.com/rust-lang/rust/blob/1fd3a42c624faf91e9402942419ec409699fb94a/src/libstd/sync/mpsc/blocking.rs
// Copyright 2014 The Rust Project Developers.

struct Inner {
    thread: Thread,
    woken: AtomicBool,
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

#[derive(Clone)]
pub struct SignalToken {
    inner: Arc<Inner>,
}

pub struct WaitToken {
    inner: Arc<Inner>,
    no_send_sync: PhantomData<*const ()>,
}

pub fn tokens() -> (WaitToken, SignalToken) {
    let inner = Arc::new(Inner {
        thread: thread::current(),
        woken: AtomicBool::new(false),
    });
    let wait_token = WaitToken {
        inner: inner.clone(),
        no_send_sync: PhantomData,
    };
    let signal_token = SignalToken {
        inner,
    };
    (wait_token, signal_token)
}

impl SignalToken {
    pub fn signal(&self) -> bool {
        let wake = !self.inner.woken.compare_and_swap(false, true, Ordering::SeqCst);
        if wake {
            self.inner.thread.unpark();
        }
        wake
    }

    /// Convert to an unsafe usize value. Useful for storing in a pipe's state
    /// flag.
    #[inline]
    pub unsafe fn cast_to_usize(self) -> usize {
        mem::transmute(self.inner)
    }

    /// Convert from an unsafe usize value. Useful for retrieving a pipe's state
    /// flag.
    #[inline]
    pub unsafe fn cast_from_usize(signal_ptr: usize) -> SignalToken {
        SignalToken { inner: mem::transmute(signal_ptr) }
    }
}

impl WaitToken {
    pub fn wait(self) {
        while !self.inner.woken.load(Ordering::SeqCst) {
            thread::park()
        }
    }

    /// Returns true if we wake up normally, false otherwise.
    pub fn wait_max_until(self, end: Instant) -> bool {
        while !self.inner.woken.load(Ordering::SeqCst) {
            let now = Instant::now();
            if now >= end {
                return false;
            }
            thread::park_timeout(end - now)
        }
        true
    }
}
