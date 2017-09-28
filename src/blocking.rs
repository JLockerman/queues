
//! generic support for turning any non-blocking queue into a blocking one.

use std::thread::{self, Thread};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use std::mem;
use std::time::Instant;

use ::{Sends, Receive, TryReceive, StoreSignalToken, TakeSignalToken};


pub struct Blocking<Q>(Q);

impl<Q> Blocking<Q> {
     pub fn new(q: Q) -> Self {
         Blocking(q)
     }
}

impl<T, Q> TryReceive<T> for Blocking<Q>
where Q: TryReceive<T> {
    fn try_receive(&mut self) -> Option<T> {
        self.0.try_receive()
    }
}

impl<T, Q> Receive<T> for Blocking<Q>
where Q: TryReceive<T> + StoreSignalToken {
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
            self.0.store_signal_token(ptr);

            // A sender may have sent while we were storing the token,
            // so we need to double check that the queue is empty.
            match self.try_receive() {
                None => {},
                Some(data) => {
                    self.0.store_signal_token(0);
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
where Q: Sends<T> + TakeSignalToken {
    fn unconditional_send(&mut self, t: T) {
        self.0.unconditional_send(t);

        let waker = self.0.take_signal_token();
        if waker != 0 {
            let waker = unsafe { SignalToken::cast_from_usize(waker) };
            waker.signal();
        }
    }
}


impl<'queue, T, Q> TryReceive<T> for &'queue Blocking<Q>
where for<'a> &'a Q: TryReceive<T> {
    fn try_receive(&mut self) -> Option<T> {
        (&self.0).try_receive()
    }
}

impl<'queue, T, Q> Receive<T> for &'queue Blocking<Q>
where for<'a> &'a Q: TryReceive<T> + StoreSignalToken {
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
            (&self.0).store_signal_token(ptr);

            // A sender may have sent while we were storing the token,
            // so we need to double check that the queue is empty.
            match self.try_receive() {
                None => {},
                Some(data) => {
                    let tok = (&self.0).take_signal_token();
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
where for<'a> &'a Q: Sends<T> + TakeSignalToken {
    fn unconditional_send(&mut self, t: T) {
        (&self.0).unconditional_send(t);

        let waker = (&self.0).take_signal_token();
        if waker != 0 {
            let waker = unsafe { SignalToken::cast_from_usize(waker) };
            waker.signal();
        }
    }
}

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
