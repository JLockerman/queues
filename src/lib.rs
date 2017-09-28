
#![feature(repr_align, attr_literals, box_syntax, hint_core_should_pause)]

use std::ops::{Deref, DerefMut};

pub mod unbounded;
pub mod blocking;

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(align(64))]
pub struct Aligner;

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheAligned<T>(pub T, pub Aligner);

impl<T> Deref for CacheAligned<T> {
     type Target = T;
     fn deref(&self) -> &Self::Target {
         &self.0
     }
}

impl<T> DerefMut for CacheAligned<T> {
     fn deref_mut(&mut self) -> &mut Self::Target {
         &mut self.0
     }
}

impl<T> CacheAligned<T> {
    pub fn new(t: T) -> Self {
        CacheAligned(t, Aligner)
    }
}

pub trait TryReceive<T> {
    fn try_receive(&mut self) -> Option<T>;
}

pub trait Receive<T> {
    fn receive(&mut self) -> T;
}

pub trait Sends<T> {
    fn unconditional_send(&mut self, t: T);
    fn try_send(&mut self, t: T) -> Option<T> {
        self.unconditional_send(t);
        None
    }
}

pub unsafe trait StoreSignalToken {
    fn store_signal_token(&self, token: usize);
    fn take_signal_token(&self) -> usize;
}

pub unsafe trait TakeSignalToken {
    fn take_signal_token(&self) -> usize;
}

mod impls {
    use std::sync::mpsc::{Sender, Receiver};

    impl<T> ::Sends<T> for Sender<T> {
         fn unconditional_send(&mut self, t: T) {
             let _ = self.send(t);
         }
    }

    impl<T> ::TryReceive<T> for Receiver<T> {
         fn try_receive(&mut self) -> Option<T> {
             use ::std::thread::yield_now;
             use std::sync::atomic::hint_core_should_pause;

             let r = self.try_recv().ok();
             if r.is_some() {
                 return r
             }
             let mut spins = 0;
             loop {
                let r = self.try_recv().ok();
                if r.is_some() {
                    return r
                }
                //constants from https://github.com/Amanieu/parking_lot/blob/36e616f70c75d098c83e6ddb1f96c5dc3aacbb35/core/src/spinwait.rs
                if spins > 10 {
                    yield_now();
                    continue
                } else {
                    spins += 1;
                    for _ in 0..(4 << spins) {
                        hint_core_should_pause()
                    }
                }
             }
         }
    }
}
