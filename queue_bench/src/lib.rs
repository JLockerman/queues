
#![feature(test, hint_core_should_pause, iterator_for_each)]

extern crate crossbeam;
extern crate test;
pub extern crate queues;

use std::sync::atomic::{fence, hint_core_should_pause, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use test::black_box;

use crossbeam::scope;

pub use queues::{Sends, TryReceive};

fn nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1000000000f64 + (d.subsec_nanos() as f64)
}

pub fn bench_spsc_throughput<S, R>((tx, rx): (S, R), count: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    bench_throughput(vec![tx], rx, count)
}

/*pub fn bench_throughput<S, R>(mut tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).for_each(|mut tx| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                for x in 0..count {
                    let _ = black_box(tx.unconditional_send(x));
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _i in 0..count {
            while let None = black_box(rx.try_receive()) { hint_core_should_pause() }
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)
}*/

pub fn bench_throughput<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    do_bench_throughput(
        tx,
        move || loop {
            match black_box(rx.try_receive()) {
                None => hint_core_should_pause(),
                Some(i) => break i,
            }
        },
        count
    )
}

pub fn bench_spsc_throughput_interrupted<S, R>((tx, rx): (S, R), count: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    bench_throughput_interrupted(vec![tx], rx, count)
}

pub fn bench_throughput_interrupted<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    do_bench_throughput_interrupted(
        tx,
        move || loop {
            match black_box(rx.try_receive()) {
                None => hint_core_should_pause(),
                Some(i) => break i,
            }
        },
        count
    )
    /*let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).for_each(|mut tx| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                for x in 0..count {
                    let _ = black_box(tx.unconditional_send(x));
                    fence(Ordering::SeqCst);
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _i in 0..count {
            while let None = black_box(rx.try_receive()) {}
            fence(Ordering::SeqCst);
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)*/
}

pub fn bench_spsc_throughput_windowed<S, R>((tx, rx): (S, R), count: u64, window: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    bench_throughput_windowed(vec![tx], rx, count, window)
}

pub fn bench_throughput_windowed<S, R>(tx: Vec<S>, mut rx: R, count: u64, window: u64) -> f64
where S: Sends<u64> + Send, R: TryReceive<u64> {
    do_bench_throughput_windowed(
        tx,
        move || loop {
            match black_box(rx.try_receive()) {
                None => hint_core_should_pause(),
                Some(i) => break i,
            }
        },
        count,
        window,
    )
    /*use queues::CacheAligned;
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let acks: &Vec<_> = &(0..tx.len()).map(|_| CacheAligned::new(AtomicUsize::new(0))).collect();
    let mut local_acks: Vec<_> = (0..tx.len()).map(|_| 0u64).collect();
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).enumerate().for_each(|(i, mut tx)| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                let mut acked = 0u64;
                let mut window = window;
                let mut sent = 0;
                while sent < count {
                    let old_acked = acked;
                    acked += acks[i].load(Ordering::Relaxed) as u64;
                    window += acked - old_acked;
                    while window == 0 {
                        hint_core_should_pause();
                        let old_acked = acked;
                        acked += acks[i].load(Ordering::Relaxed) as u64;
                        window += acked - old_acked;
                    }
                    for _ in 0..window {
                        let _ = black_box(tx.unconditional_send(i as u64));
                    }
                    sent += window;
                    window = 0;
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _ in 0..count {
            loop {
                match black_box(rx.try_receive()) {
                    None => {}
                    Some(i) => {
                        let i = i as usize;
                        local_acks[i] += 1;
                        acks[i].store(local_acks[i] as usize, Ordering::Relaxed);
                        break
                    }
                }
            }
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)*/
}

///////////////////////////////////////

pub fn do_bench_throughput<S, R>(mut tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: FnMut() -> u64 {
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).for_each(|mut tx| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                for x in 0..count {
                    let _ = black_box(tx.unconditional_send(x));
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _i in 0..count { rx(); }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)
}

pub fn do_bench_throughput_interrupted<S, R>(mut tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: FnMut() -> u64 {
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).for_each(|mut tx| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                for x in 0..count {
                    let _ = black_box(tx.unconditional_send(x));
                    fence(Ordering::SeqCst);
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _i in 0..count {
            rx();
            fence(Ordering::SeqCst);
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)
}

pub fn do_bench_throughput_windowed<S, R>(mut tx: Vec<S>, mut rx: R, count: u64, window: u64) -> f64
where S: Sends<u64> + Send, R: FnMut() -> u64 {
    use queues::CacheAligned;
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let acks: &Vec<_> = &(0..tx.len()).map(|_| CacheAligned::new(AtomicUsize::new(0))).collect();
    let mut local_acks: Vec<_> = (0..tx.len()).map(|_| 0u64).collect();
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).enumerate().for_each(|(i, mut tx)| {
            scope.spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);
                while started.load(Ordering::Relaxed) < num_threads {
                    hint_core_should_pause()
                };

                let mut acked = 0u64;
                let mut window = window;
                let mut sent = 0;
                while sent < count {
                    let old_acked = acked;
                    acked += acks[i].load(Ordering::Relaxed) as u64;
                    window += acked - old_acked;
                    while window == 0 {
                        hint_core_should_pause();
                        let old_acked = acked;
                        acked += acks[i].load(Ordering::Relaxed) as u64;
                        window += acked - old_acked;
                    }
                    for _ in 0..window {
                        let _ = black_box(tx.unconditional_send(i as u64));
                    }
                    sent += window;
                    window = 0;
                }
            });
        });

        started.fetch_add(1, Ordering::Relaxed);

        for _ in 0..count {
            let i = rx()  as usize;
            local_acks[i] += 1;
            acks[i].store(local_acks[i] as usize, Ordering::Relaxed);
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)
}