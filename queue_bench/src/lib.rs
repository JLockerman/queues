
#![feature(test, hint_core_should_pause, iterator_for_each)]

extern crate crossbeam;
extern crate test;
pub extern crate queues;

#[cfg(linux)] extern crate nix;
#[cfg(linux)] extern crate num_cpus;

#[cfg_attr(linux, macro_use)] #[cfg(linux)] extern crate lazy_static;

use std::sync::atomic::{fence, hint_core_should_pause, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use test::black_box;

use crossbeam::scope;

pub use queues::{Sends, Receive};

pub fn bench_spsc_throughput<S, R>((tx, rx): (S, R), count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput(vec![tx], rx, count)
}

pub fn bench_throughput<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
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
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput_interrupted(vec![tx], rx, count)
}

pub fn bench_throughput_interrupted<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
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
}

pub fn bench_spsc_throughput_windowed<S, R>(
    (tx, rx): (S, R), count: u64, window: u64, ack_buffer: u64
) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput_windowed(vec![tx], rx, count, window, ack_buffer)
}

pub fn bench_throughput_windowed<S, R>(
    tx: Vec<S>, mut rx: R, count: u64, window: u64, ack_buffer: u64
) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
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
        ack_buffer,
    )
}

////////////////////////////////////////

pub fn bench_blocking_spsc_throughput<S, R>((tx, rx): (S, R), count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput(vec![tx], rx, count)
}

pub fn bench_blocking_throughput<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    do_bench_throughput(
        tx,
        move || black_box(rx.receive()),
        count
    )
}

pub fn bench_blocking_spsc_throughput_interrupted<S, R>((tx, rx): (S, R), count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput_interrupted(vec![tx], rx, count)
}

pub fn bench_blocking_throughput_interrupted<S, R>(tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    do_bench_throughput_interrupted(
        tx,
        move || black_box(rx.receive()),
        count
    )
}

pub fn bench_blocking_spsc_throughput_windowed<S, R>(
    (tx, rx): (S, R), count: u64, window: u64, ack_buffer: u64,
) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    bench_throughput_windowed(vec![tx], rx, count, window, ack_buffer)
}

pub fn bench_blocking_throughput_windowed<S, R>(
    tx: Vec<S>, mut rx: R, count: u64, window: u64, ack_buffer: u64
) -> f64
where S: Sends<u64> + Send, R: Receive<u64> {
    do_bench_throughput_windowed(
        tx,
        move || black_box(rx.receive()),
        count,
        window,
        ack_buffer,
    )
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

fn nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1000000000f64 + (d.subsec_nanos() as f64)
}

#[cfg(linux)]
lazy_static! {
    static ref NUM_SENDING_CPUS: usize = num_cpus::get() - 1;
}

#[cfg(linux)]
fn set_cpu(sender_num: usize, num_sending_cpus: usize) {
    if num_sending_cpus > 0 {
        use nix::CpuSet;
        use nix::sched::{sched_setaffinity, Pid};
        // We want to leave one core just for the receiver
        let cpu = (sender_num % num_sending_cpus) + 1;
        let mut cpu_set = CpuSet::new();
        cpu_set.set(cpu).expect("cannot set cpu");
        sched_setaffinity(Pid::this(), &cpu_set).expect("cannot set cpu affinity");
    }
}

#[cfg(not(linux))]
fn set_cpu(_: usize, _: usize) {}

#[cfg(linux)]
fn get_num_cpus() -> usize { NUM_CPUS.get() }

#[cfg(not(linux))]
fn get_num_cpus() -> usize { 0 }

pub fn do_bench_throughput<S, R>(mut tx: Vec<S>, mut rx: R, count: u64) -> f64
where S: Sends<u64> + Send, R: FnMut() -> u64 {
    let num_sending_cpus = get_num_cpus();
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).enumerate().for_each(|(i, mut tx)| {
            scope.spawn(move || {
                set_cpu(i, num_sending_cpus);
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
    let num_sending_cpus = get_num_cpus();
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).enumerate().for_each(|(i, mut tx)| {
            scope.spawn(move || {
                set_cpu(i, num_sending_cpus);
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

pub fn do_bench_throughput_windowed<S, R>(
    mut tx: Vec<S>, mut rx: R, count: u64, window: u64, ack_buffer: u64,
) -> f64
where S: Sends<u64> + Send, R: FnMut() -> u64 {
    use queues::CacheAligned;

    let num_sending_cpus = get_num_cpus();
    let num_threads = tx.len() + 1;
    let started = &AtomicUsize::new(0);
    let acks: &Vec<_> = &(0..tx.len()).map(|_| CacheAligned::new(AtomicUsize::new(0))).collect();
    let mut buffered_acks: Vec<_> = (0..tx.len()).map(|_| 0).collect();
    let mut local_acks: Vec<_> = (0..tx.len()).map(|_| 0u64).collect();
    let start = Instant::now();
    scope(|scope| {
        tx.drain(..).enumerate().for_each(|(i, mut tx)| {
            scope.spawn(move || {
                set_cpu(i, num_sending_cpus);
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
            buffered_acks[i] += 1;
            local_acks[i] += 1;
            if buffered_acks[i] >= ack_buffer {
                acks[i].store(local_acks[i] as usize, Ordering::Relaxed);
                buffered_acks[i] = 0;
            }
        }
    });
    let d = start.elapsed();

    nanos(d) / (count as f64)
}
