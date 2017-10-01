#![feature(conservative_impl_trait)]

extern crate queue_bench;
extern crate structopt;
#[macro_use] extern crate structopt_derive;

use std::sync::mpsc;

use structopt::StructOpt;

use queue_bench::*;
use queue_bench::queues::unbounded::spsc::std_stream::Queue;
//use queue_bench::queues::blocking::Blocking;

#[derive(StructOpt, Debug)]
#[structopt(name = "shootout", about = "multithreaded queue benchmarks")]
struct Args {
    #[structopt(short="r", long="rounds", default_value="1", help = "Number of time to runthe benchmark.\nSome CPUs throttle aggressively enough that later bechmarks perform noticeably different than earlier ones. This argument allows all the benchmarks to be run multiple times, in hope that the later output is stable.")]
    rounds: usize,

    #[structopt(short="c", long="count", default_value="10000000", help = "Elements per run in send clocked benchmarks.")]
    count: usize, //FIXME u64 does not work

    #[structopt(short="w", long="window", default_value="100", help = "max queue length allowed for windowed tests.")]
    window: usize, //FIXME u64 does not work
}

macro_rules! experiment {
    ($($name:expr, $experiment:expr);* $(;)*) => {
        let names = [$($name),* ];
        let max_name_len = names.into_iter().map(|name| name.len()).max().unwrap_or(0);
        let mut name_and_align = names.into_iter().map(|&name| (name, max_name_len + 1));
        $(
            let (name, align) = name_and_align.next().unwrap();
            println!("{:>width$} {:>5.0}", name, $experiment, width=align);
        )*
    };
}

macro_rules! experiments {
    ($($expr_name:expr => { $($name:expr, $experiment:expr);* $(;)* })*) => {
        let names = [$($expr_name),* ];
        let max_name_len = names.into_iter().map(|name| name.len()).max().unwrap_or(0);
        let mut name_and_align = names.into_iter().map(|&name| (name, max_name_len));
        $(
            let (name, align) = name_and_align.next().unwrap();
            println!("--- {:^width$} ---", name, width=align);
            experiment!($($name, $experiment);*);
            println!("");
        )*
    };
}

fn main() {
    use std::mem::size_of_val;
    unsafe {
        println!("            base {:4}", size_of_val(&Queue::<u64>::new(128)));
        println!("           block {:4}", size_of_val(&Queue::<u64, _, _>::blocking(128)));
        println!(" compact counter {:4}", size_of_val(&Queue::<u64, _, _>::compact_counter_blocking(128)));
        println!("separate counter {:4}", size_of_val(&Queue::<u64, _, _>::separate_counter_blocking(128)));
    }

    let args @ Args{..} = StructOpt::from_args();
    if args.rounds == 1 { println!("running 1 round, {} count, window {}.", args.count, args.window) }
    else { println!("running {} rounds, {} count, window {}.", args.rounds, args.count, args.window) }

    let mut ack_window = args.window as u64 / 2;
    if ack_window % 2 != 0 { ack_window -= 1 };

    for round in 0..args.rounds {
        if args.rounds != 1 { println!("==== running round {} ====", round + 1) }
        experiments!(
            "spsc" => {
                "mpsc", bench_spsc_throughput(mpsc::channel(), args.count as u64);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_spsc_throughput((&*q, &*q), args.count as u64)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_spsc_throughput((&*q, &*q), args.count as u64)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_spsc_throughput((&*q, &*q), args.count as u64)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_spsc_throughput((&*q, &*q), args.count as u64)};
            }

            /*"spsc, interrupted" => {
                "mpsc", bench_spsc_throughput_interrupted(mpsc::channel(), args.count as u64);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
            }*/


            "spsc, windowed 1" => {
                "mpsc", bench_blocking_spsc_throughput_windowed(mpsc::channel(), args.count as u64, args.window as u64, 1);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
            }

            "spsc, windowed w/2" => {
                "mpsc", bench_blocking_spsc_throughput_windowed(mpsc::channel(), args.count as u64, args.window as u64, ack_window);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
            }

            "blocking spsc" => {
                "mpsc", bench_blocking_spsc_throughput(mpsc::channel(), args.count as u64);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput((&*q, &*q), args.count as u64)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput((&*q, &*q), args.count as u64)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput((&*q, &*q), args.count as u64)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput((&*q, &*q), args.count as u64)};
            }

            /*"blocking spsc, interrupted" => {
                "mpsc", bench_blocking_spsc_throughput_interrupted(mpsc::channel(), args.count as u64);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput_interrupted((&*q, &*q), args.count as u64)};
            }*/


            "blocking spsc, windowed 1" => {
                "mpsc", bench_blocking_spsc_throughput_windowed(mpsc::channel(), args.count as u64, args.window as u64, 1);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, 1)};
            }

            "blocking spsc, windowed w/2" => {
                "mpsc", bench_blocking_spsc_throughput_windowed(mpsc::channel(), args.count as u64, args.window as u64, ack_window);
                "stream", unsafe {let q = Box::new(Queue::new(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "blocking stream", unsafe {let q = Box::new(Queue::blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "counter stream", unsafe {let q = Box::new(Queue::compact_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
                "sounter stream", unsafe {let q = Box::new(Queue::separate_counter_blocking(128)); bench_blocking_spsc_throughput_windowed((&*q, &*q), args.count as u64, args.window as u64, ack_window)};
            }
        );
    }
}
