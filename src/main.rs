extern crate database;
extern crate distance;
extern crate timely;

use database::*;
use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        
        // let params = get_params(std::env::args());
        // println!("{:?}", params);

        let process_id = worker.index();
        let n_processes = worker.peers();

        let mut input = InputHandle::new();

        println!("My process_id is: {}\npeers are: {}", process_id, n_processes);

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow(|scope|
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}:\thello {}", process_id, x))
                 .probe()
        );

        // introduce data and watch!
        if process_id == 0 {
            for round in 0..10 {
                input.send(round);
                input.advance_to(round + 1);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
        }

        // let path = "data/ml-latest-small/ratings.csv";

        // let num_records = database::count_records(path);
        // println!("{}", num_records);

        // let i = params.process_id;
        // let num_workers = params.processes;

        // let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;

        // println!("N records by process: {}", div_rec_work);

        // let db = Database::from_file(path,(i*div_rec_work),(div_rec_work*(i+1)));
        // println!("{:?}", db);

    }).unwrap();
}

// extern crate getopts;
// #[derive(Debug)]
// struct Environment {
//     process_id: usize,   // ID of process
//     processes: usize,    // Number of processes
//     threads: usize,     // Threads per Worker
// }

// fn get_params<I: Iterator<Item=String>>(args: I) -> Environment {
//     let mut opts = getopts::Options::new();
//     opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
//     opts.optopt("p", "process", "identity of this process", "IDX");
//     opts.optopt("n", "processes", "number of processes", "NUM");
//     opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
//     opts.optflag("r", "report", "reports connection progress");


//     opts.parse(args)
//         .map_err(|e| format!("{:?}", e))
//         .map(|matches| {
//             let threads = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
//             let process = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
//             let processes = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
//             // let report = matches.opt_present("report");

//             Environment {
//                 threads: threads,
//                 process_id: process,
//                 processes: processes
//             }
//         }).ok().unwrap()
// }