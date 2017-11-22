extern crate database;
extern crate distance;
extern crate timely;

use database::*;

use std::collections::HashMap;
use std::io::{stdin, BufRead};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Map, Operator, Inspect, Probe};
// use timely::dataflow::channels::pact::Exchange;

// FUNCTION GetInput:
fn get_input() ->  i32 {
    let mut buffer = String::new();
    let stdin = stdin();
    stdin
        .lock()
        .read_line(&mut buffer)
        .expect("Could not read line");
    buffer.pop();
    buffer.parse::<i32>().unwrap()
}
//

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let process_id = worker.index();
        let n_processes = worker.peers();

        let mut input = InputHandle::new();

        println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

        let path = "data/ml-latest-small/ratings.csv";

        // let num_records = database::count_records(path);
        // println!("{}", num_records);
        
        let i = process_id;
        let num_workers = n_processes;

        // let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;
        // println!("N records by process: {}", div_rec_work);

        let db = Database::from_file(path);
        // println!("{:?}", db);

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| {
                     println!("{:?}", db.user_based_recommendation(*x as i32));
                    //  println!("worker {}:\thello {}", process_id, x)
                 })
                 .probe()
        });

        // introduce data and watch!
        if process_id == 0 {
            print!("Escribir id de usuario: ");
            let user_id = get_input();

            for step in 0..n_processes {
                // input.send((user_id, range_origin, range_end));
                input.send(user_id as u64);
                input.advance_to(step + 1);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
        }
        //println!("esto es para get_input: {}", get_input());

    }).unwrap();
}

// let params = get_params(std::env::args());
// println!("{:?}", params);



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