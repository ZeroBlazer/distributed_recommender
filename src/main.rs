extern crate database;
extern crate distance;
extern crate timely;
extern crate getopts;

use database::*;

#[derive(Debug)]
struct Environment {
    threads: usize,     // Threads per Worker
    process_id: usize,   // ID of process
    processes: usize    // Number of processes
}

fn get_params<I: Iterator<Item=String>>(args: I) -> Environment {
    let mut opts = getopts::Options::new();
    opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
    opts.optopt("p", "process", "identity of this process", "IDX");
    opts.optopt("n", "processes", "number of processes", "NUM");
    opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
    opts.optflag("r", "report", "reports connection progress");


    opts.parse(args)
        .map_err(|e| format!("{:?}", e))
        .map(|matches| {
            let threads = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
            let process = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
            let processes = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
            let report = matches.opt_present("report");

            Environment {
                threads: threads,
                process_id: process,
                processes: processes
            }
        }).ok().unwrap()
}

fn main() {
    let path = "data/ml-latest-small/ratings_prueba.csv";
    let num_records = database::count_records(path);
    println!("{}", num_records);
    let num_workers: usize = 10;
    let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;

    println!("valor {}", div_rec_work);
    for i in (0..num_workers) {
        let db = Database::from_file(path,(i*div_rec_work),(div_rec_work*(i+1)));
        println!("{:#?}", db);
    }
/*
    {
        let db = Database::from_file(path,0,3);
        println!("{:#?}", db);
    }
    {
        let db = Database::from_file(path,3,6);
        println!("{:#?}", db);
    }
*/
    // println!("{:?}", db.user_based_recommendation(1));
}