extern crate database;
extern crate distance;
extern crate timely;

use database::*;

use std::io::{stdin, Write, BufRead};
use std::sync::{Arc, Mutex};
// use std::collections::HashMap;

use timely::dataflow::{InputHandle, ProbeHandle};
// use timely::dataflow::operators::{Inspect, Map, Operator, Probe, ToStream};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::message::Content;
use timely::dataflow::operators::*;

// FUNCTION GetInput:
fn get_input() -> i32 {
    std::io::stdout()
        .flush()
        .ok()
        .expect("Could not flush stdout");

    let mut buffer = String::new();
    let stdin = stdin();
    stdin
        .lock()
        .read_line(&mut buffer)
        .expect("Could not read line");
    buffer.pop();
    buffer.parse::<i32>().unwrap()
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        // let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
        let path = "data/ml-20m/ratings.csv";
        let db = Database::from_file(path);

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let process_id = worker.index();
        let n_processes = worker.peers();

        let users = db.get_users_ids();
        let n_users = users.len();
        let users = Arc::new(Mutex::new(users));
        let db1 = Arc::new(Mutex::new(db));
        let db2 = db1.clone();
        let partition_size = (n_users as f32 / n_processes as f32).ceil() as usize;

        worker.dataflow::<u64, _, _>(|scope| {
            println!("PID: {} - Peers: {}", process_id, n_processes);
            
            input.to_stream(scope)
                 .unary(Pipeline, "user-user_distances", |capability|

                     move |input, output| {
                        while let Some((time, data)) = input.next() {
                            let mut session = output.session(&time);
                            for datum in data.drain(..) {
                                let (range_0, range_f, id_1): (usize, usize, i32) = datum;
                                println!("id_{}: ({} -> {})", id_1, range_0, range_f);

                                for id_2 in users.lock().unwrap()[range_0..range_f].iter() {
                                    if id_1 != *id_2 {
                                        session.give((*id_2, db1.lock().unwrap().distance_between_users(id_1,
                                                                                                       *id_2,
                                                                                                       distance::pearson_coef)));
                                    }
                                }
                            }
                        }
                    }
                 )
                 .unary(Pipeline, "user-based_recommend", |capability|
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            let mut session = output.session(&time);

                            let dist_vec: Vec<(i32, f32)> = data.take().to_vec();
                            session.give(db2.lock().unwrap().user_based_recommendation(dist_vec));
                        }
                    }
                 )
                 .inspect(|x| println!("UBR: {:?}", x));
        });

        let mut qry_user_id = 0;
        if process_id == 0 {
            print!("Escribir id de usuario: ");
            qry_user_id = get_input();
        }

        for p_id in 0..n_processes {
            if process_id == 0 {
                let range = (p_id * partition_size, if p_id != n_processes - 1 { (p_id + 1) * partition_size } else { n_users });
                input.send((range.0, range.1, qry_user_id));
            }
            input.advance_to(p_id as u64 + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
