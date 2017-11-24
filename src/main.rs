extern crate database;
extern crate distance;
extern crate timely;

use database::*;

// use std::collections::HashMap;
use std::io::{stdin, BufRead, Write};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Map, Operator, Inspect, Probe};
use timely::dataflow::channels::pact::Exchange;

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
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let exchange = Exchange::new(|x: &(i32, i32)| x.0 as u64);

        let process_id = worker.index();
        let n_processes = worker.peers();

        println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

        let path = "data/ml-latest-small/ratings.csv";
        let i = process_id;
        let num_workers = n_processes;
        let db = Database::from_file(path);

        // create a new input, exchange data, and inspect its output

        let probe = worker.dataflow(|scope| {
            input.to_stream(scope).iter().flat_map(|(user1, user2)| {
                println!("{:?}", db.distance_between_users(user1, user2, distance::pearson_coef));
            });
        });

        // introduce data and watch!
        if process_id == 0 {
            print!("Escribir id de usuario: ");
            std::io::stdout().flush().ok().expect("Could not flush stdout");
            
            let user_id = get_input();
            if let Some(movies) = db.user_rated_movies(user_id) {
                let users = db.get_users_ids();

                for (step, user) in users.iter().enumerate() {
                    // input.send((user_id, range_origin, range_end));
                    input.send((user_id, *user));
                    input.advance_to(step + 1);
                    while probe.less_than(input.time()) {
                        worker.step();
                    }
                }
            } else {
                panic!("User didn't rated any movie");
            }
        }

    }).unwrap();
}

// let num_records = database::count_records(path);
// println!("{}", num_records);

        // let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;
        // println!("N records by process: {}", div_rec_work);
