extern crate database;
extern crate distance;
extern crate timely;

use database::*;

// use std::collections::HashMap;
use std::io::{stdin, BufRead};

// use timely::dataflow::*;
// use timely::dataflow::operators::*;
// use timely::dataflow::operators::Exchange;
// use timely::dataflow::channels::pact::Exchange;

use timely::dataflow::operators::{ToStream, Inspect};
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::channels::pact::Pipeline;

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

fn main() {
    timely::execute_from_args(std::env::args(), |scope| {
    // timely::example(|scope| {
        let process_id = scope.index();
        let n_processes = scope.peers();
        println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

        let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
        let db = Database::from_file(path);
        let user_id = 12; // get_input();

        if let Some(movies) = db.user_rated_movies(user_id) {
            let users = db.get_users_ids();
            users.into_iter()
            .to_stream(scope)
            .inspect(|x| println!("hello: {:?}", x));
        } else {
            panic!("User didn't rated any movie");
        }



        // let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
        // let db = Database::from_file(path);

        // // introduce data and watch!
        // if process_id == 0 {
        // //     print!("Escribir id de usuario: ");
        // //     std::io::stdout().flush().ok().expect("Could not flush stdout");
            
        //     let user_id = 12; // get_input();
        //     if let Some(movies) = db.user_rated_movies(user_id) {
        //         let users = db.get_users_ids();

        //         for (step, user) in users.iter().enumerate() {
        //             // input.send((user_id, range_origin, range_end));
        //             input.send(user_id as u64);
        //             input.advance_to(step + 1);
        //             while probe.less_than(input.time()) {
        //                 worker.step();
        //             }
        //         }
        //     } else {
        //         panic!("User didn't rated any movie");
        //     }
        // }



        // let index = worker.index();
        // let peers = worker.peers();

        // let mut input = worker.dataflow::<u64,_,_>(|scope| {

        //     let (input, stream) = scope.new_input();

        //     stream
        //         .broadcast()
        //         .inspect(move |x| println!("{} -> {:?}", index, x));

        //     input
        // });

        // for round in 0u64..10 {
        //     if (round as usize) % peers == index {
        //         input.send(round);
        //     }
        //     input.advance_to(round + 1);
        //     worker.step();
        // }




        // exchange data and inspect its output.
        // root.scoped::<u64, _, _>move(|builder| {
        //     unimplemented!();
        // });

        // worker.dataflow(|scope| {
        //     input.to_stream(scope)
        //          .exchange(|x| *x)
        //          .inspect(move |x| println!("worker {}:\thello {}", process_id, x))
        //          .probe_with(&mut probe);
        // });

        // let mut input = InputHandle::new();
        // // let mut probe = ProbeHandle::new();

        // // let exchange = Exchange::new(|x: &(i32, i32)| x.0 as u64);
        // let process_id = worker.index();
        // let n_processes = worker.peers();
        // println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

        // let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
        // let db = Database::from_file(path);

        // worker.dataflow(|scope| {
        //     scope.input_from(&mut input).inspect(|user| println!("{:?}", user));
        // });

        // // let probe = worker.dataflow(|scope| {
        // //     input.to_stream(scope). .exchange().flat_map(|(user1, user2)| {
        // //         println!("{:?}", db.distance_between_users(user1, user2, distance::pearson_coef));
        // //     });
        // // });

        // // introduce data and watch!

        // if process_id == 0 {
        // //     print!("Escribir id de usuario: ");
        // //     std::io::stdout().flush().ok().expect("Could not flush stdout");
            
        //     let user_id = 12; // get_input();
        //     if let Some(movies) = db.user_rated_movies(user_id) {
        //         let users = db.get_users_ids();

        //         for (step, user) in users.iter().enumerate() {
        //             // input.send((user_id, range_origin, range_end));
        //             input.send((user_id, *user));
        //             input.advance_to(step + 1);
        //             // while probe.less_than(input.time()) {
        //             //     worker.step();
        //             // }
        //         }
        //     } else {
        //         panic!("User didn't rated any movie");
        //     }
        // }
    })
}

        // let i = process_id;
//         let num_workers = n_processes;

// let num_records = database::count_records(path);
// println!("{}", num_records);

        // let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;
        // println!("N records by process: {}", div_rec_work);
