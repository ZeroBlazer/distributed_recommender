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

use timely::dataflow::operators::*;

// FUNCTION GetInput:
fn get_input() ->  i32 {
    std::io::stdout().flush().ok().expect("Could not flush stdout");

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
        let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
        let db = Database::from_file(path);

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let process_id = worker.index();
        let n_processes = worker.peers();

        // let user_id = 1; // get_input();
        // if let Some(movies) = db.user_rated_movies(user_id) {
        let users = db.get_users_ids();
        let n_users = users.len();
        let users = Arc::new(Mutex::new(users));
        let db = Arc::new(Mutex::new(db));
        let partition_size = (n_users as f32 / n_processes as f32).ceil() as usize;

        worker.dataflow::<u64, _, _>(|scope| {
            println!("PID: {} - Peers: {}", process_id, n_processes);
            
            input.to_stream(scope)
                 .unary(Pipeline, "user-based", |capability|
                    //  let mut dist_vec = Vec::new();

                     move |input, output| {
                        while let Some((time, data)) = input.next() {
                            let mut session = output.session(&time);
                            for datum in data.drain(..) {
                                let (range_0, range_f, id_1): (usize, usize, i32) = datum;

                                for id_2 in users.lock().unwrap()[range_0..range_f].iter() {
                                    if id_1 != *id_2 {
                                        session.give((*id_2, db.lock().unwrap().distance_between_users(id_1, *id_2, distance::pearson_coef)));
                                    }
                                    // println!("{}", );
                                }
                            }
                        }
                    }
                 )
                 .inspect(|x| println!("{:?}", x));
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

// scope.input_from(&mut input)
            //     //  .exchange(|x| *x)
            //      .inspect(move |&(id_1, id_2): &(i32, i32)| println!("{}", db.distance_between_users(id_1, id_2, distance::pearson_coef)))
            //      .probe_with(&mut probe);

        // for (step, user) in users[range.0..range.1].iter().enumerate() {
        //     // input.send((user_id, range_origin, range_end));
        //     input.send((user_id, *user));
        //     input.advance_to(step + 1);
        //     while probe.less_than(input.time()) {
        //         worker.step();
        //     }
        // }


// } else {
        //     panic!("User didn't rated any movie");
        // }

        // // introduce data and watch!
        // for round in 0..10 {
        //     input.send((2, 1));
        //     input.advance_to(round + 1);
        //     while probe.less_than(input.time()) {
        //         worker.step();
        //     }
        // }


// timely::execute_from_args(std::env::args(), |scope| {
    // // timely::example(|scope| {
    //     let process_id = scope.index();
    //     let n_processes = scope.peers();
    //     println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

    //     let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
    //     let db = Database::from_file(path);
    //     let user_id = 12; // get_input();

    //     if let Some(movies) = db.user_rated_movies(user_id) {
    //         let users = db.get_users_ids();
    //         users.into_iter()
    //         .to_stream(scope)
    //         .inspect(|x| println!("hello: {:?}", x));
    //     } else {
    //         panic!("User didn't rated any movie");
    //     }



    //     // let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
    //     // let db = Database::from_file(path);

    //     // // introduce data and watch!
    //     // if process_id == 0 {
    //     // //     print!("Escribir id de usuario: ");
    //     // //     std::io::stdout().flush().ok().expect("Could not flush stdout");
            
    //     //     let user_id = 12; // get_input();
    //     //     if let Some(movies) = db.user_rated_movies(user_id) {
    //     //         let users = db.get_users_ids();

    //     //         for (step, user) in users.iter().enumerate() {
    //     //             // input.send((user_id, range_origin, range_end));
    //     //             input.send(user_id as u64);
    //     //             input.advance_to(step + 1);
    //     //             while probe.less_than(input.time()) {
    //     //                 worker.step();
    //     //             }
    //     //         }
    //     //     } else {
    //     //         panic!("User didn't rated any movie");
    //     //     }
    //     // }



    //     // let index = worker.index();
    //     // let peers = worker.peers();

    //     // let mut input = worker.dataflow::<u64,_,_>(|scope| {

    //     //     let (input, stream) = scope.new_input();

    //     //     stream
    //     //         .broadcast()
    //     //         .inspect(move |x| println!("{} -> {:?}", index, x));

    //     //     input
    //     // });

    //     // for round in 0u64..10 {
    //     //     if (round as usize) % peers == index {
    //     //         input.send(round);
    //     //     }
    //     //     input.advance_to(round + 1);
    //     //     worker.step();
    //     // }







    //     // exchange data and inspect its output.
    //     // root.scoped::<u64, _, _>move(|builder| {
    //     //     unimplemented!();
    //     // });

    //     // worker.dataflow(|scope| {
    //     //     input.to_stream(scope)
    //     //          .exchange(|x| *x)
    //     //          .inspect(move |x| println!("worker {}:\thello {}", process_id, x))
    //     //          .probe_with(&mut probe);
    //     // });

    //     // let mut input = InputHandle::new();
    //     // // let mut probe = ProbeHandle::new();

    //     // // let exchange = Exchange::new(|x: &(i32, i32)| x.0 as u64);
    //     // let process_id = worker.index();
    //     // let n_processes = worker.peers();
    //     // println!("My process_id is: {}\nWe are {} processes", process_id, n_processes);

    //     // let path = "data/ml-latest-small/ratings.csv"; // = std::env::args().nth(1).unwrap();
    //     // let db = Database::from_file(path);

    //     // worker.dataflow(|scope| {
    //     //     scope.input_from(&mut input).inspect(|user| println!("{:?}", user));
    //     // });

    //     // // let probe = worker.dataflow(|scope| {
    //     // //     input.to_stream(scope). .exchange().flat_map(|(user1, user2)| {
    //     // //         println!("{:?}", db.distance_between_users(user1, user2, distance::pearson_coef));
    //     // //     });
    //     // // });

    //     // // introduce data and watch!

    //     // if process_id == 0 {
    //     // //     print!("Escribir id de usuario: ");
    //     // //     std::io::stdout().flush().ok().expect("Could not flush stdout");
            
    //     //     let user_id = 12; // get_input();
    //     //     if let Some(movies) = db.user_rated_movies(user_id) {
    //     //         let users = db.get_users_ids();

    //     //         for (step, user) in users.iter().enumerate() {
    //     //             // input.send((user_id, range_origin, range_end));
    //     //             input.send((user_id, *user));
    //     //             input.advance_to(step + 1);
    //     //             // while probe.less_than(input.time()) {
    //     //             //     worker.step();
    //     //             // }
    //     //         }
    //     //     } else {
    //     //         panic!("User didn't rated any movie");
    //     //     }
    //     // }
    // })






        // let i = process_id;
//         let num_workers = n_processes;

// let num_records = database::count_records(path);
// println!("{}", num_records);

        // let div_rec_work = (num_records as f32 / num_workers as f32).ceil() as usize;
        // println!("N records by process: {}", div_rec_work);
