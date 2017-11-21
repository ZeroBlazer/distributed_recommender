extern crate database;
extern crate distance;
extern crate timely;

use database::*;

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