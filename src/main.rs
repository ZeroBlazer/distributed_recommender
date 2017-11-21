extern crate database;
extern crate distance;
extern crate timely;

use database::*;

fn main() {
    let path = "data/ml-latest-small/ratings.csv";
    let num_records = database::count_records(path);
    println!("{}", num_records)
    // let db = Database::from_file(path);

    // println!("{:?}", db.user_based_recommendation(1));
}