extern crate database;

use database::*;

fn main() {
    let db = Database::from_file("../../Downloads/ml-latest-small/ratings.csv");
    println!("{:?}", db);
}