extern crate database;
extern crate distance;

use database::*;
use distance::pearson_coef;

fn main() {
    let db = Database::from_file("data/ml-20m/ratings.csv");

    println!("{:?}", db.user_based_recommendation(1));
}