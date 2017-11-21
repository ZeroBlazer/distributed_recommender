extern crate database;
extern crate distance;

use database::*;
use distance::pearson_coef;

fn main() {
    let db = Database::from_file("../../Downloads/ml-latest-small/ratings.csv");

    println!("{:?}", db.user_based_recommendation(1));
}