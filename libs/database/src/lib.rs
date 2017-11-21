extern crate csv;
extern crate serde;
#[macro_use]
extern crate serde_derive;

// use std::fs::File;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Database {
    users: HashMap<i32, HashMap<i32, usize>>,
    movies: HashMap<i32, HashMap<i32, usize>>,
    ratings: Vec<f32>
}

// fn nearest_neighbors(db: &Database, user_id: i32, func: fn(&[f32], &[f32]) -> f32)
//                      -> Vec<(f32, String)> {
//     let mut dist_vec: Vec<(f32, String)> = Vec::new();

//     for (rec_id, _) in db.0.iter() {
//         if id != rec_id.as_str() {
//             let rec_str = rec_id.clone();
//             let (obj_vec, rec_vec) = users_rating_vectors(db, id, rec_id);
//             dist_vec.push((func(&obj_vec, &rec_vec), rec_str));
//         }
//     }

//     dist_vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
//     dist_vec
// }

impl Database {
    pub fn new() -> Database {
        Database {
            users: HashMap::new(),
            movies: HashMap::new(),
            ratings: Vec::new()
        }
    }

    pub fn from_file(path: &str) -> Database {
        let mut rdr = csv::ReaderBuilder::new().delimiter(b',').from_path(path).unwrap();

        let mut users: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut movies: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut ratings: Vec<f32> = Vec::new();

        for (i, record) in rdr.deserialize().enumerate() {
            let values: (i32, i32, f32, u64) = record.unwrap();

            ratings.push(values.2);
            let user_ratings = users.entry(values.0).or_insert(HashMap::new());
            let movie_ratings = movies.entry(values.1).or_insert(HashMap::new());

            user_ratings.insert(values.1, i);
            movie_ratings.insert(values.0, i);
        }

        Database {
            users: users,
            movies: movies,
            ratings: ratings
        }
    }

    fn get_user_ratings(user_id: i32) -> Vec<f32> {

    }

    fn get_item_ratings(user_id: i32) -> Vec<f32> {

    }

    // fn user_based_recommendation(user_id: i32) -> f32 {

    // }

    // fn item_based_recommendation(user_id: i32) -> f32 {

    // }
}