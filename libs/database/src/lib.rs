extern crate csv;
extern crate distance;
extern crate serde;
#[macro_use]
extern crate serde_derive;

// use std::fs::File;
use std::collections::HashMap;
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct Database {
    users: HashMap<i32, HashMap<i32, usize>>,
    movies: HashMap<i32, HashMap<i32, usize>>,
    ratings: Vec<f32>,
}

pub fn count_records(path: &str) -> usize {
    let mut tmp = csv::ReaderBuilder::new()
        .delimiter(b',')
        .from_path(path)
        .unwrap();

    tmp.records().count()
}

impl Database {
    pub fn new() -> Database {
        Database {
            users: HashMap::new(),
            movies: HashMap::new(),
            ratings: Vec::new(),
        }
    }

    pub fn from_file(path: &str) -> Database {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b',')
            .from_path(path)
            .unwrap();

        let mut users: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut movies: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut ratings: Vec<f32> = Vec::new();

        //for (i, record) in rdr.deserialize().enumerate() {
        for (i, record) in rdr.deserialize().enumerate() {
            // skip and take
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
            ratings: ratings,
        }
    }

    pub fn from_range_in_file(path: &str, inicio: usize, fin: usize) -> Database {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b',')
            .from_path(path)
            .unwrap();

        let mut users: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut movies: HashMap<i32, HashMap<i32, usize>> = HashMap::new();
        let mut ratings: Vec<f32> = Vec::new();

        //for (i, record) in rdr.deserialize().enumerate() {
        for (i, record) in rdr.deserialize()
            .skip(inicio)
            .take(fin - inicio)
            .enumerate()
        {
            // skip and take
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
            ratings: ratings,
        }
    }

    fn get_ratings_from_user(&self, user_id: i32, movies: &[&i32]) -> Vec<f32> {
        let mut ratings: Vec<f32> = Vec::new();

        if let Some(user_movie_ratings) = self.users.get(&user_id) {
            for movie in movies {
                if let Some(rating) = user_movie_ratings.get(movie) {
                    ratings.push(self.ratings[*rating]);
                } else {
                    ratings.push(0.0);
                }
            }
        } else {
            panic!("User didn't rate any movie");
        };

        ratings
    }

    // fn get_item_ratings(user_id: i32) -> Vec<f32> {

    // }

    fn get_users_common_ratings(&self, user_id1: i32, user_id2: i32) -> Vec<&i32> {
        let rated_movies_us1: BTreeSet<&i32> = if let Some(ratings) = self.users.get(&user_id1) {
            ratings.keys().collect()
        } else {
            BTreeSet::new()
        };

        let rated_movies_us2: BTreeSet<&i32> = if let Some(ratings) = self.users.get(&user_id2) {
            ratings.keys().collect()
        } else {
            BTreeSet::new()
        };

        rated_movies_us1.union(&rated_movies_us2).cloned().collect()
    }

    pub fn user_distance_vector(
        &self,
        user_id: i32,
        func: fn(&[f32], &[f32]) -> f32,
    ) -> Vec<(i32, f32)> {
        let mut dist_vec: Vec<(i32, f32)> = Vec::new();

        let users: Vec<&i32> = self.users.keys().collect();
        for user in &users {
            if **user != user_id {
                let common_ratings = self.get_users_common_ratings(user_id, **user);
                let usr1_vec = self.get_ratings_from_user(user_id, &common_ratings);
                let usr2_vec = self.get_ratings_from_user(**user, &common_ratings);

                dist_vec.push((**user, func(&usr1_vec, &usr2_vec)));
            }
        }

        dist_vec
    }



    pub fn user_based_recommendation(&self, user_id: i32) -> Vec<(i32, f32)> {
        let mut dist_vec = self.user_distance_vector(user_id, distance::pearson_coef);

        dist_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        dist_vec
    }

    // fn item_based_recommendation(user_id: i32) -> f32 {

    // }

    pub fn get_users_ids(&self) -> Vec<i32> {
        self.users.keys().cloned().collect()
    }

    pub fn get_movies_ids(&self) -> Vec<i32> {
        self.movies.keys().cloned().collect()
    }
}
