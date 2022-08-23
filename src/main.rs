use chrono::{Duration, Utc, TimeZone};
use std::{time::Instant, error::Error, fs::{self, DirEntry}, sync::{Arc, Mutex}};
use rayon::prelude::*;
use colored::Colorize;
use csv::Reader;

// Reading csv rows
// Sequential = Finished after 240.51s, 164_853_576 rows processed using 500KB RAM
// Rayon = Finished after 35.55s, 164_853_576 rows processed using 3MB RAM
// Rayon with 100 Threads = Finished after 28.75s, 164853576 rows processed

const RAW_FOLDER_PATH: &str = "/Users/chrigu/Development/mk42-binance/src/backtesting/data/raw/klines";

fn main() {
    let before = Instant::now();
    println!("Hello, {}!", "world".yellow());

    // TODO: optimized for chrigus macbook
    std::env::set_var("RAYON_NUM_THREADS", "100");

    let start_date = Utc.ymd(2021, 3, 1);
    let end_date = Utc.ymd(2022, 5, 15);
    let mut day = start_date;

    let row_count = Arc::new(Mutex::new(0));

    while day <= end_date {
        let day_formated = day.format("%Y-%m-%d").to_string();
        println!("Processing {}", day_formated.yellow());
        day = day + Duration::days(1);

        let paths = fs::read_dir(format!("{}/{}", RAW_FOLDER_PATH, day_formated)).unwrap();
        let files: Vec<DirEntry> = paths.map(|entry| {
            let entry = entry.unwrap();
            entry
          }).collect();

        files.par_iter().for_each(|file| {
            let path = file.path();
            if path.extension().unwrap() == "csv" {
                println!("{}", path.display());
                let affected_rows = parse_csv(path.display().to_string()).unwrap();
                *row_count.lock().unwrap() += affected_rows;
            }
        });
    }

    println!("Finished after {:.2?}, {} rows processed", before.elapsed(), row_count.lock().unwrap());
}

fn parse_csv(path: String) -> Result<u32, Box<dyn Error>> {
    let mut numberOfRows: u32 = 0;
    let mut rdr = Reader::from_path(path)?;
    for result in rdr.records() {
        //let record = result?;
        //println!("{:?}", record);
        numberOfRows += 1;
    }
    //println!("done");
    Ok(numberOfRows)
}