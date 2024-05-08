use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
extern crate rayon;
use rayon::prelude::*;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;

// Define a function to read a file and count words
fn count_words_in_file(file_path: &str) -> HashMap<String, usize> {
    let file = File::open(file_path).expect("Failed to open file");
    let reader = BufReader::new(file);
    let mut word_counts = HashMap::new();

    for line in reader.lines() {
        if let Ok(line) = line {
            for word in line.split_whitespace() {
                *word_counts.entry(word.to_string()).or_insert(0) += 1;
            }
        }
    }

    word_counts
}

// Sequential implementation
fn sequential_word_count(files: &[&str]) -> HashMap<String, usize> {
    let mut total_word_counts = HashMap::new();

    for file in files {
        let counts = count_words_in_file(file);
        for (word, count) in counts {
            *total_word_counts.entry(word).or_insert(0) += count;
        }
    }

    total_word_counts
}

// Task Parallelism implementation using Rayon
fn parallel_word_count(files: &[&str]) -> HashMap<String, usize> {
    files.par_iter().map(|&file| count_words_in_file(file)).reduce(
        || HashMap::new(),
        |mut counts1, counts2| {
            for (word, count) in counts2 {
                *counts1.entry(word).or_insert(0) += count;
            }
            counts1
        },
    )
}

// Actor Model implementation using channels
fn actor_word_count(files: &[&str]) -> HashMap<String, usize> {
    let (tx, rx): (Sender<(String, HashMap<String, usize>)>, Receiver<(String, HashMap<String, usize>)>) = channel();
    let mut handles = vec![];

    for file in files {
        let tx = tx.clone();
        let file = file.to_string();
        let handle = thread::spawn(move || {
            let counts = count_words_in_file(&file);
            tx.send((file, counts)).expect("Failed to send data from thread");
        });
        handles.push(handle);
    }

    drop(tx);

    let mut total_word_counts = HashMap::new();
    for handle in handles {
        handle.join().expect("Failed to join thread");
    }

    for (file, counts) in rx.iter() {
        println!("Received counts for file: {}", file);
        for (word, count) in counts {
            *total_word_counts.entry(word).or_insert(0) += count;
        }
    }

    total_word_counts
}

fn main() {
    // Define file paths
    let files = [
        "lwtext1.txt", "cptext2.txt", "sltext3.txt", "wptext4.txt", "pptext5.txt",
        "ottext6.txt", "kytext7.txt", "tmtext8.txt", "wntext9.txt", "ctext10.txt",
    ];

    // Sequential implementation
    let start = std::time::Instant::now();
    let seq_result = sequential_word_count(&files);
    let seq_time = start.elapsed();

    println!("Sequential result: \n{:?}", seq_result);
    println!("Sequential time: \n{:?}", seq_time);

    // Task Parallelism implementation
    let start = std::time::Instant::now();
    let par_result = parallel_word_count(&files);
    let par_time = start.elapsed();

    println!("\nParallel result: \n{:?}", par_result);
    println!("Parallel time: {:?}", par_time);

    // Actor Model implementation
    let start = std::time::Instant::now();
    let actor_result = actor_word_count(&files);
    let actor_time = start.elapsed();

    println!("\nActor model result: \n{:?}", actor_result);
    println!("\nActor model time: {:?}", actor_time);


    // Performance comparison
    // Compare times and discuss efficiency and effectiveness of each approach

    println!("Sequential time: {:?}", seq_time);
    println!("Parallel time: {:?}", par_time);
    println!("Actor model time: {:?}", actor_time);

    let seq_time_secs = seq_time.as_secs() as f64 + seq_time.subsec_nanos() as f64 * 1e-9;
    let par_time_secs = par_time.as_secs() as f64 + par_time.subsec_nanos() as f64 * 1e-9;
    let actor_time_secs = actor_time.as_secs() as f64 + actor_time.subsec_nanos() as f64 * 1e-9;

    println!("Sequential time (in seconds): {:.6}", seq_time_secs);
    println!("Parallel time (in seconds): {:.6}", par_time_secs);
    println!("Actor model time (in seconds): {:.6}", actor_time_secs);

    println!("Speedup of Parallel over Sequential: {:.2}", seq_time_secs / par_time_secs);
    println!("Speedup of Actor Model over Sequential: {:.2}", seq_time_secs / actor_time_secs);
}