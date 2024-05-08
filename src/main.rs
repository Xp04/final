use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
extern crate rayon;
use rayon::prelude::*;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;

//Defines a function to read a file and count words
fn count_words_in_file(file_path: &str) -> HashMap<String, usize> {
    //Opens the file specified by the file_path parameter or panic if it fails
    let file = File::open(file_path).expect("Failed to open file");
    
    //Creates a buffered reader to efficiently read lines from the file
    let reader = BufReader::new(file);
    
    //Creates a HashMap to store word counts
    let mut word_counts = HashMap::new();

    //Iterates over each line in the file
    for line in reader.lines() {
        //Unwraps the Result to get the line string or skip if there's an error
        if let Ok(line) = line {
            //Iterates over each word in the line by splitting it by whitespace
            for word in line.split_whitespace() {
                //Updates the word count in the HashMap, inserting if necessary
                *word_counts.entry(word.to_string()).or_insert(0) += 1;
            }
        }
    }

    //Returns the HashMap containing word counts
    word_counts
}

//Sequential implementation
fn sequential_word_count(files: &[&str]) -> HashMap<String, usize> {
    //Creates a new HashMap to store the total word counts across all files
    let mut total_word_counts = HashMap::new();

    //Iterates over each file path in the given slice of file paths
    for file in files {
        //Counts the words in the current file and store the result in 'counts'
        let counts = count_words_in_file(file);

        //Iterates over each (word, count) pair in 'counts' and update the total word counts
        for (word, count) in counts {
            //Updates the total word count for 'word' by adding 'count' to its current count,
            //or inserts 'count' if 'word' doesn't exist in 'total_word_counts'
            *total_word_counts.entry(word).or_insert(0) += count;
        }
    }

    //Returns the aggregated word counts across all files
    total_word_counts
}

// Task Parallelism implementation using Rayon
fn parallel_word_count(files: &[&str]) -> HashMap<String, usize> {
    //Uses Rayon's parallel iterator to iterate over files concurrently. 
    //Maps each file path to its word count HashMap using the count_words_in_file function.
    //Then reduces the word count HashMaps into a single HashMap.
    files.par_iter().map(|&file| count_words_in_file(file)).reduce(
        
        //Initial value for the reduction: an empty HashMap.
        || HashMap::new(),
        //A reduction function: merges counts from two HashMaps.
        |mut counts1, counts2| {
            //Iterates over each (word, count) pair in the second HashMap.
            for (word, count) in counts2 {
                //Adds or updates the count for each word in the first HashMap.
                *counts1.entry(word).or_insert(0) += count;
            }
            //Returns the merged HashMap.
            counts1
        },
    )
}

//Actor Model implementation using channels
fn actor_word_count(files: &[&str]) -> HashMap<String, usize> {
    //Creates channels for communication between threads
    //`tx` for sending data from the main thread to the worker threads,
    //`rx` for receiving data from the worker threads back to the main thread.
    let (tx, rx): (Sender<(String, HashMap<String, usize>)>, Receiver<(String, HashMap<String, usize>)>) = channel();
    //Creates a vector to hold thread handles
    let mut handles = vec![];

    //Iterates over each file path in the input slice
    for file in files {
        //Clones the sender channel to move into the thread closure for sending data
        let tx = tx.clone();
        //Converts the file path to a String to pass into the thread closure
        let file = file.to_string();

        //Spawns a new thread to process each file concurrently
        let handle = thread::spawn(move || {
            //Calls the `count_words_in_file` function to count words in the file
            let counts = count_words_in_file(&file);
            //Sends the file path and word counts back to the main thread through the channel
            tx.send((file, counts)).expect("Failed to send data from thread");
        });
        
        //Stores the thread handle in the handles vector
        handles.push(handle);
    }

    //Closes the sender channel to indicate that no more data will be sent
    drop(tx);

    //Creates a HashMap to store the total word counts across all files
    let mut total_word_counts = HashMap::new();
    //Waits for all threads to finish and collect their results
    for handle in handles {
        handle.join().expect("Failed to join thread");
    }

    //Iterates over received data from the receiver channel
    for (file, counts) in rx.iter() {
        //Prints a message indicating the counts received for each file
        println!("Received counts for file: {}", file);
        //Updates the total word counts HashMap with the counts received from each file
        for (word, count) in counts {
            *total_word_counts.entry(word).or_insert(0) += count;
        }
    }

    //Returns the total word counts HashMap
    total_word_counts
}

fn main() {
    //Defines file paths
    let files = [
        "lwtext1.txt", "cptext2.txt", "sltext3.txt", "wptext4.txt", "pptext5.txt",
        "ottext6.txt", "kytext7.txt", "tmtext8.txt", "wntext9.txt", "ctext10.txt",
    ];

    // Sequential implementation
    let start = std::time::Instant::now();
    let seq_result = sequential_word_count(&files);
    let seq_time = start.elapsed();

    //Prints the following messages
    println!("Sequential result: \n{:?}", seq_result);
    println!("Sequential time: \n{:?}", seq_time);

    //Task Parallelism implementation
    let start = std::time::Instant::now();
    let par_result = parallel_word_count(&files);
    let par_time = start.elapsed();    

    //Prints the following messages
    println!("\nParallel result: \n{:?}", par_result);
    println!("Parallel time: {:?}", par_time);

    //Actor Model implementation
    let start = std::time::Instant::now();
    let actor_result = actor_word_count(&files);
    let actor_time = start.elapsed();

    //Prints the following messages
    println!("\nActor model result: \n{:?}", actor_result);
    println!("\nActor model time: {:?}", actor_time);


    //Performance comparison
    println!("Sequential time: {:?}", seq_time);
    println!("Parallel time: {:?}", par_time);
    println!("Actor model time: {:?}", actor_time);

    //Calculates the total time taken by the implementations in seconds
    let seq_time_secs = seq_time.as_secs() as f64 + seq_time.subsec_nanos() as f64 * 1e-9;
    let par_time_secs = par_time.as_secs() as f64 + par_time.subsec_nanos() as f64 * 1e-9;
    let actor_time_secs = actor_time.as_secs() as f64 + actor_time.subsec_nanos() as f64 * 1e-9;

    //Prints the following messages
    println!("Sequential time (in seconds): {:.6}", seq_time_secs);
    println!("Parallel time (in seconds): {:.6}", par_time_secs);
    println!("Actor model time (in seconds): {:.6}", actor_time_secs);

    println!("Speedup of Parallel over Sequential: {:.2}", seq_time_secs / par_time_secs);
    println!("Speedup of Actor Model over Sequential: {:.2}", seq_time_secs / actor_time_secs);
}
