use std::fs::File;
use std::io::{self, Write, BufRead, BufReader}; 
use std::sync::Mutex; 
use lazy_static::lazy_static; 
use serde_json::{self};  
use num_traits::Float; 
use mysql::*;
use mysql::prelude::*;
use std::collections::{HashSet, HashMap}; 
use std::error::Error;
use csv::ReaderBuilder; 
use rand::seq::SliceRandom; 
use rand::thread_rng;  

lazy_static! {
    static ref HASHMAP: Mutex<HashMap<String, String>> = Mutex::new({
        let mut ctrl_vars = HashMap::new();        
        ctrl_vars.insert(String::from("highest_in_session"), String::from("unknown")); 
        ctrl_vars.insert(String::from("lowest_in_session"), String::from("unknown")); 
        ctrl_vars.insert(String::from("upper_conversion_threshold"), String::from("unknown")); 
        ctrl_vars.insert(String::from("lower_conversion_threshold"), String::from("unknown"));  
        ctrl_vars.insert(String::from("current_trend"), String::from("Downward"));  
        ctrl_vars  
    });
} 

#[derive(Debug)]
struct Stock {
    trade_date: String, 
    close: f32,   
    yesterday: Option<String>, 
    serial_number: usize, 
}

#[derive(Debug, Clone)] 
enum TrendType {
    Upward,
    Downward,     
}

#[derive(Debug, Clone)] 
struct Trend {    
    trend_type: TrendType,    
} 

#[derive(Debug, Clone)]  
struct Ratio {
    trade_date: String,
    relative_strength: f32,
    yesterday: Option<String>, 
}

fn generate_pattern_vec() -> Vec<f32> {
    let q = 1.065;
    let min_value = 0.01;
    let max_value = 2000.0;

    let mut pattern_vec = vec![1.0]; 
    let mut current_value = 1.0;     

    while current_value < max_value {
        current_value *= q;
        if current_value >= min_value && current_value <= max_value {
            pattern_vec.push(current_value);
        }
    }

    current_value = 1.0;
    while current_value >= min_value {
        current_value /= q;
        if current_value >= min_value {
            pattern_vec.push(current_value);
        }
    }

    pattern_vec.sort_by(|a, b| a.partial_cmp(b).unwrap()); 
    pattern_vec.iter().map(|&value| Float::round(value * 10000.0) / 10000.0).collect() 
}

fn write_pattern_vec_to_file(pattern_vec: &[f32]) -> io::Result<()> {
    let mut file = File::create("./data/relative_strength.txt")?;
    for value in pattern_vec {
        writeln!(file, "{}", value)?;
    }
    Ok(())
} 

fn read_stock_data(file_path: &str) -> Vec<Stock> {
    let file = File::open(file_path).expect("Failed to open the CSV file");
    let reader = BufReader::new(file);
    let mut stocks = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        if index == 0 {
            continue; // Skip the header line
        }
        let line = line.expect("Failed to read line from the CSV file");
        let fields: Vec<&str> = line.split(',').collect();
        let trade_date = fields[0].to_string();
        let close = fields[4].parse::<f32>().expect("Failed to parse close value");         
        let stock = Stock {
            trade_date,
            close,
            yesterday: None,
            serial_number: index - 1,
        };
        stocks.push(stock);
    }
    stocks
} 

fn calculate_mean(vector: &[Stock]) -> f32 {
    let sum: f32 = vector.iter().map(|stock| stock.close).sum();
    sum / vector.len() as f32
} 

fn calculate_ratio(pattern_vec_a: &[Stock], pattern_vec_b: &[Stock]) -> Vec<Ratio> {
    let mean_a = calculate_mean(pattern_vec_a);
    let mean_b = calculate_mean(pattern_vec_b);
    let scale_factor = (mean_b / mean_a) as f32;    

    let mut ratios = Vec::new();

    let mut iter_a = pattern_vec_a.iter().peekable();
    let mut iter_b = pattern_vec_b.iter().peekable();

    while let (Some(stock_a), Some(stock_b)) = (iter_a.peek(), iter_b.peek()) {
        if stock_a.trade_date == stock_b.trade_date {            
            let relative_strength = (stock_a.close / stock_b.close * scale_factor * 10000.0).round() / 10000.0;
            let ratio = Ratio {
                trade_date: stock_a.trade_date.clone(),
                relative_strength: relative_strength, 
                yesterday: Some(String::from("NaN")),  
            };
            ratios.push(ratio);
            iter_a.next(); 
            iter_b.next();              
        } else if stock_a.trade_date < stock_b.trade_date {            
            iter_a.next();            
        } else {
            iter_b.next();            
        }
    }

    ratios
}

fn find_closest_indices(ratios: &[Ratio], pattern_vec: &[f32]) -> Vec<Ratio> {
    let mut closest_ratios = Vec::new();

    for ratio in ratios {
        let closest_index = match pattern_vec.binary_search_by(|&value| value.partial_cmp(&ratio.relative_strength).unwrap()) {
            Ok(index) => index,
            Err(index) => {
                if index > 0 {
                    index - 1
                } else {
                    continue; // No element lower than the current ratio.relative_strength found
                }
            }
        };

        closest_ratios.push(Ratio {
            trade_date: ratio.trade_date.clone(),
            relative_strength: closest_index as f32, 
            yesterday: Some(String::from("NaN")),  
        });
    }

    closest_ratios
} 

fn assign_additional_fields(ratios: &mut [Ratio]) {     
    for i in 1..ratios.len() {
        let previous_date = ratios[i - 1].trade_date.clone();
        ratios[i].yesterday = previous_date.into(); 
    }    
}

fn find_initial_trend(ratios: &[Ratio]) -> HashMap<String, String> {
    let mut initial_trend: HashMap<String, String> = HashMap::new(); 
    let Some(day_zero) = ratios.first() else {todo!() }; 
    let day_zero_strength: f32 = day_zero.relative_strength;      
    let day_zero_date: String = day_zero.trade_date.clone();     
    
    for ratio in ratios.iter().skip(1) {
        let current_ratio: f32 = ratio.relative_strength;               
        if current_ratio > day_zero_strength {   
            let index_diff: usize = (current_ratio - day_zero_strength) as usize; 
            if index_diff >= 2 {        
                initial_trend.insert("start_date".to_string(), day_zero_date.to_string());    
                initial_trend.insert("start_price".to_string(), day_zero_strength.to_string()); 
                initial_trend.insert("trend_type".to_string(), format!("{:?}", TrendType::Upward));                    
                break;
            };
        } else if current_ratio < day_zero_strength {   
            let index_diff: usize = (day_zero_strength - current_ratio) as usize; 
            if index_diff >= 2 {                        
                initial_trend.insert("start_date".to_string(), day_zero_date.to_string());    
                initial_trend.insert("start_price".to_string(), day_zero_strength.to_string()); 
                initial_trend.insert("trend_type".to_string(), format!("{:?}", TrendType::Downward));                      
                break;
            };             
        }
    }     
    initial_trend  
}

fn analyze_time_series(ratios: &mut [Ratio]) -> Vec<HashMap<String, String>> {
    let mut trends: Vec<HashMap<String, String>> = Vec::new();
    // Find the inital trend in stock data      
    let initial_trend = find_initial_trend(&ratios);

    // Access the values by key and assign them to variables
    let initial_start_price = initial_trend.get("start_price").cloned();
    
    let trend_type_zero: String = initial_trend.get("trend_type").cloned().unwrap_or(String::from("unknown"));
    let mut hashmap = HASHMAP.lock().unwrap();  
    hashmap.insert(String::from("current_trend"), trend_type_zero.clone()); 
    hashmap.insert(String::from("current_trend"), trend_type_zero.clone()); 
    
    if trend_type_zero == "Upward" { 
        if let Some(start_price) = initial_start_price {
            if let Ok(start_price_num) = start_price.parse::<i32>() {
                let highest_in_session: String = (start_price_num + 2).to_string();
                let lower_conversion_threshold: String = (start_price_num - 1).to_string();                 
                hashmap.insert(String::from("highest_in_session"), highest_in_session);
                hashmap.insert(String::from("lowest_in_session"), String::from("2000")); 
                hashmap.insert(String::from("lower_conversion_threshold"), lower_conversion_threshold);
            }
        }
    } else if trend_type_zero == "Downward" {
        if let Some(start_price) = initial_start_price {
            if let Ok(start_price_num) = start_price.parse::<i32>() {
                let lowest_in_session = (start_price_num - 2).to_string();
                let upper_conversion_threshold: String = (start_price_num + 1).to_string();                 
                hashmap.insert(String::from("lowest_in_session"), lowest_in_session);
                hashmap.insert(String::from("highest_in_session"), String::from("0"));  
                hashmap.insert(String::from("upper_conversion_threshold"), upper_conversion_threshold);
            }
        } 
    }    
    
    const THRESHOLD: f64 = 3.0; 
    let mut continous_count: i32 = 0; // Count the number of iterations matained for the current trend  
    trends.push(initial_trend.clone()); 
    let mut highest_in_session: f64 = 0.0;              // Placeholder value
    let mut lowest_in_session: f64 = 10000.0;           // Placeholder value
    let mut upper_conversion_threshold: f64 = 0.0;      // Placeholder value
    let mut lower_conversion_threshold: f64 = 0.0;      // Placeholder value     
    let mut conversion_threshold: f64 = 0.0;      
    
    for ratio in ratios.iter().skip(1) {                   
        let current_trend = hashmap.get("current_trend").unwrap().to_owned(); 
        if let Some(highest_in_session_str) = hashmap.get("highest_in_session") {
            if let Ok(value) = highest_in_session_str.parse::<f64>() {
                highest_in_session = value;                
            }
        } 
        if let Some(lowest_in_session_str) = hashmap.get("lowest_in_session") {
            if let Ok(value) = lowest_in_session_str.parse::<f64>() {
                lowest_in_session = value;                
            }
        }   
        if current_trend == "Upward" {         
            if let Some(lower_conversion_threshold_str) = hashmap.get("lower_conversion_threshold") {
                if let Ok(value) = lower_conversion_threshold_str.parse::<f64>() {
                    lower_conversion_threshold = value;                    
                }
            } 
            if (ratio.relative_strength as f64) > highest_in_session { 
                highest_in_session = ratio.relative_strength as f64;  
                hashmap.insert(String::from("highest_in_session"), highest_in_session.to_string());  
                conversion_threshold = (ratio.relative_strength as f64) - THRESHOLD;                  
                hashmap.insert(String::from("lower_conversion_threshold"), conversion_threshold.to_string());   
                continous_count += 1; 
            } else if (ratio.relative_strength as f64) <= lower_conversion_threshold {                                    
                // Check if there is a previous trend_data 
                if let Some(last_trend) = trends.last_mut() {
                    if continous_count == 0 {                           
                        last_trend.insert("end_price".to_string(), (lower_conversion_threshold + 3.0).to_string());            
                    } else {                         
                        last_trend.insert("end_price".to_string(), highest_in_session.to_string());                         
                    }                      
                    let end_date = match ratio.yesterday.as_ref() {
                        Some(date) => date.to_string(),
                        None => String::new(),
                    }; 
                    last_trend.insert("end_date".to_string(), end_date);                      
                }

                let mut trend_data = HashMap::new();
                trend_data.insert("start_date".to_string(), ratio.trade_date.to_string());
                if continous_count == 0 {  
                    trend_data.insert("start_price".to_string(), (lower_conversion_threshold + 2.0).to_string());
                } else {
                    trend_data.insert("start_price".to_string(), (highest_in_session - 1.0).to_string()); 
                }
                trend_data.insert("trend_type".to_string(), format!("{:?}", TrendType::Downward)); 
                trends.push(trend_data);           

                hashmap.insert(String::from("current_trend"), String::from("Downward")); 
                hashmap.insert(String::from("highest_in_session"), String::from("0.0")); 
                hashmap.insert(String::from("lowest_in_session"), lower_conversion_threshold.to_string());
                hashmap.insert(String::from("lower_conversion_threshold"), String::from("10000.0")); 
                hashmap.insert(String::from("upper_conversion_threshold"), highest_in_session.to_string());
                
                continous_count = 0;                 
            }
        } 
        else if current_trend == "Downward" { 
            if let Some(upper_conversion_threshold_str) = hashmap.get("upper_conversion_threshold") {
                if let Ok(value) = upper_conversion_threshold_str.parse::<f64>() {
                    upper_conversion_threshold = value;                    
                }
            } 
            if (ratio.relative_strength as f64) < lowest_in_session {                    
                lowest_in_session = ratio.relative_strength as f64;   
                conversion_threshold = (ratio.relative_strength as f64) + THRESHOLD;    
                hashmap.insert(String::from("lowest_in_session"), lowest_in_session.to_string());               
                hashmap.insert(String::from("upper_conversion_threshold"), conversion_threshold.to_string()); 
                continous_count += 1; 
            } else if (ratio.relative_strength as f64) >= upper_conversion_threshold {                   
                if let Some(last_trend) = trends.last_mut() {
                    if continous_count == 0 {  
                        last_trend.insert("end_price".to_string(), (upper_conversion_threshold - 3.0).to_string());  
                    } else {
                        last_trend.insert("end_price".to_string(), lowest_in_session.to_string()); 
                    }
                    let end_date = match ratio.yesterday.as_ref() {
                        Some(date) => date.to_string(),
                        None => String::new(),
                    }; 
                    last_trend.insert("end_date".to_string(), end_date);                       
                }

                let mut trend_data = HashMap::new();
                trend_data.insert("start_date".to_string(), ratio.trade_date.to_string());                     
                if continous_count == 0 {  
                    trend_data.insert("start_price".to_string(), (upper_conversion_threshold - 2.0).to_string());
                } else {
                    trend_data.insert("start_price".to_string(), (lowest_in_session + 1.0).to_string()); 
                }
                trend_data.insert("trend_type".to_string(), format!("{:?}", TrendType::Upward));                 

                trends.push(trend_data); 
                
                hashmap.insert(String::from("current_trend"), String::from("Upward")); 
                hashmap.insert(String::from("lowest_in_session"), String::from("10000.0"));                     
                hashmap.insert(String::from("highest_in_session"), upper_conversion_threshold.to_string()); 
                hashmap.insert(String::from("upper_conversion_threshold"), String::from("0.0")); 
                hashmap.insert(String::from("lower_conversion_threshold"), lowest_in_session.to_string()); 
                continous_count = 0; 
            }
        } else {
            println!("Something unexpected happened.") 
        }
    }
    trends 
} 

fn analyze_stock_data<'a>(pool: &'a Pool, ts_code_a: &'a str, ts_code_b: &'a str) -> &'a str {
    let stocks_a = match fetch_data(&pool, ts_code_a) {
        Ok(stocks) => stocks,
        Err(_) => return ts_code_a, // Handle the error case
    };    

    let stocks_b = match fetch_data(&pool, ts_code_b) {
        Ok(stocks) => stocks,
        Err(_) => return ts_code_a, // Handle the error case
    };    

    let pattern_vec = generate_pattern_vec(); 
    let ratios = calculate_ratio(&stocks_a, &stocks_b);
    
    if ratios.len() == 0 {
        if stocks_a.len() > stocks_b.len() {
            return ts_code_a;
        } else {
            return ts_code_b;
        }
    } 
    let mut closest_ratios = find_closest_indices(&ratios, &pattern_vec);

    assign_additional_fields(closest_ratios.as_mut_slice());    
    let results: Vec<HashMap<String, String>> = analyze_time_series(&mut closest_ratios);

    if let Some(last_result) = results.last() {
        if let Some(trend_type) = last_result.get("trend_type") {            
            if trend_type == "Downward" {                
                return ts_code_b;
            } else if trend_type == "Upward" {                
                return ts_code_a; 
            }
        }
    }

    // Default return value if no suitable trend_type is found
    ts_code_a  
} 

fn write_data_to_json(data: &Vec<HashMap<String, String>>, file_path: &str) {
    let json_data = serde_json::to_string_pretty(data).expect("Failed to serialize data to JSON");
    let mut file = File::create(file_path).expect("Failed to create file");
    file.write_all(json_data.as_bytes()).expect("Failed to write data to file");
}  

fn fetch_distinct_ts_codes(pool: &Pool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn()?;
    let query = "SELECT DISTINCT ts_code FROM stock.weekly";
    let mut result: Vec<String> = conn
        .query_map(query, |ts_code| ts_code)?;

    result.retain(|ts_code| !ts_code.ends_with(".BJ"));  
    Ok(result)
}  

fn fetch_data(pool: &Pool, ts_code: &str) -> Result<Vec<Stock>, Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn()?;
    let query = format!("SELECT trade_date, close FROM weekly 
            WHERE ts_code = '{}' 
            AND trade_date <= '2023-11-24' 
            ORDER BY trade_date ASC", ts_code);
    let result: Vec<Stock> = conn
        .query_map(query, |(trade_date, close)| Stock {
            trade_date,
            close,
            yesterday: Some(String::from("NaN")),
            serial_number: 0,
        })?;
    Ok(result)
}  

fn read_csv_column(filename: &str, start_row: usize) -> Result<Vec<String>, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut reader = ReaderBuilder::new().has_headers(false).from_reader(file);
    
    let mut iwencai_code: Vec<String> = Vec::new();
    
    for (index, result) in reader.records().enumerate() {
        if let Ok(record) = result {
            if index + 1 >= start_row {
                if let Some(field) = record.get(0) {
                    iwencai_code.push(field.to_owned());
                }
            }
        } else {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "Error reading CSV record",
            )));
        }
    }
    
    Ok(iwencai_code)
} 

fn calculate_intersection(iwencai_code: &[String], distinct_ts_codes: &[String]) -> HashSet<String> {
    let iwencai_set: HashSet<_> = iwencai_code.iter().cloned().collect();
    let distinct_set: HashSet<_> = distinct_ts_codes.iter().cloned().collect();

    let intersection: Vec<_> = iwencai_set.intersection(&distinct_set).cloned().collect();

    let filtered_intersection: HashSet<_> = intersection
        .into_iter()
        .filter(|code| !code.ends_with(".BJ"))
        .collect();

    filtered_intersection 
} 

fn tournament_elimination(pool: &Pool, candidates: HashSet<String>, exit_condition: usize) -> Vec<String> {
    // algorithm of "tournament-style elimination"  
    let mut remaining: Vec<String> = candidates.into_iter().collect();
    let mut round = 1;

    while remaining.len() > exit_condition {
        println!("Round {}", round);

        let mut grouped_indices: Vec<(usize, usize)> = Vec::new();
        let mut odd_element: Option<usize> = None;
        
        for i in (0..remaining.len()).step_by(2) {
            if i + 1 < remaining.len() {
                grouped_indices.push((i, i + 1));
            } else {                
                odd_element = Some(i);
            }
        }        
        
        let mut survived: Vec<String> = Vec::new();
        for (i, j) in grouped_indices {
            let ts_code_a = &remaining[i];
            let ts_code_b = &remaining[j];            
            let winner = analyze_stock_data(&pool, &ts_code_a, &ts_code_b);              
            survived.push(winner.to_string());
        }        
        
        if let Some(index) = odd_element {
            survived.push(remaining[index].clone());
        }         
        
        remaining = survived;        
        remaining.shuffle(&mut thread_rng());        
        round += 1;
    }

    remaining
} 



fn main() -> Result<(), Box<dyn std::error::Error>> {     
    let url = "mysql://root:<password>@localhost:3306/stock";
    let pool = Pool::new(url)?;
    
    let distinct_ts_codes = fetch_distinct_ts_codes(&pool)?; 
    let distinct_ts_codes_set: HashSet<String> = distinct_ts_codes.into_iter().collect();


    // let filename = "./data/2023-10-20日股价位于10周均线上方的股.csv"; 
    // let start_row = 2;

    // If you have better stock candicates, do a intersection! 
    // let iwencai_code = read_csv_column(filename, start_row)?; 
    // let intersection = calculate_intersection(&iwencai_code, &distinct_ts_codes);

    let exit_condition = 40; 
    let survived = tournament_elimination(&pool, distinct_ts_codes_set, exit_condition); 

    println!("----- -----");  
    println!("Survived elements: {:?}", survived); 

    Ok(()) 
} 

