use std::collections::HashMap;
use std::fs::File;
use std::io::Write; 
use std::io::{BufRead, BufReader};
use std::sync::Mutex; 
use lazy_static::lazy_static; 
use serde_json::{self};  


lazy_static! {
    static ref FLAG: Mutex<bool> = Mutex::new(true);
    static ref HIGHEST_IN_SESSION: Mutex<usize> = Mutex::new(0); 
    static ref LOWEST_IN_SESSION: Mutex<usize> = Mutex::new(0);      

} 

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
    high: f32, 
    low: f32, 
    close: f32,
    aligned_high: f32, 
    aligned_low: f32, 
    aligned_close: f32,
    high_index: usize, 
    low_index: usize, 
    close_index: usize,    
    yesterday: Option<String>, 
    serial_number: usize, 
}

impl Clone for Stock {
    fn clone(&self) -> Self {
        Stock {
            trade_date: self.trade_date.clone(),
            high: self.high,
            low: self.low,
            close: self.close, 
            aligned_high: self.aligned_high, 
            aligned_low: self.aligned_low, 
            aligned_close: self.aligned_close,
            high_index: self.high_index,   
            low_index: self.low_index,    
            close_index: self.close_index,   
            yesterday: self.yesterday.clone(), 
            serial_number: self.serial_number,             
        }
    }
} 

#[derive(Debug, Clone)] 
enum TrendType {
    Upward,
    Downward,     
}

// By adding Clone to the list of derived traits using #[derive(Debug, Clone)], 
//the Trend struct will automatically implement the Clone trait.
#[derive(Debug, Clone)] 
struct Trend {    
    trend_type: TrendType,    
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
        let trade_date = fields[1].to_string();
        let high = fields[3].parse::<f32>().expect("Failed to parse close value");
        let low = fields[4].parse::<f32>().expect("Failed to parse close value");
        let close = fields[5].parse::<f32>().expect("Failed to parse close value");
        let stock = Stock {
            trade_date: trade_date.clone(),
            high, 
            low, 
            close,
            aligned_high: 0.0, 
            aligned_low: 0.0, 
            aligned_close: 0.0,
            high_index: 0,
            low_index: 0,
            close_index: 0, 
            yesterday: None, 
            serial_number: 0, 
        };
        stocks.push(stock);
    }
    stocks
} 


fn assign_additional_fields(stocks: &mut [Stock]) {
    let element_index_pairs: Vec<(f32, usize)> = generate_element_index_pairs();
    let high_values: Vec<f32> = stocks.iter().map(|stock| stock.high).collect(); 
    let low_values: Vec<f32> = stocks.iter().map(|stock| stock.low).collect(); 
    let close_values: Vec<f32> = stocks.iter().map(|stock| stock.close).collect();    
    
    let closest_indices_for_high = find_closest_indices(&high_values, &element_index_pairs);
    let closest_indices_for_low = find_closest_indices(&low_values, &element_index_pairs);
    let closest_indices_for_close = find_closest_indices(&close_values, &element_index_pairs);
    
    for (stock, &closest_index) in stocks.iter_mut().zip(closest_indices_for_high.iter()) {
        stock.aligned_high = closest_index.0; 
        stock.high_index = closest_index.1;  
    } 

    for (stock, &closest_index) in stocks.iter_mut().zip(closest_indices_for_low.iter()) {
        stock.aligned_low = closest_index.0; 
        stock.low_index = closest_index.1;
    }

    for (stock, &closest_index) in stocks.iter_mut().zip(closest_indices_for_close.iter()) {
        stock.aligned_close = closest_index.0; 
        stock.close_index = closest_index.1; 
    }

    for i in 1..stocks.len() {
        let previous_date = stocks[i - 1].trade_date.clone();
        stocks[i].yesterday = previous_date.into(); 
    }    

    for (index, stock) in stocks.iter_mut().enumerate() {
        stock.serial_number = index;
    } 
    
} 

fn generate_pattern_vector() -> Vec<f32> {
    let mut pattern_vec = Vec::new();
    // Range: 0 to 5 (exclusive), space: 0.25
    for i in 0..20 {
        let element = 0.25 * i as f32;
        if element < 5.0 {
            pattern_vec.push(element);
        } else {
            break;
        }
    }
    // Range: 5 to 20 (exclusive), space: 0.50
    for i in 0..30 {
        let element = 5.0 + 0.50 * i as f32;
        if element < 20.0 {
            pattern_vec.push(element);
        } else {
            break;
        }
    }
    // Range: 20 to 100 (exclusive), space: 1.00
    for i in 0..80 {
        let element = 20.0 + 1.00 * i as f32;
        if element < 100.0 {
            pattern_vec.push(element);
        } else {
            break;
        }
    }
    // Range: 100 to 200 (exclusive), space: 2.00
    for i in 0..50 {
        let element = 100.0 + 2.00 * i as f32;
        if element < 200.0 {
            pattern_vec.push(element);
        } else {
            break;
        }
    }
    // Range: 200 to 2000 (inclusive), space: 4.00
    for i in 0..451 {
        let element = 200.0 + 4.00 * i as f32;
        if element <= 2000.0 {
            pattern_vec.push(element);
        } else {
            break;
        }
    }
    pattern_vec
} 

fn generate_element_index_pairs() -> Vec<(f32, usize)> {
    let pattern_vector = generate_pattern_vector();
    let mut element_index_pairs = Vec::new();
    let mut index = 0;
    let mut element_index_map: HashMap<u32, usize> = HashMap::new();
    for element in pattern_vector {
        let key = (element * 100.0) as u32; // Convert f32 to u32 for use as key
        element_index_map.insert(key, index);
        index += 1;
    }    
    for (element, index) in element_index_map {
        let element_f32 = element as f32 / 100.0; // Convert u32 back to f32
        element_index_pairs.push((element_f32, index));
    } 
    // Sort by index 
    element_index_pairs.sort_by_key(|&(_, index)| index); 
    element_index_pairs
} 

fn find_closest_indices(vector: &[f32], element_index_pairs: &[(f32, usize)]) -> Vec<(f32, usize)> {
    let mut closest_indices = Vec::new();
    for &element in vector {
        let closest_index = match element_index_pairs.binary_search_by(|&(pair_element, _)| pair_element.partial_cmp(&element).unwrap()) {
            Ok(index) => index,
            Err(index) => {
                if index > 0 {
                    index - 1
                } else {
                    continue; // No element lower than the current element found
                }
            }
        };
        let closest_pair = element_index_pairs[closest_index];
        closest_indices.push((closest_pair.0, closest_pair.1));
    }
    closest_indices
} 

fn find_initial_trend(stocks: &[Stock]) -> HashMap<String, String> {
    let mut initial_trend: HashMap<String, String> = HashMap::new(); 
    let Some(day_zero) = stocks.first() else {todo!() }; 
    let day_zero_high: usize = day_zero.high_index;
    let day_zero_low: usize = day_zero.low_index;        
    let day_zero_index: usize = day_zero.close_index;   
    let day_zero_date: String = day_zero.trade_date.clone();     
    
    for stock in stocks.iter().skip(1) {
        let current_high: usize = stock.high_index;
        let current_low: usize = stock.low_index;        
        if current_high > day_zero_high {   
            let index_diff: usize = current_high - day_zero_high;
            if index_diff >= 2 {        // && price_diff >= 0.0
                initial_trend.insert("start_date".to_string(), day_zero_date.to_string());    
                initial_trend.insert("start_price".to_string(), day_zero_index.to_string()); 
                initial_trend.insert("trend_type".to_string(), format!("{:?}", TrendType::Upward));                    
                break;
            };
        } else if current_low < day_zero_low {   
            let index_diff: usize = day_zero_low - current_low; 
            if index_diff >= 2 {        // && price_diff >= 0.0                 
                initial_trend.insert("start_date".to_string(), day_zero_date.to_string());    
                initial_trend.insert("start_price".to_string(), day_zero_index.to_string()); 
                initial_trend.insert("trend_type".to_string(), format!("{:?}", TrendType::Downward));                      
                break;
            };             
        }
    }     
    initial_trend  
} 

fn read_integer_from_file(file_path: &str) -> Result<u32, std::io::Error> {
    // Open the file in read mode
    let file = File::open(file_path)?;

    // Create a buffered reader to read the file line by line
    let reader = BufReader::new(file);

    // Read the first line from the file
    if let Some(line) = reader.lines().next() {
        // Attempt to parse the line as a u32 integer
        if let Ok(value) = line?.parse::<u32>() {
            return Ok(value);
        }
    }

    // If reading or parsing fails, return an error
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Failed to read integer from file",
    ))
} 

fn write_integer_to_file(value: u32, file_path: &str) {
    // Open the file in write mode, creating it if it doesn't exist
    let mut file = File::create(file_path).expect("Failed to create the file");

    // Convert the integer to a string
    let value_str = value.to_string();

    // Write the string representation of the integer to the file
    file.write_all(value_str.as_bytes())
        .expect("Failed to write to the file");
} 

/*
In Rust, you cannot use the let keyword to declare a static mutable va
riable within a function. Static variables need to be declared at the 
module level, outside of any function.
*/

fn analyze_time_series(stocks: &mut [Stock]) -> Vec<HashMap<String, String>> {
    let mut trends: Vec<HashMap<String, String>> = Vec::new();
    // Find the inital trend in stock data 
    let initial_trend = find_initial_trend(&stocks);

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
    
    for stock in stocks.iter().skip(1) {                   
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
            if (stock.high_index as f64) > highest_in_session { 
                highest_in_session = stock.high_index as f64;  
                hashmap.insert(String::from("highest_in_session"), highest_in_session.to_string());  
                conversion_threshold = (stock.high_index as f64) - THRESHOLD;                  
                hashmap.insert(String::from("lower_conversion_threshold"), conversion_threshold.to_string());   
                continous_count += 1; 
            } else if (stock.low_index as f64) <= lower_conversion_threshold {                                    
                // Check if there is a previous trend_data 
                if let Some(last_trend) = trends.last_mut() {
                    if continous_count == 0 {  
                        // last_trend.insert("end_price".to_string(), (stock.high_index).to_string()); 
                        last_trend.insert("end_price".to_string(), (lower_conversion_threshold + 3.0).to_string());  
                        last_trend.insert("end_aligned_price".to_string(), (stock.aligned_low).to_string());   // debug 

                    } else {                         
                        last_trend.insert("end_price".to_string(), highest_in_session.to_string());  
                        last_trend.insert("end_aligned_price".to_string(), (stock.aligned_low).to_string());   // debug
                    }                      
                    let end_date = match stock.yesterday.as_ref() {
                        Some(date) => date.to_string(),
                        None => String::new(),
                    }; 
                    last_trend.insert("end_date".to_string(), end_date);                      
                }

                let mut trend_data = HashMap::new();
                trend_data.insert("start_date".to_string(), stock.trade_date.to_string());
                if continous_count == 0 {  
                    trend_data.insert("start_price".to_string(), (lower_conversion_threshold + 2.0).to_string());
                } else {
                    trend_data.insert("start_price".to_string(), (highest_in_session - 1.0).to_string()); 
                }
                trend_data.insert("trend_type".to_string(), format!("{:?}", TrendType::Downward)); 
                trend_data.insert("start_aligned_price".to_string(), (stock.aligned_low).to_string());   // debug 
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
            if (stock.low_index as f64) < lowest_in_session {                    
                lowest_in_session = stock.low_index as f64;   
                conversion_threshold = (stock.low_index as f64) + THRESHOLD;    
                hashmap.insert(String::from("lowest_in_session"), lowest_in_session.to_string());               
                hashmap.insert(String::from("upper_conversion_threshold"), conversion_threshold.to_string()); 
                continous_count += 1; 
            } else if (stock.high_index as f64) >= upper_conversion_threshold {                   
                if let Some(last_trend) = trends.last_mut() {
                    if continous_count == 0 {  
                        last_trend.insert("end_price".to_string(), (upper_conversion_threshold - 3.0).to_string());  
                    } else {
                        last_trend.insert("end_price".to_string(), lowest_in_session.to_string()); 
                    }
                    let end_date = match stock.yesterday.as_ref() {
                        Some(date) => date.to_string(),
                        None => String::new(),
                    }; 
                    last_trend.insert("end_date".to_string(), end_date);                       
                }

                let mut trend_data = HashMap::new();
                trend_data.insert("start_date".to_string(), stock.trade_date.to_string());                     
                if continous_count == 0 {  
                    trend_data.insert("start_price".to_string(), (upper_conversion_threshold - 2.0).to_string());
                } else {
                    trend_data.insert("start_price".to_string(), (lowest_in_session + 1.0).to_string()); 
                }
                trend_data.insert("trend_type".to_string(), format!("{:?}", TrendType::Upward)); 
                trend_data.insert("start_aligned_price".to_string(), (stock.aligned_high).to_string()); 

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


fn write_data_to_json(data: &Vec<HashMap<String, String>>, file_path: &str) {
    let json_data = serde_json::to_string_pretty(data).expect("Failed to serialize data to JSON");
    let mut file = File::create(file_path).expect("Failed to create file");
    file.write_all(json_data.as_bytes()).expect("Failed to write data to file");
} 


fn main() {
    // Read stock data from file
    let file_path = "data/sailisi.csv";
    let stocks = read_stock_data(file_path);

    // Align closing prices
    let mut stocks_mut: Vec<Stock> = stocks.clone(); 
    assign_additional_fields(&mut stocks_mut);  

    for stock in stocks_mut.iter().take(3) {
        println!("{:?}", stock); 
    }
    
    let results: Vec<HashMap<String, String>> = analyze_time_series(&mut stocks_mut);     
    for row in results.iter().take(5) {
        // Process or print each row/item as desired
        println!("{:?}", row);
    } 

    let file_path = "./data/pf_json.json"; 
    write_data_to_json(&results, file_path);  

}