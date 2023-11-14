use plotters::prelude::*;
use plotters::prelude::{DrawingArea,  FontDesc, FontStyle, Text}; 
use plotters::element::{Drawable}; 
use plotters::backend; 
use plotters::drawing::DrawingAreaErrorKind::BackendError; 
use serde::Deserialize;
use std::fs::File;
use std::io::Read; 
use std::cmp::{max, min}; 

#[derive(Deserialize)]
struct TrendData {
    start_price: String,
    end_price: String,
    start_date: String,
    end_date: String,
    trend_type: String,
    start_aligned_price: Option<String>,
    end_aligned_price: Option<String>,
} 

fn read_trend_data(filename: &str) -> Result<(Vec<Vec<f64>>, Vec<Vec<f64>>, Option<String>), Box<dyn std::error::Error>> {
    let mut file = File::open(filename)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let data: Vec<TrendData> = serde_json::from_str(&contents)?;

    let mut x_data: Vec<Vec<f64>> = Vec::new();
    let mut y_data: Vec<Vec<f64>> = Vec::new();
    let trend_type: Option<String> = data.get(0).map(|trend: &TrendData| trend.trend_type.clone()); 

    for (index, trend) in data.iter().enumerate() { 
        let start_price = trend.start_price.parse::<f64>()?;
        let end_price = trend.end_price.parse::<f64>()?; 

        let min_price = min(start_price as i64, end_price as i64);
        let max_price = max(start_price as i64, end_price as i64); 

        let mut y_values: Vec<f64> = Vec::new();

        for price in min_price..=max_price {
            y_values.push(price as f64);
        }

        let x_values = vec![index as f64 + 1.0 ; y_values.len()];

        y_data.push(y_values);
        x_data.push(x_values);
    }

    Ok((x_data, y_data, trend_type))
} 

fn calculate_bounds(x_data: &[Vec<f64>], y_data: &[Vec<f64>]) -> Option<(f64, f64, f64, f64)> {
    let max_x = x_data.iter().flatten().cloned().fold(f64::NEG_INFINITY, f64::max) + 3.0;
    let min_y = y_data.iter().flatten().cloned().fold(f64::INFINITY, f64::min) - 3.0;
    let max_y = y_data.iter().flatten().cloned().fold(f64::NEG_INFINITY, f64::max) + 3.0;

    if max_x.is_finite() && min_y.is_finite() && max_y.is_finite() {
        Some((0.0, max_x, min_y, max_y))
    } else {
        None
    }
} 

fn draw_multiple_scatter_series(x_data: Vec<Vec<f64>>, y_data: Vec<Vec<f64>>, trend_type: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    // Create a drawing area with a Cartesian coordinate system
    let root = BitMapBackend::new("./figure/scatter_plot.png", (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    // Create a chart context
    let bounds = calculate_bounds(&x_data, &y_data);
    if let Some((x_min, x_max, y_min, y_max)) = bounds { 
        let mut chart = ChartBuilder::on(&root)
            .caption("Scatter Plot", ("sans-serif", 20).into_font())
            .margin(10)
            .x_label_area_size(40)
            .y_label_area_size(40)
            .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

        // Configure the mesh and draw it
        chart
            .configure_mesh()
            .x_desc("X")
            .y_desc("Y")
            .axis_desc_style(("sans-serif", 15)) 
            .draw()?;

        // Plot the scatter series
        for (i, (x_data, y_data)) in x_data.iter().zip(y_data.iter()).enumerate() {
            let color = if i % 2 == 0 {
                RGBColor(0, 0, 255) // Blue for even series
            } else {
                RGBColor(255, 0, 0) // Red for odd series
            }; 
            chart.draw_series(
                y_data
                    .iter()
                    .zip(x_data.iter())
                    .map(|(y, x)| Circle::new((*x, *y), 5, ShapeStyle::from(&color).filled())),
            )?;
        }

        return Ok(()); 
    } else {
        return Err("Invalid data".into()); 
    }      
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename: &str = "./data/pf_json.json"; 
    let (x_data, y_data, trend_type) = read_trend_data(filename)?;     
    
    draw_multiple_scatter_series(x_data, y_data, trend_type)?;

    Ok(())
}