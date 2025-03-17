import numpy as np
import pandas as pd
import joblib
import os

# Load model and feature names
model_path = os.path.join(os.path.dirname(__file__), 'rf_model.joblib')
feature_names_path = os.path.join(os.path.dirname(__file__), 'feature_names.joblib')

rf_model = joblib.load(model_path)
feature_names = joblib.load(feature_names_path)

# Import the numerical dataset for Monte Carlo simulations
from data.preprocessing import create_enriched_dataset, prepare_numerical_matrix
df_enriched = create_enriched_dataset()
df_numerical = prepare_numerical_matrix(df_enriched)

def predict_retrieval_times_monte_carlo(
    item_type, 
    production_line, 
    cases_produced, 
    cases_shipped,
    day_of_week,
    hour_of_day,
    n_simulations=1000):
    """
    Predict retrieval times using Monte Carlo simulation to handle location uncertainty.
    
    Parameters:
    -----------
    item_type : str
        'SHRF' or 'LABL'
    production_line : str
        'LOU1', 'LOU2', 'LOU3', 'LOU4', or 'LOU5'
    cases_produced : float
        Forecasted total cases produced for the day
    cases_shipped : float
        Forecasted total cases shipped for the day
    day_of_week : int
        0=Monday through 6=Sunday
    hour_of_day : int
        Hour of the day (0-23)
    n_simulations : int
        Number of Monte Carlo simulations to run
    
    Returns:
    --------
    Dictionary with simulation results and statistics
    """
    
    # Create a base record with the known features
    base_record = {
        'total_cases_produced': cases_produced,
        'total_cases_shipped': cases_shipped,
        'cases_ratio': cases_produced / (cases_shipped + 1e-10),
        'line_LOU1': 1 if production_line == 'LOU1' else 0,
        'line_LOU2': 1 if production_line == 'LOU2' else 0,
        'line_LOU3': 1 if production_line == 'LOU3' else 0,
        'line_LOU4': 1 if production_line == 'LOU4' else 0,
        'item_type_SHRF': 1 if item_type == 'SHRF' else 0,
        'storage_location_type_RSR': 1,
        'business_hours': 1 if (hour_of_day >= 8 and hour_of_day < 17 and day_of_week < 5) else 0,
        'night_shift': 1 if (hour_of_day >= 22 or hour_of_day < 6) else 0,
        'weekend': 1 if day_of_week >= 5 else 0,
        'morning_shrf': 1 if (hour_of_day < 12 and item_type == 'SHRF') else 0,
        'evening_labl': 1 if (hour_of_day >= 17 and item_type == 'LABL') else 0,
        'rsr_with_shrf': 1 if item_type == 'SHRF' else 0,
        'ssr_with_labl': 0
    }
    
    # Add hour and day dummy variables
    for h in range(1, 24):
        base_record[f'hour_{h}'] = 1 if hour_of_day == h else 0
    
    for d in range(1, 7):
        base_record[f'day_{d}'] = 1 if day_of_week == d else 0
    
    # Filter for relevant records in df_numerical
    filter_mask = (
        (df_numerical['item_type_SHRF'] == base_record['item_type_SHRF']) &
        (df_numerical[f'line_{production_line}'] == 1)
    )
    
    matching_records = df_numerical[filter_mask]
    print(f"Found {len(matching_records)} matching records")
    
    if len(matching_records) > 0:
        # Sample location statistics from matching records
        predictions = []
        for _ in range(n_simulations):
            random_idx = np.random.randint(0, len(matching_records))
            selected_record = matching_records.iloc[random_idx]
            
            # Copy the base record and add location statistics
            simulation_record = base_record.copy()
            simulation_record['location_avg_time'] = selected_record['location_avg_time']
            simulation_record['location_median_time'] = selected_record['location_median_time']
            simulation_record['location_std_time'] = selected_record['location_std_time']
            
            # Ensure all required features are present
            simulation_df = pd.DataFrame([simulation_record])
            
            # Make sure all features are in the right order
            for feature in feature_names:
                if feature not in simulation_df.columns:
                    simulation_df[feature] = 0
            
            simulation_df = simulation_df[feature_names]
            
            # Make prediction and convert from log10 back to minutes
            log_prediction = rf_model.predict(simulation_df)[0]
            prediction_minutes = 10**log_prediction
            predictions.append(prediction_minutes)
        
        # Calculate statistics
        predictions = np.array(predictions)
        result = {
            'min': predictions.min(),
            '25th_percentile': np.percentile(predictions, 25),
            'median': np.median(predictions),
            'mean': predictions.mean(),
            '75th_percentile': np.percentile(predictions, 75),
            'max': predictions.max(),
            'std_dev': predictions.std(),
            'risk_45min': (predictions > 45).mean() * 100,
            'raw_predictions': predictions
        }
        return result
    else:
        return {"error": "No matching records found for this combination"}

def forecast_retrieval_risks(day_of_week, shipping_forecast, production_forecast, 
                            lines=None, materials=None, hours=None):
    """
    Generate a daily retrieval risk forecast
    
    Parameters:
    -----------
    day_of_week: int
        Day of week (0=Monday, 1=Tuesday, etc.)
    shipping_forecast: float
        Forecasted cases to be shipped
    production_forecast: float
        Forecasted cases to be produced
    lines: list
        List of production lines to check
    materials: list
        List of materials to check
    hours: list
        List of hours to check
    """
    
    # Default values
    if lines is None:
        lines = ['LOU1', 'LOU2', 'LOU3', 'LOU4', 'LOU5']
    if materials is None:
        materials = ['SHRF', 'LABL']
    if hours is None:
        hours = [9, 13, 17, 21]  # 9AM, 1PM, 5PM, 9PM
    
    # Create a results table
    results = []
    
    for line in lines:
        for material in materials:
            for hour in hours:
                # Run Monte Carlo simulation
                sim_result = predict_retrieval_times_monte_carlo(
                    item_type=material,
                    production_line=line,
                    cases_produced=production_forecast,
                    cases_shipped=shipping_forecast,
                    day_of_week=day_of_week,
                    hour_of_day=hour,
                    n_simulations=200
                )
                
                # Calculate risk metrics
                if 'raw_predictions' in sim_result:
                    predictions = sim_result['raw_predictions']
                    risk_30min = (predictions > 30).mean() * 100
                    risk_45min = (predictions > 45).mean() * 100
                    
                    results.append({
                        'line': line,
                        'material': material,
                        'time': f"{hour:02d}:00",
                        'median_minutes': round(sim_result['median'], 1),
                        'max_minutes': round(sim_result['max'], 1),
                        'risk_over_30min': round(risk_30min, 1),
                        'risk_over_45min': round(risk_45min, 1),
                        'risk_level': 'High' if risk_45min > 20 else 'Medium' if risk_45min > 5 else 'Low'
                    })
    
    # Convert to dataframe
    return pd.DataFrame(results)
