import numpy as np
import pandas as pd
import joblib
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
data_dir = os.path.join(project_root, "data")
sys.path.append(data_dir)

# Load model and feature names
model_path = os.path.join(os.path.dirname(__file__), 'rf_model.joblib')
feature_names_path = os.path.join(os.path.dirname(__file__), 'feature_names.joblib')

rf_model = joblib.load(model_path)
feature_names = joblib.load(feature_names_path)


# Import the numerical dataset for predictions
from data.preprocessing import create_enriched_dataset, prepare_numerical_matrix
from data.database import get_future_production_schedule

def forecast_daily_retrieval_times(future_production_schedule):
    """
    Forecast average retrieval times based on future production schedule
    
    Parameters:
    -----------
    future_production_schedule: DataFrame
        DataFrame with columns: Date, Job_Number, Total_Cases
        This is the production schedule for future dates
    
    Returns:
    --------
    DataFrame with daily average retrieval time forecasts
    """
    # Load model and feature names
    # model = joblib.load('rf_model.joblib')
    # feature_names = joblib.load('feature_names.joblib')
    model = joblib.load(model_path)
    feature_names = joblib.load(feature_names_path)
    
    # First, calculate the historical average cases_ratio from your training data
    df_enriched = create_enriched_dataset()
    df_numerical, list_of_prod_lines, list_of_item_types = prepare_numerical_matrix(df_enriched)
    
    # Get the average cases_ratio from historical data
    avg_cases_ratio = df_numerical['cases_ratio'].mean()
    
    # Aggregate future production by date
    daily_production = future_production_schedule.groupby('Date')['Total_Cases'].sum().reset_index()
    daily_production = daily_production.rename(columns={'Total_Cases': 'total_cases_produced_for_day'})
    
    # Use the historical cases_ratio to estimate shipping volumes
    daily_production['total_cases_shipped_for_day'] = daily_production['total_cases_produced_for_day'] / avg_cases_ratio
    
    # Calculate cases_ratio (should be close to the average, but preserves the calculation method)
    epsilon = 1e-10     # Add epsilon to avoid dividing by zero
    daily_production['cases_ratio'] = daily_production['total_cases_produced_for_day'] / (daily_production['total_cases_shipped_for_day'] + epsilon)
    daily_production['cases_ratio'] = daily_production['cases_ratio'].clip(0, 10)  # Limit ratios to between 0-10, to avoid outliers
    
    # Add day of week
    daily_production['day_of_week'] = pd.to_datetime(daily_production['Date']).dt.dayofweek
    
    # Get average location statistics from training data
    avg_location_stats = {
        'location_avg_time': df_numerical['location_avg_time'].mean(),
        'location_median_time': df_numerical['location_median_time'].mean(),
        'location_std_time': df_numerical['location_std_time'].mean()
    }
    
    # Create predictions for each line and material type
    results = []
    
    for date_idx, row in daily_production.iterrows():
        date = row['Date']
        cases_produced = row['total_cases_produced_for_day']
        cases_shipped = row['total_cases_shipped_for_day']
        cases_ratio = row['cases_ratio']
        
        # For each line and material type combination
        for line in list_of_prod_lines + ['LOU5']:  # Add back LOU5 for predictions
            for material in list_of_item_types + ['LABL']:  # Add back LABL for predictions
                # Create feature vector
                X = {
                    'total_cases_produced_for_day': cases_produced,
                    'total_cases_shipped_for_day': cases_shipped,
                    'cases_ratio': cases_ratio,
                }
                
                # Add line features
                for prod_line in list_of_prod_lines:
                    X[f'line_{prod_line}'] = 1 if line == prod_line else 0
                
                # Add item type features
                for item_typ in list_of_item_types:
                    X[f'item_type_{item_typ}'] = 1 if material == item_typ else 0
                
                # Add location statistics
                X.update(avg_location_stats)
                
                # Prepare for prediction
                X_df = pd.DataFrame([X])
                
                # Ensure all required features are present in the right order
                for feature in feature_names:
                    if feature not in X_df.columns:
                        X_df[feature] = 0
                
                X_df = X_df[feature_names]
                
                # Predict
                log_prediction = model.predict(X_df.values)[0]
                prediction_minutes = 10**log_prediction
                
                results.append({
                    'Date': date,
                    'Line': line,
                    'Material': material,
                    'Predicted_Retrieval_Time': prediction_minutes,
                    # 'Cases_Produced': cases_produced,
                    # 'Cases_Shipped': cases_shipped,
                    # 'Cases_Ratio': cases_ratio
                })
    
    results_df = pd.DataFrame(results)
    
    # Calculate daily averages
    daily_avg = results_df.groupby('Date')['Predicted_Retrieval_Time'].mean().reset_index()
    daily_avg = daily_avg.rename(columns={'Predicted_Retrieval_Time': 'Avg_Retrieval_Time'})
    
    # Merge with original data
    final_df = pd.merge(daily_production, daily_avg, on='Date')
    
    return final_df, results_df

if __name__ == '__main__':
    # Example usage
    future_schedule = get_future_production_schedule()
    daily_forecast, detailed_forecast = forecast_daily_retrieval_times(future_schedule)

    # Print summary
    print("Daily Average Retrieval Time Forecast:")
    #print(daily_forecast[['Date', 'total_cases_produced_for_day', 'Avg_Retrieval_Time']])
    #print(daily_forecast)
    print(detailed_forecast)
