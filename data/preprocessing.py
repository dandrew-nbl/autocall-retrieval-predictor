import pandas as pd
import numpy as np
from data.database import load_retrieval_data, load_production_data, load_shipping_data

def create_enriched_dataset():
    """Create the enriched dataset with all features"""
    # Load data
    df_fetch_requests = load_retrieval_data()
    df_production = load_production_data()
    df_shipping = load_shipping_data()
    
    # Convert dates
    df_fetch_requests['insert_date'] = pd.to_datetime(df_fetch_requests['insert_date'])
    df_fetch_requests['complete_date'] = pd.to_datetime(df_fetch_requests['complete_date'])
    df_fetch_requests['insert_dttm'] = pd.to_datetime(df_fetch_requests['insert_dttm'])
    df_fetch_requests['complete_dttm'] = pd.to_datetime(df_fetch_requests['complete_dttm'])
    
    # Merge production and shipping data
    df_enriched = pd.merge(
        df_fetch_requests, 
        df_production.rename(columns={'date': 'insert_date', 'prod_line': 'line'}),
        on=['insert_date', 'line'], 
        how='left'
    )
    
    df_enriched = pd.merge(
        df_enriched,
        df_shipping.rename(columns={'shipped_date': 'insert_date'}),
        on='insert_date',
        how='left'
    )
    
    # Create line and item type dummy variables
    for line in ['LOU1', 'LOU2', 'LOU3', 'LOU4', 'LOU5']:
        df_enriched[f'line_{line}'] = (df_enriched['line'] == line).astype(int)
        
    for item_type in ['SHRF', 'LABL']:
        df_enriched[f'item_type_{item_type}'] = (df_enriched['item_type'] == item_type).astype(int)
    
    # Add storage location type
    df_enriched['storage_location_type'] = df_enriched['from_location'].str[4:7]
    
    for storage_type in ['RSR', 'SSR']:
        df_enriched[f'storage_location_type_{storage_type}'] = (df_enriched['storage_location_type'] == storage_type).astype(int)
    
    # Add location statistics
    location_stats = df_enriched.groupby('from_location')['duration_in_minutes'].agg(
        location_avg_time='mean',
        location_median_time='median',
        location_std_time='std'
    ).fillna(0)
    
    df_enriched = pd.merge(
        df_enriched,
        location_stats,
        left_on='from_location',
        right_index=True,
        how='left'
    )
    
    # Add cross features
    df_enriched['rsr_with_shrf'] = ((df_enriched['storage_location_type'] == 'RSR') & 
                                    (df_enriched['item_type'] == 'SHRF')).astype(int)
    df_enriched['ssr_with_labl'] = ((df_enriched['storage_location_type'] == 'SSR') & 
                                   (df_enriched['item_type'] == 'LABL')).astype(int)
    
    # Add time-based features
    df_enriched['hour_of_day'] = df_enriched['insert_dttm'].dt.hour
    df_enriched['day_of_week'] = df_enriched['insert_dttm'].dt.dayofweek
    
    # Create dummy variables for hours and days
    for hour in range(1, 24):
        df_enriched[f'hour_{hour}'] = (df_enriched['hour_of_day'] == hour).astype(int)
        
    for day in range(1, 7):
        df_enriched[f'day_{day}'] = (df_enriched['day_of_week'] == day).astype(int)
    
    # Add business hours and shift indicators
    df_enriched['business_hours'] = ((df_enriched['hour_of_day'] >= 8) & 
                                   (df_enriched['hour_of_day'] < 17) &
                                   (df_enriched['day_of_week'] < 5)).astype(int)
    
    df_enriched['night_shift'] = ((df_enriched['hour_of_day'] >= 22) | 
                                 (df_enriched['hour_of_day'] < 6)).astype(int)
    
    df_enriched['weekend'] = (df_enriched['day_of_week'] >= 5).astype(int)
    
    # Add cases ratio
    df_enriched['cases_ratio'] = df_enriched['total_cases_produced'] / (df_enriched['total_cases_shipped'] + 1e-10)
    df_enriched['cases_ratio'] = df_enriched['cases_ratio'].clip(0, 10)
    
    # Add time + item type interactions
    df_enriched['morning_shrf'] = ((df_enriched['hour_of_day'] < 12) & 
                                 (df_enriched['item_type'] == 'SHRF')).astype(int)
    df_enriched['evening_labl'] = ((df_enriched['hour_of_day'] >= 17) & 
                                 (df_enriched['item_type'] == 'LABL')).astype(int)
    
    return df_enriched

def prepare_numerical_matrix(df_enriched):
    """Create the numerical matrix for model training"""
    numerical_columns = [
        'duration_in_minutes', 
        'total_cases_produced', 
        'total_cases_shipped',
        'line_LOU1', 'line_LOU2', 'line_LOU3', 'line_LOU4',
        'item_type_SHRF',
        'storage_location_type_RSR',
        'location_avg_time', 'location_median_time', 'location_std_time',
        'rsr_with_shrf', 'ssr_with_labl',
        'cases_ratio',
        'business_hours', 'night_shift', 'weekend',
        'morning_shrf', 'evening_labl'
    ]
    
    # Add hour and day features
    for hour in range(1, 24):
        numerical_columns.append(f'hour_{hour}')
        
    for day in range(1, 7):
        numerical_columns.append(f'day_{day}')
    
    # Select the columns that exist in the dataframe
    available_columns = [col for col in numerical_columns if col in df_enriched.columns]
    
    return df_enriched[available_columns]
