import pandas as pd
import numpy as np
from database import load_item_lookup_data, load_retrieval_data, load_production_data, load_shipping_data #, load_daily_jobs_data

#Removing import for total daily jobs since we're not using that feature anymore

def create_enriched_dataset():
    """Create the enriched dataset with all features"""
    # Load data
    df_item_lookup = load_item_lookup_data()
    df_fetch_requests = load_retrieval_data()
    df_production = load_production_data()
    df_shipping = load_shipping_data()
    df_total_daily_jobs = load_daily_jobs_data()
    

    # Merge item lookup and fetch requests
    df_enriched = pd.merge(
        df_item_lookup,
        df_fetch_requests, 
        left_on='item', 
        right_on='sku'
        how='inner'
    )

    # Merge with production data
    df_enriched = pd.merge(
        df_enriched, 
        df_production,
        left_on='insert_date', 
        right_on='date'
        how='left'      #Change this to an inner join later
    )

    # Merge with shipping data
    df_enriched = pd.merge(
        df_enriched, 
        df_shipped,
        left_on='insert_date', 
        right_on='shipped_date'
        how='inner'
    )


#Removing merge to total_daily_jobs because we're not using that feature anymore
    # # Merge with total daily jobs data
    # df_enriched = pd.merge(
    #     df_enriched, 
    #     df_total_daily_jobs,
    #     left_on='insert_date', 
    #     right_on='shipped_date'
    #     how='inner'
    # ) 
    
    
    # Create a copy of the DataFrame so we don't alter the original DataFrame
    df_enhanced = df_enriched.copy()
    
    # Create line and item type dummy variables
    for line in ['LOU1', 'LOU2', 'LOU3', 'LOU4', 'LOU5']:
        df_enhanced[f'line_{line}'] = (df_enhanced['line'] == line).astype(int)
        
    for item_type in ['SHRF', 'LABL']:
        df_enhanced[f'item_type_{item_type}'] = (df_enhanced['item_type'] == item_type).astype(int)

    #### FEATURE ENGINEERING ####

    # Add Location Statistics
    location_avg = df_enhanced.groupby('location_grouped')['duration_in_minutes'].mean().to_dict()
    location_median = df_enhanced.groupby('location_grouped')['duration_in_minutes'].median().to_dict()
    location_std = df_enhanced.groupby('location_grouped')['duration_in_minutes'].std().fillna(0).to_dict()

    df_fetch_requests_enhanced['location_avg_time'] = df_fetch_requests_enhanced['location_grouped'].map(location_avg)
    df_fetch_requests_enhanced['location_median_time'] = df_fetch_requests_enhanced['location_grouped'].map(location_median)
    df_fetch_requests_enhanced['location_std_time'] = df_fetch_requests_enhanced['location_grouped'].map(location_std)
    
    # Calculate cases produced/shipped ratios
    # Add a small value to avoid division by zero
    epsilon = 1e-10
    df_fetch_requests_enhanced['cases_ratio'] = df_fetch_requests_enhanced['total_cases_produced_for_day'] / (df_fetch_requests_enhanced['total_cases_shipped_for_day'] + epsilon)

    # Cap extreme ratios to prevent outliers
    df_fetch_requests_enhanced['cases_ratio'] = df_fetch_requests_enhanced['cases_ratio'].clip(0, 10)
    
    return df_enriched

def prepare_numerical_matrix(df_enriched):
    """Create the numerical matrix for model training"""
    numerical_columns = [
        'duration_in_minutes', 
        'total_cases_produced_for_day', 
        'total_cases_shipped_for_day',
        'line_LOU1', 'line_LOU2', 'line_LOU3', 'line_LOU4',
        'item_type_SHRF',
        #'storage_location_type_RSR',
        'location_avg_time', 'location_median_time', 'location_std_time',
        #'rsr_with_shrf', 'ssr_with_labl',
        'cases_ratio',
        # 'business_hours', 'night_shift', 'weekend',
        # 'morning_shrf', 'evening_labl'
    ]

    
    # # Add hour and day features
    # for hour in range(1, 24):
    #     numerical_columns.append(f'hour_{hour}')
        
    # for day in range(1, 7):
    #     numerical_columns.append(f'day_{day}')
    
    # Select the columns that exist in the dataframe
    available_columns = [col for col in numerical_columns if col in df_enriched.columns]
    
    return df_enriched[available_columns]

# TESTING
df_enriched_test = create_enriched_dataset()
prepare_numerical_matrix(df_enriched_test)

