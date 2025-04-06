import pandas as pd
import numpy as np
from database import load_item_lookup_data, load_retrieval_data, load_production_data, load_shipping_data #, load_daily_jobs_data

#Removing import for total daily jobs since we're not using that feature anymore

# Create line and item type dummy variables
list_of_prod_lines = ['LOU1', 'LOU2', 'LOU3', 'LOU4', 'LOU5']
list_of_item_types = ['SHRF', 'LABL']

# Remove the final element of each list, to avoid multicollinearity
list_of_prod_lines = list_of_prod_lines[:len(list_of_prod_lines)-1]
list_of_item_types = list_of_item_types[:len(list_of_item_types)-1]

def create_enhanced_dataset():
    """Create the enriched dataset with all features"""
    # Load data
    df_item_lookup = load_item_lookup_data()
    df_fetch_requests = load_retrieval_data()
    df_production = load_production_data()
    df_shipping = load_shipping_data()
    #df_total_daily_jobs = load_daily_jobs_data()

    # Merge item lookup and fetch requests
    df_enriched = pd.merge(
        df_item_lookup,
        df_fetch_requests, 
        left_on='item', 
        right_on='sku',
        how='inner'
    )

    # Set date format to string, prior to merge
    df_enriched['insert_date'] = pd.to_datetime(df_enriched['insert_date']).dt.strftime('%Y-%m-%d')
    df_production['date'] = pd.to_datetime(df_production['date']).dt.strftime('%Y-%m-%d')

    # Merge with production data
    df_enriched = pd.merge(
        df_enriched, 
        df_production,
        left_on='insert_date', 
        right_on='date',
        how='inner'
    )

    # Set date format to string, prior to merge
    df_shipping['shipped_date'] = pd.to_datetime(df_shipping['shipped_date']).dt.strftime('%Y-%m-%d')

    # Merge with shipping data
    df_enriched = pd.merge(
        df_enriched, 
        df_shipping,
        left_on='insert_date', 
        right_on='shipped_date',
        how='inner'
    )

    # Create a copy of the DataFrame so we don't alter the original DataFrame
    df_enhanced = df_enriched.copy()
    
    for line in list_of_prod_lines:
        df_enhanced[f'line_{line}'] = (df_enhanced['line'] == line).astype(int)
        
    #for item_type in ['SHRF', 'LABL']:
    for item_type in list_of_item_types:
        df_enhanced[f'item_type_{item_type}'] = (df_enhanced['item_type'] == item_type).astype(int)

    #### FEATURE ENGINEERING ####

    # Handle Rare Locations
    location_counts = df_enhanced['from_location'].value_counts()
    rare_locations = location_counts[location_counts < 10].index
    df_enhanced['location_grouped'] = df_enhanced['from_location'].apply(
        lambda x: 'RARE_LOCATION' if x in rare_locations else x
    )

    # Add Location Statistics
    location_avg = df_enhanced.groupby('location_grouped')['duration_in_minutes'].mean().to_dict()
    location_median = df_enhanced.groupby('location_grouped')['duration_in_minutes'].median().to_dict()
    location_std = df_enhanced.groupby('location_grouped')['duration_in_minutes'].std().fillna(0).to_dict()

    df_enhanced['location_avg_time'] = df_enhanced['location_grouped'].map(location_avg)
    df_enhanced['location_median_time'] = df_enhanced['location_grouped'].map(location_median)
    df_enhanced['location_std_time'] = df_enhanced['location_grouped'].map(location_std)
    
    # Calculate cases produced/shipped ratios
    # Add a small value to avoid division by zero
    epsilon = 1e-10
    df_enhanced['cases_ratio'] = df_enhanced['total_cases_produced_for_day'] / (df_enhanced['total_cases_shipped_for_day'] + epsilon)

    # Cap extreme ratios to prevent outliers
    df_enhanced['cases_ratio'] = df_enhanced['cases_ratio'].clip(0, 10)
    
    return df_enhanced

def prepare_numerical_matrix(df_enhanced):
    """Create the numerical matrix for model training"""
    numerical_columns = [
        'duration_in_minutes'
        ,'total_cases_produced_for_day'
        ,'total_cases_shipped_for_day'
        ,'location_avg_time', 'location_median_time', 'location_std_time'
        ,'cases_ratio'
    ]

    for prod_line in list_of_prod_lines:
        numerical_columns.append(f"line_{prod_line}")
    
    for item_type in list_of_item_types:
        numerical_columns.append(f"item_type_{item_type}")
       
    # Select the columns that exist in the dataframe
    available_columns = [col for col in numerical_columns if col in df_enhanced.columns]
    
    df_enhanced = df_enhanced[available_columns]
    #return df_enhanced[numerical_columns]
    return df_enhanced

# TESTING
print(prepare_numerical_matrix(create_enhanced_dataset()))

