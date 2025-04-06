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
    
    # Create line and item type dummy variables
    for line in ['LOU1', 'LOU2', 'LOU3', 'LOU4', 'LOU5']:
        df_enriched[f'line_{line}'] = (df_enriched['line'] == line).astype(int)
        
    for item_type in ['SHRF', 'LABL']:
        df_enriched[f'item_type_{item_type}'] = (df_enriched['item_type'] == item_type).astype(int)
    
    # # Add storage location type
    # df_enriched['storage_location_type'] = df_enriched['from_location'].str[4:7]
    
    # for storage_type in ['RSR', 'SSR']:
    #     df_enriched[f'storage_location_type_{storage_type}'] = (df_enriched['storage_location_type'] == storage_type).astype(int)
    
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
    
    # # Add cross features
    # df_enriched['rsr_with_shrf'] = ((df_enriched['storage_location_type'] == 'RSR') & 
    #                                 (df_enriched['item_type'] == 'SHRF')).astype(int)
    # df_enriched['ssr_with_labl'] = ((df_enriched['storage_location_type'] == 'SSR') & 
    #                                (df_enriched['item_type'] == 'LABL')).astype(int)
    
    # # Add time-based features
    # df_enriched['hour_of_day'] = df_enriched['insert_dttm'].dt.hour
    # df_enriched['day_of_week'] = df_enriched['insert_dttm'].dt.dayofweek
    
    # # Create dummy variables for hours and days
    # for hour in range(1, 24):
    #     df_enriched[f'hour_{hour}'] = (df_enriched['hour_of_day'] == hour).astype(int)
        
    # for day in range(1, 7):
    #     df_enriched[f'day_{day}'] = (df_enriched['day_of_week'] == day).astype(int)
    
    # # Add business hours and shift indicators
    # df_enriched['business_hours'] = ((df_enriched['hour_of_day'] >= 8) & 
    #                                (df_enriched['hour_of_day'] < 17) &
    #                                (df_enriched['day_of_week'] < 5)).astype(int)
    
    # df_enriched['night_shift'] = ((df_enriched['hour_of_day'] >= 22) | 
    #                              (df_enriched['hour_of_day'] < 6)).astype(int)
    
    # df_enriched['weekend'] = (df_enriched['day_of_week'] >= 5).astype(int)
    
    # Add cases ratio
    df_enriched['cases_ratio'] = df_enriched['total_cases_produced'] / (df_enriched['total_cases_shipped'] + 1e-10)
    df_enriched['cases_ratio'] = df_enriched['cases_ratio'].clip(0, 10)
    
    # # Add time + item type interactions
    # df_enriched['morning_shrf'] = ((df_enriched['hour_of_day'] < 12) & 
    #                              (df_enriched['item_type'] == 'SHRF')).astype(int)
    # df_enriched['evening_labl'] = ((df_enriched['hour_of_day'] >= 17) & 
    #                              (df_enriched['item_type'] == 'LABL')).astype(int)
    
    return df_enriched

def prepare_numerical_matrix(df_enriched):
    """Create the numerical matrix for model training"""
    numerical_columns = [
        'duration_in_minutes', 
        'total_cases_produced', 
        'total_cases_shipped',
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

