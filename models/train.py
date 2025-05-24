import sys
import os
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
data_dir = os.path.join(project_root, "data")
sys.path.append(data_dir)

from data.preprocessing import create_enriched_dataset, prepare_numerical_matrix

def prepare_X(df):
    """Prepare feature matrix for model training"""
    df = df.copy()
    
    # Remove target variable if it exists
    if 'duration_in_minutes' in df.columns:
        df = df.drop('duration_in_minutes', axis=1)

    if 'log_duration' in df.columns:
        df = df.drop('log_duration', axis=1)
        
    feature_names = list(df.columns)
    X = df.values
    return X, feature_names

def train_model():
    """Train the Random Forest model"""
    # Get and prepare data
    df_enriched = create_enriched_dataset()
    df_numerical, ignore, ignore  = prepare_numerical_matrix(df_enriched)
    
    # Take log of duration
    df_numerical['log_duration'] = np.log10(df_numerical['duration_in_minutes'])
    
    # Split data
    train_idx, test_idx = train_test_split(
        np.arange(len(df_numerical)), 
        test_size=0.2, 
        random_state=42
    )
    
    df_train = df_numerical.iloc[train_idx].copy()
    df_test = df_numerical.iloc[test_idx].copy()
    
    # Prepare feature matrices
    X_train, feature_names = prepare_X(df_train)
    y_train = np.log10(df_train['duration_in_minutes'].values)
    
    X_test, _ = prepare_X(df_test)
    y_test = np.log10(df_test['duration_in_minutes'].values)
    
    # Train model with best parameters from grid search
    best_params = {
        'n_estimators': 200,
        'max_depth': 30,
        'min_samples_split': 2,
        'min_samples_leaf': 1
    }
    
    model = RandomForestRegressor(**best_params, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_rmse = np.sqrt(mean_squared_error(y_train, train_pred))
    test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))
    
    print(f"Training RMSE: {train_rmse:.4f}")
    print(f"Test RMSE: {test_rmse:.4f}")
    
    # Save model and feature names
    joblib.dump(model, 'rf_model.joblib')
    joblib.dump(feature_names, 'feature_names.joblib')
    
    return model, feature_names

if __name__ == "__main__":
    train_model()
