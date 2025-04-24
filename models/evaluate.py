import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
data_dir = os.path.join(project_root, "data")
sys.path.append(data_dir)

from data.preprocessing import create_enriched_dataset, prepare_numerical_matrix
from models.train import prepare_X

def evaluate_model():
    """Evaluate the trained model on test data"""
    # Load model and feature names
    model = joblib.load('rf_model.joblib')
    feature_names = joblib.load('feature_names.joblib')
    
    # Get and prepare data
    df_enriched = create_enriched_dataset()
    df_numerical = prepare_numerical_matrix(df_enriched)
    
    # Split data into train and test sets
    from sklearn.model_selection import train_test_split
    train_data, test_data = train_test_split(df_numerical, test_size=0.2, random_state=42)
    
    # Prepare test features
    X_test, _ = prepare_X(test_data)
    y_test = np.log10(test_data['duration_in_minutes'].values)
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"Test RMSE (log10 scale): {rmse:.4f}")
    print(f"Test MAE (log10 scale): {mae:.4f}")
    print(f"Test R² Score: {r2:.4f}")
    
    # Convert to original scale
    y_test_orig = 10**y_test
    y_pred_orig = 10**y_pred
    
    rmse_orig = np.sqrt(mean_squared_error(y_test_orig, y_pred_orig))
    mae_orig = mean_absolute_error(y_test_orig, y_pred_orig)
    
    print(f"Test RMSE (minutes): {rmse_orig:.4f}")
    print(f"Test MAE (minutes): {mae_orig:.4f}")
    
    # Plot actual vs predicted
    plt.figure(figsize=(10, 6))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
    plt.xlabel('Actual log10(duration)')
    plt.ylabel('Predicted log10(duration)')
    plt.title('Actual vs Predicted (log scale)')
    plt.savefig('evaluation_plot.png')
    
    # Plot feature importance
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    
    plt.figure(figsize=(12, 8))
    plt.title('Feature Importances')
    plt.bar(range(len(indices[:20])), importances[indices[:20]], align='center')
    plt.xticks(range(len(indices[:20])), [feature_names[i] for i in indices[:20]], rotation=90)
    plt.tight_layout()
    plt.savefig('feature_importance.png')
    
    return {
        'rmse': rmse,
        'mae': mae,
        'r2': r2,
        'rmse_orig': rmse_orig,
        'mae_orig': mae_orig
    }

if __name__ == "__main__":
    evaluate_model()
