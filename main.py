"""
Entry point for the application
"""

import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import API app 
from api.app import app
from models.train import train_model
from models.evaluate import evaluate_model

def setup():
    """Set up the application"""
    print("Setting up the application...")
    
    # Check if model exists
    if not os.path.exists('models/rf_model.joblib'):
        print("Training model...")
        train_model()
        
        print("Evaluating model...")
        evaluate_model()
    
    print("Setup complete.")

if __name__ == "__main__":
    # Run setup
    setup()
    
    # Start the Flask application
    app.run(host='0.0.0.0', port=5000)
