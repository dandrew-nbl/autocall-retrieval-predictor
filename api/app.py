from flask import Flask, request, jsonify, render_template
import pandas as pd
import sys
import os
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.predict import forecast_daily_retrieval_times
from data.database import get_future_production_schedule
#from data.preprocessing import create_enriched_dataset, prepare_numerical_matrix

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/forecast_schedule', methods=['GET'])
def forecast_schedule():
    """Endpoint for forecasting based on future production schedule"""
    try:
        # Get production schedule from request
        production_data = get_future_production_schedule()
        
        # Convert to DataFrame
        production_df = pd.DataFrame(production_data)
        
        # Generate forecast
        daily_forecast, detailed_forecast = forecast_daily_retrieval_times(production_df)
        
        # Return results
        return jsonify({
            'daily_forecast': daily_forecast.to_dict(orient='records'),
            'detailed_forecast': detailed_forecast.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    app.run(host='0.0.0.0', port=5080, debug=True)
