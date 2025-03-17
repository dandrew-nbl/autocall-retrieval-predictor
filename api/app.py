from flask import Flask, request, jsonify, render_template
import pandas as pd
import sys
import os
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.predict import predict_retrieval_times_monte_carlo, forecast_retrieval_risks

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    """Endpoint for individual predictions"""
    data = request.json
    
    try:
        result = predict_retrieval_times_monte_carlo(
            item_type=data.get('item_type', 'SHRF'),
            production_line=data.get('production_line', 'LOU2'),
            cases_produced=data.get('cases_produced', 85000),
            cases_shipped=data.get('cases_shipped', 170000),
            day_of_week=data.get('day_of_week', 0),
            hour_of_day=data.get('hour_of_day', 10),
            n_simulations=data.get('n_simulations', 500)
        )
        
        # Remove raw predictions from response to reduce size
        if 'raw_predictions' in result:
            result.pop('raw_predictions')
            
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/forecast', methods=['POST'])
def forecast():
    """Endpoint for daily forecast"""
    data = request.json
    
    try:
        risk_df = forecast_retrieval_risks(
            day_of_week=data.get('day_of_week', 0),
            shipping_forecast=data.get('shipping_forecast', 170000),
            production_forecast=data.get('production_forecast', 85000),
            lines=data.get('lines', None),
            materials=data.get('materials', None),
            hours=data.get('hours', None)
        )
        
        return jsonify(risk_df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    app.run(host='0.0.0.0', port=5000, debug=True)
