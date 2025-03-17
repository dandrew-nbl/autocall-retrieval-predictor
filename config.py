"""
Configuration settings for the application
"""

import os

# Database settings
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'warehouse_db')
DB_USER = os.environ.get('DB_USER', 'user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')

# Model settings
MODEL_PATH = os.environ.get('MODEL_PATH', 'models/rf_model.joblib')
FEATURE_NAMES_PATH = os.environ.get('FEATURE_NAMES_PATH', 'models/feature_names.joblib')

# API settings
API_HOST = os.environ.get('API_HOST', '0.0.0.0')
API_PORT = int(os.environ.get('API_PORT', '5000'))
DEBUG = os.environ.get('DEBUG', 'True').lower() in ('true', '1', 't')

# Monte Carlo simulation settings
DEFAULT_SIMULATIONS = int(os.environ.get('DEFAULT_SIMULATIONS', '500'))
