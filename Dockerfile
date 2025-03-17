FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directories for model artifacts
RUN mkdir -p models
RUN mkdir -p logs
RUN mkdir -p api/templates

# Create a simple index.html template
RUN mkdir -p api/templates && echo '<!DOCTYPE html>\n\
<html>\n\
<head>\n\
    <title>Retrieval Time Predictor</title>\n\
    <style>\n\
        body { font-family: Arial, sans-serif; margin: 40px; }\n\
        h1 { color: #333; }\n\
        .container { max-width: 800px; margin: 0 auto; }\n\
    </style>\n\
</head>\n\
<body>\n\
    <div class="container">\n\
        <h1>Warehouse Retrieval Time Predictor</h1>\n\
        <p>Welcome to the Retrieval Time Prediction API.</p>\n\
        <p>Use the following endpoints:</p>\n\
        <ul>\n\
            <li><strong>/predict</strong> - For individual retrieval time predictions</li>\n\
            <li><strong>/forecast</strong> - For daily retrieval risk forecasts</li>\n\
        </ul>\n\
    </div>\n\
</body>\n\
</html>' > api/templates/index.html

# Train the model if it doesn't exist
RUN mkdir -p models

EXPOSE 5000

CMD ["python", "api/app.py"]
