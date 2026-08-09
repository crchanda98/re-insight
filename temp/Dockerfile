# Use a clean, stable Python environment
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies if any library requires C-compilation pooling
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker caching layers
COPY requirements.txt .

# Upgrade pip and install all data science and streaming packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir psycopg2-binary && \
    pip install flask-limiter

# Copy the rest of your application code into the container
COPY . .

# Expose the default Streamlit port
EXPOSE 8501

# Run Streamlit and bind it to all network interfaces
CMD ["streamlit", "run", "dashboard.py", "--server.port=8502", "--server.address=0.0.0.0"]