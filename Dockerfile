FROM python:3.11-slim

# Make logs show up immediately in Cloud Run logs
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your package source
COPY src/ src/

# If your ETL requires a data file at runtime, decide later whether to COPY it.
# In production it's usually better to read input from GCS instead of baking it into the image.
# COPY data/ data/

CMD ["python", "-m", "etl", "run"]
