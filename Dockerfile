FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy packaging metadata + source, then install the package
COPY pyproject.toml .
COPY README.md .
COPY src/ src/
RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "etl", "run"]
