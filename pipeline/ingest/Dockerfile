FROM python:3.13.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

CMD ["python", "ingest.py"]
