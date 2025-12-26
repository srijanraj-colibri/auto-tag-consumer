FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer consumer
COPY workers workers
COPY services services  
COPY core core

CMD ["python", "-m", "consumer.main"]
