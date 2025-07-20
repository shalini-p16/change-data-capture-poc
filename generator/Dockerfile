FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary faker

COPY data-generator.py .

ENTRYPOINT ["python", "data-generator.py"]
