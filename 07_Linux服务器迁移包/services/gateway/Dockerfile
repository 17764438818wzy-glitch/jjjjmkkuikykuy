FROM python:3.12-slim
WORKDIR /app
COPY requirements-qdrant.txt .
RUN pip install --no-cache-dir -r requirements-qdrant.txt
COPY paperflow_qdrant ./paperflow_qdrant
ENV HAYSTACK_TELEMETRY_ENABLED=false PYTHONUNBUFFERED=1
CMD ["uvicorn","paperflow_qdrant.server:app_factory","--factory","--host","0.0.0.0","--port","8097","--no-access-log"]
