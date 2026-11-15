FROM paperflow-qdrant-20261103-gateway
WORKDIR /app
COPY requirements.txt /app/article-requirements.txt
RUN pip install --no-cache-dir -r /app/article-requirements.txt
COPY article_repository /app/article_repository
COPY library.py /app/paperflow_qdrant/library.py
COPY frontend/dist /app/article-site
COPY majors.json legacy-records.json /app/
CMD ["uvicorn","article_repository.server:app_factory","--factory","--host","0.0.0.0","--port","8098","--no-access-log"]
