FROM paperflow-article-repository-repository
RUN pip install --no-cache-dir minio==7.2.4
COPY library.py /app/paperflow_qdrant/library.py
COPY catalog.py sync.py /app/article_repository/
COPY legacy_migration.py migrate.py nas_transfer.py attachment_notes.py repair_attachments.py /app/migration/
ENV PYTHONPATH=/app:/app/migration HAYSTACK_TELEMETRY_ENABLED=false PYTHONUNBUFFERED=1
ENTRYPOINT ["python","/app/migration/migrate.py"]
