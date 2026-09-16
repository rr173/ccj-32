FROM python:3.11-slim
WORKDIR /app
COPY common/ ./common/
COPY registry-service/app.py ./
ENV PYTHONUNBUFFERED=1 PORT=8001 DB_PATH=/data/registry.db
EXPOSE 8001
CMD ["python", "app.py"]
