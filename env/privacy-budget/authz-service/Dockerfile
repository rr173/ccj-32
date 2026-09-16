FROM python:3.11-slim
WORKDIR /app
COPY common/ ./common/
COPY authz-service/app.py ./
ENV PYTHONUNBUFFERED=1 PORT=8003 DB_PATH=/data/authz.db
EXPOSE 8003
CMD ["python", "app.py"]
