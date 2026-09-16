FROM python:3.11-slim
WORKDIR /app
COPY common/ ./common/
COPY budget-service/app.py ./
ENV PYTHONUNBUFFERED=1 PORT=8002 DB_PATH=/data/budget.db
EXPOSE 8002
CMD ["python", "app.py"]
