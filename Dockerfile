FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./optimizer_service /app/optimizer_service

CMD ["uvicorn", "optimizer_service.main:app", "--host", "0.0.0.0", "--port", "8000"]