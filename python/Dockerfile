FROM python:3.12-slim
RUN pip install --no-cache-dir kafka-python==2.3.0
COPY kafka_bridge.py /app/kafka_bridge.py
ENTRYPOINT ["python", "-u", "/app/kafka_bridge.py"]
