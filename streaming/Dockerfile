FROM python:3.11
WORKDIR /app
RUN pip install kafka-python
COPY consumer.py .
CMD ["python", "consumer.py"]
