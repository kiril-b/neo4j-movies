FROM python:3.12.3-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY preprocess_data.py /app/

ENTRYPOINT [ "python", "-u", "preprocess_data.py" ]