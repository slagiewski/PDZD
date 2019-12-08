FROM python:3.8.0-buster

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ["config.py", "converters.py", "hbase_import.py" , "./"]

ENTRYPOINT bash