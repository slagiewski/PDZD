FROM python:3.8-buster

RUN mkdir /app
WORKDIR /app
COPY requirements.txt requirements.txt
RUN apt-get update \
    && apt-get install -y libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit \
    && pip3 install -r requirements.txt

COPY src .
COPY hive ./hive
CMD ["python", "-u", "/app/main.py"]
