
FROM python:3.8-buster

RUN mkdir /app
WORKDIR /app
    
COPY requirements.txt requirements.txt
RUN apt-get update \
    && apt-get install -y libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit \
    && pip3 install -r requirements.txt

COPY run_driver.sh run_driver.sh
RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /wait-for-it.sh \
    && chmod u+x /wait-for-it.sh \
    && chmod u+x ./run_driver.sh

COPY src .
COPY hive ./hive
ENV DRIVER_MODE ""
CMD ["./run_driver.sh"]
