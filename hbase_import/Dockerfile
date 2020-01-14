FROM python:3.8.0-buster

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ["converters.py", "main.py" , "./"]
COPY "config.py" "./"

CMD [ "python3", "-u", "main.py" ]
