FROM python:3.10-slim-buster

RUN mkdir /app
COPY requirements.txt ./app

RUN pip install -r requirements.txt

COPY . ./app
WORKDIR /app

ENTRYPOINT [ "python", "client.py" ]