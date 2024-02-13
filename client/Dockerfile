FROM python:3.10-slim-buster

RUN mkdir /app
COPY requirements.txt ./app

WORKDIR /app
RUN pip install -r requirements.txt

COPY . ./app

ENTRYPOINT [ "python", "client.py" ]