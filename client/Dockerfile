FROM python:3.10-slim-buster

COPY . ./app
COPY requirements.txt ./app

WORKDIR /app

RUN pip install -r requirements.txt

CMD [ "python", "client.py" ]
