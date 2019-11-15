FROM python:3.7-alpine

COPY . /app

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apk update && \
    apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add postgresql-dev

RUN pip install -r requirements.txt


