FROM ubuntu:latest as os

#Need to remove the upgrade command, can add it back ater

RUN apt-get update && \ 
    apt-get upgrade -y && \
    apt-get install -y software-properties-common &&\
    apt-get install -y python3.9 &&\
    apt-get install -y pipenv

FROM os as app

COPY . /app

FROM app as environment

WORKDIR /app
RUN pipenv install
RUN pipenv shell
RUN pip install python-dateutil

FROM environment as run

RUN crontab -e

