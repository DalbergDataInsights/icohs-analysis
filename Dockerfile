FROM ubuntu:latest as os

#Need to remove the upgrade command, can add it back ater

FROM os as upgrade

RUN apt-get update && \ 
    apt-get upgrade -y && \
    apt-get install -y software-properties-common

FROM upgrade as python

RUN apt-get install -y python3.9 &&\
    apt-get install -y python3-pip

FROM python as app

COPY . /app

FROM app as environment

WORKDIR /app
RUN pip3 install -r requirements.txt
RUN pip3 install python-dateutil


