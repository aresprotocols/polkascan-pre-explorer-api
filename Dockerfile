# base image
FROM python:3.8-buster
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get -y install default-mysql-client

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN pip3 install --upgrade pip

# add requirements
COPY ./requirements.txt /usr/src/app/requirements.txt

# install requirements
RUN pip3 install -r requirements.txt

# add app
COPY . /usr/src/app
