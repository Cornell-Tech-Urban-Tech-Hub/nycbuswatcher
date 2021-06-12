FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

# RUN apt-get update -y; apt-get upgrade -y; apt-get install -y default-libmysqlclient-dev python-dev gcc python3-pip
RUN apt-get update -y; apt-get upgrade -y; apt-get install -y python3-pip

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY ./build/api/requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


ENV PYTHON_ENV="production"

# copy repo
WORKDIR /app

COPY . .
