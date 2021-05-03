# future could use a smaller python 3.7 image?
FROM tiangolo/uwsgi-nginx-flask:python3.7

# RUN apt-get update -y; apt-get upgrade -y; apt-get install -y default-libmysqlclient-dev python-dev gcc python3-pip
RUN apt-get update -y; apt-get upgrade -y; apt-get install -y python3-pip

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY ./build/acquire/requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


ENV PYTHON_ENV="production"

# copy repo
WORKDIR /app
COPY . .