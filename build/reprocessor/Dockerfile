FROM python:3.9

# RUN apt-get update -y; apt-get upgrade -y; apt-get install -y default-libmysqlclient-dev python-dev gcc python3-pip
RUN apt-get update -y; apt-get upgrade -y; apt-get install -y python3-pip default-libmysqlclient-dev

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY ./build/reprocessor/requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV PYTHON_ENV="production"

# copy repo
WORKDIR /app
COPY . .

# keep the container alive to await interactive login
# CMD tail -f /dev/null