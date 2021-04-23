
# future work

## 1. branch: fast_api
1. test and debug
2. merge back into dashboard_new branch

## 2. branch: new_dashboard
1. finish TODOs and test locally
2. do alembic migration
3. debug docker stack
4. push deploy commit to github
5. deploy to cornell server for testing on this branch
6. merge `new_dashboard` branch to `master` and redeploy to server


### reprocessor
1. start protoyping in a notebook
    - just use a copy of the database code for now
    - dynamically create the `Table` class with `type()` like here on [StackOverflow](https://stackoverflow.com/questions/973481/dynamic-table-creation-and-orm-mapping-in-sqlalchemy)

        ```python
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import Column, Integer, String

        Base = declarative_base()
   
        from datetime import datetime

        tablename = 'buses' + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")

        attribs = {
            '__tablename__': tablename,
            'test_id': Column(Integer, primary_key=True, autoincrement=True),
            'fldA': Column(String),  
            ... other columns
            }
        
        Test = type('Test', (Base,), attribs )
        
        Base.metadata.create_all(engine)
        
        #  passed session create with sqlalchemy
        session.query(Test).all() 
        ```
    - then map the `BusOservation` object model to this table?
3. basic requirements
    - can crawl everything in the current folder
    - can take a date range and just look for those files
    - can open and unzip the files
    - can pass the JSON responses through the same parser/duplicate parser
    - can connect to two SQlalchemy tables (`buses` and the temp one it wants to write, unless dry-)
    - Should compare existing records to ones it wants to add or update
    - two levels of failsafe
        - has a dry-run switch (by default) that says what it is going to write
        - has a real-update switch (off by default) that makes it update the master db 
    - can use the same db_dump or equivalent
    - never overwrites anything that contains data (only inserts records and updates empty/null/0 fields)
    
3. from prototying to development
    - separate script—just implement the prototype notebook as a separate script, copy the code and hope we dont need to update it later
    - refactor-finish refactoring `grabber.py` and `Database.py`?

4. from development to production
-    test first on a local server with a copy of the master db, dropping all but a month's worth or records



# NYC MTA BusTime Scraper
- v1.2 2021 Apr 19
- Anthony Townsend <atownsend@cornell.edu>

## description

Fetches list of active routes from MTA BusTime OneBusAway API via asynchronous http requests, then cycles through and fetches current vehicle positions for all buses operating on these routes. This avoids the poor performance of trying to grab the entire system feed from the MTA BusTime SIRI API. Dumps full API response (for later reprocessing to extract additional data) to compressed individual files and most of the vehicle status fields to mysql table (the upcoming stop data is omitted from the database dump for now). Fully dockerized, runs on scheduler 1x per minute. Data storage requirments ~ 1-2 Gb/day (guesstimate).


## installation 

#### with docker-compose

1. clone the repo

    `git clone https://github.com/anthonymobile/nycbuswatcher.git`
    
2. obtain API keys and put them in .env (quotes not needed apparently)
    - http://bustime.mta.info/wiki/Developers/Index/

    ```txt
    API_KEY = fasjhfasfajskjrwer242jk424242
    ```
    
3. build and run the images

    ```
    cd nycbuswatcher
    docker-compose up -d --build
    ```

#### manual installation

1. clone the repo

    `git clone https://github.com/anthonymobile/nycbuswatcher.git`
    
2. obtain an API key from http://bustime.mta.info/wiki/Developers/Index/ and put it in .env

    `echo 'API_KEY = fasjhfasfajskjrwer242jk424242' > .env`
    
3. create the database (mysql only, 5.7 recommended)
    ```sql
    CREATE DATABASE buses;
    USE buses;
    CREATE USER 'nycbuswatcher'@'localhost' IDENTIFIED BY 'bustime';
    GRANT ALL PRIVILEGES ON * . * TO 'nycbuswatcher'@'localhost';
    FLUSH PRIVILEGES;
 
    ```
3. run
    ```python
    python grabber.py # development: run once and quit
    python grabber.py -p # production: runs in infinite loop at set interval using scheduler (hardcoded for now)
    ```

## usage 

#### 1. localhost production mode

if you just want to test out the grabber, you can run `export PYTHON_ENV=development; python grabber.py -l` and it will run once, dump the responses to a pile of files, and quit after throwing a database connection error. (or not, if you did step 3 in "manual" above). if you have a mysql database running it will operate in production mode locally until stopped.

#### 2. docker stack


###### dash
- Dash app running the front end.

####### api v2 (april 2021)
- FastAPI app providing the API endpoints:
    - `/api/v1/nyc/livemap` Selected fields for buses seen in the last 60 seconds.
    - `/api/v1/nyc/buses?` Returns a selected set of fields for all positions during a time interval specific using ISO 8601 format for a single route at a time.
    - Required:
        - `output=geojson`
        - `route_short` e.g. `Bx4`
        - `start`
        - `end` in ISO8601, max 1 hour. e.g.
    - example: 
        ```json
        http://nyc.buswatcher.org/api/v1/nyc/buses?output=geojson&route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-03-28T01:00:00+00:00
        ```
- Swagger doc endpoint `http://127.0.0.1:8000/docs`
- ReDoc doc endpoint `http://127.0.0.1:8000/redoc`

###### grabber
- Daemon that uses apscheduler to trigger a set of asynchronous API requests to get each route's current bus locations, dump the responses to archive files, parse the response and dump that to the database. For debugging, its possible to get a shell on the container and run another instance of the script, it should run with the same environment as the docker entrypoint and will spit out any errors that process is having without having to hunt around through log files.

    ```
    docker exec -it nycbuswatcher_grabber_1 /bin/bash
    python buswatcher.py
    ```

###### db
- For debugging, run the mysql client directly inside the container.
    
    ```
    docker exec -it nycbuswatcher_db_1 mysql -uroot -p buses
    [root password=bustime]
    ```
    
- quick diagnostic query for how many records per day

    ```sql
    SELECT service_date, COUNT(*) FROM buses GROUP BY service_date;
    ```

- query # of records by date/hour/minute

    ```sql
     SELECT service_date, date_format(timestamp,'%Y-%m-%d %H-%i'), COUNT(*) \
     FROM buses GROUP BY service_date, date_format(timestamp,'%Y-%m-%d %H-%i');
    ```
