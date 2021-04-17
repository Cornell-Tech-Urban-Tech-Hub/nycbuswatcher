# NYC MTA BusTime Scraper
This is a tagline.

#### v1.2 2021 Apr 19
Anthony Townsend <atownsend@cornell.edu>

## database migration plan

1. stop the stack

    ```bash
    cd nycbuswatcher
    docker-compose down
    ```

1. dump the db to a backup

    ```bash
    mysqldump buses buses | gzip -c > "buses.through.$(date +"%Y_%m_%d_%I_%M_%p").sql.gz"
    ```
    
2. move it somewhere safe

2. add 4 new columns to table

    ```sql
    ALTER TABLE buses ADD next_stop_id varchar(63) ;
    ALTER TABLE buses ADD next_stop_eta varchar(63) ;
    ALTER TABLE buses ADD next_stop_d_along_route float ;
    ALTER TABLE buses ADD next_stop_d float ;
    ```

5. pull the newest code and deploy it

    ```bash
    git pull
    git checkout new_dashboard
    docker-compose up -d --build
    ```
6. debugging possibilities

    - need to delete the static volume?
    
        ```bash
        docker volume rm buswatcher_1_bus_static
        ```



## function

Fetches list of active routes from MTA BusTime OneBusAway API via asynchronous http requests, then cycles through and fetches current vehicle positions for all buses operating on these routes. This avoids the poor performance of trying to grab the entire system feed from the MTA BusTime SIRI API. Dumps full API response (for later reprocessing to extract additional data) to compressed individual files and most of the vehicle status fields to mysql table (the upcoming stop data is omitted from the database dump for now). Fully dockerized, runs on scheduler 1x per minute. Data storage requirments ~ 1-2 Gb/day (guesstimate).


## installation 

### (docker)

1. clone the repo

    `git clone https://github.com/anthonymobile/nycbuswatcher.git`
    
2. obtain API keys and put them in .env (quotes not needed apparently)
    - http://bustime.mta.info/wiki/Developers/Index/
    - MapBox

    ```txt
    API_KEY = fasjhfasfajskjrwer242jk424242
    MAPBOX_API_KEY = pk.ey42424fasjhfasfajskjrwer242jk424242
    ```
    
3. build and run the images

    ```
    cd nycbuswatcher
    docker-compose up -d --build
    ```

### (manual)

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

# usage 

## 1. localhost production mode

if you just want to test out the grabber, you can run `export PYTHON_ENV=development; python grabber.py -l` and it will run once, dump the responses to a pile of files, and quit after throwing a database connection error. (or not, if you did step 3 in "manual" above). if you have a mysql database running it will operate in production mode locally until stopped.

## 2. docker stack

### grabber

1. get a shell on the container and run another instance of the script, it should run with the same environment as the docker entrypoint and will spit out any errors that process is having without having to hunt around through log files
    ```
    docker exec -it nycbuswatcher_grabber_1 /bin/bash
    python buswatcher.py
    ```
 

### mysql database

talking to a database inside a docker container is a little weird

1. *connect to mysql inside a container* to start a mysql client inside a mysql docker container

    ```
    docker exec -it nycbuswatcher_db_1 mysql -uroot -p buses
    [root password=bustime]
    ```
    
2. quick diagnostic query for how many records per day

    ```sql
   SELECT service_date, COUNT(*) FROM buses GROUP BY service_date;
    ```
    
3. query # of records by date/hour/minute

    ```sql
     SELECT service_date, date_format(timestamp,'%Y-%m-%d %H-%i'), COUNT(*) \
     FROM buses GROUP BY service_date, date_format(timestamp,'%Y-%m-%d %H-%i');
    ```

## 3. API

The API returns a selected set of fields for all positions during a time interval specific using ISO 8601 format for a single route at a time. e.g.

Required arguments: `output, route_short, start, end`
Output must be `geojson` for now, other formats may be supported in the future. Also try to limit to one hour of data per request.

```json
http://127.0.0.:5000/api/v1/nyc/buses?output=json&route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-03-28T01:00:00+00:00
```



# master to-do list
Can draw on these for our project steps as we have time/interest/relevance.

1. **database optimization.** Field types, add indices in the ORM model or on the server? Query optimization, etc.
1. **Batch processor for archives.** Script or switch that can unzip/tar and parse JSON API responses through parser, db_dump.
3. **Replace flask frontend.** Rebuild entire front end as a Gatsby app (using the [gatsby-starter-mapbox](https://github.com/anthonymobile/gatsby-starter-mapbox) and [gatsby-start-mapbox-examples](https://github.com/astridx/gatsby-starter-mapbox-examples) templates).
4. **Parser extension.** Ad parsing for the MonitoredCall portion of API response for each bus (currently skipped).
5. **New parser/parser plug-in for SIRIStopMonitoring API.** [SIRIStopMonitoring](http://bustime.mta.info/wiki/Developers/SIRIStopMonitoring) reports info on individual stops, 1 at a time only.
6. **OneBusAway API parser.** Route geometry from [OneBusAway API](http://bustime.mta.info/wiki/Developers/OneBusAwayRESTfulAPI) may be easier than working with the GTFS:
            - Full information about each stop covered by MTA Bus Time (e.g. the lat/lon coordinates, stop name, list of routes serving that stop)
            - The stops served by a given route
            - The physical geometry for a given route (for mapping and geographic calculations) **MTA endpoint appears to be inoperative**
            - The schedule of trips serving a given stop or route (repeat: schedule, having nothing to do with the real-time data)
            - The stops or routes near a given location
7. **GTFS Integration.** [Read this first](https://medium.com/analytics-vidhya/the-hitchhikers-guide-to-gtfs-with-python-e9790090952a)
