# NYCBusWatcher
- v2.0 June 2021
- Anthony Townsend <atownsend@cornell.edu>

## How it works

`acquire.py` runs as an `apscheduler` daemon. Once per minute  function `async_grab_and_store` grabs 200+ JSON responses from the MTA and uses them to initialize two class instances, one `DataLake` and one `DataStore`. The first simply dumps the files to a directory structure, the second parses the JSON into `BusObservation` object instances and pickles them to disk in a similar directory structure.

- `/data/lake/puddles/YYYY/MM/DD/HH/RouteID/drop_xxx.json`
- `/data/store/barrels/YYYY/MM/DD/HH/RouteID/barrel_xxx.dat`

Once per hour, another `apscheduler` job searches these trees for any of these folders that have not been processed, and are not currently in use (e.g. in the current hour). They are then either archived into a `.tar.gz` (for the JSON) or loaded and concatenated and serialized as a JSON file (in the case of the pickles), which is what will be served as static files by  web API in response to client requests.




#333333333333333333333333333333
## v2 design vision

### Goals:
- provide single robust, low cost point of data access to processed SIRI API data for students
- Provide ongoing archival of feed JSON

### Data Acquisition + Storage

A slightly modified version of v1.2 `acquire.py` runs the following scheduled jobs:

##### Per minute:
1. Fetches list of active routes from MTA BusTime OneBusAway API
2. Retrieves SIRI `VehicleMonitoring` response for each route asynchronously. (This avoids the poor performance of trying to grab the entire system feed from the MTA BusTime SIRI API.)
3. `BusObservations` are parsed and pickled to disk (in one file for all routes per scheduled run, e.g. `BusObservations_2021-05-02T12:00:11.2342.pickle`).
4. API JSON responses are saved directly to disk without parsing (one file per route).

##### Per hour:
5. Per route, all pickle files for the hour are loaded, concatenated, and serialized into a single static JSON file which is served up by app.py FastAPI app with an endpoint like:
    `http://api.buswatcher.org/2021/05/11/09/M15`
   Optionally, export a single file for all routes at `http://api.buswatcher.org/2021/05/11/09/all`
6. Available routes for an hourly interval are discoverable at:
   `http://api.buswatcher.org/2021/05/11/09/routes`
7. The individual JSON responses (n routes * ~60 grabs) are bundled together into a compressed tarball and put in cold storage. Data storage requirements ~ 1-2 Gb/day (guesstimate).

### pros
- Reuses existing code
- Uses existing docker stack
- No database
### cons
- All API queries need to be hardcoded/built statically
- need to export entire database to static files
- storage requirements, but can integrate with S3

--------------
# REVISE EVERYTHING BELOW HERE FOR V2
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
    python acquire.py # development: run once and quit
    python acquire.py -p # production: runs in infinite loop at set interval using scheduler (hardcoded for now)
    ```

## usage 

#### 1. localhost production mode

if you just want to test out the grabber, you can run `export PYTHON_ENV=development; python grabber.py -l` and it will run once, dump the responses to a pile of files, and quit after throwing a database connection error. (or not, if you did step 3 in "manual" above). if you have a mysql database running it will operate in production mode locally until stopped.

#### 2. docker stack


###### app
- Dash app running the front end.

####### api v2 (april 2021)
- FastAPI app providing the API endpoints:
    - `/api/v1/nyc/livemap` Selected fields for buses seen in the last 60 seconds.
    - `/api/v1/nyc/buses?` Returns a selected set of fields for all positions during a time interval specific using ISO 8601 format for a single route at a time.
    - Required:
        - `output=geojson`
        - `route_short` e.g. `Bx4`
        - `start`
        - `end` in ISO8601, max 24 hours. e.g.
    - example: 
        ```json
        http://nyc.buswatcher.org/api/v1/nyc/buses?output=geojson&route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-03-28T01:00:00+00:00
        ```
- Swagger doc endpoint `http://127.0.0.1:8000/docs`
- ReDoc doc endpoint `http://127.0.0.1:8000/redoc`

###### acquire
- Daemon that uses apscheduler to trigger a set of asynchronous API requests to get each route's current bus locations, dump the responses to archive files, parse the response and dump that to the database. For debugging, its possible to get a shell on the container and run another instance of the script, it should run with the same environment as the docker entrypoint and will spit out any errors that process is having without having to hunt around through log files.

    ```
    docker exec -it nycbuswatcher_grabber_1 /bin/bash
    python buswatcher.py
    ```
