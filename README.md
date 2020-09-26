# NYC MTA BusTime Scraper
#### v1.0 2020 Sept 23
Anthony Townsend <atownsend@cornell.edu>

# function

Fetches list of active routes from OneBusAway API via asynchronous http requests, then cycles through and fetches current vehicle positions for all buses operating on these routes. This avoids the poor performance of trying to grab the entire system feed from the BusTime API. Dumps full API response (for later reprocessing to extract additional data) to compressed individual files and most of the vehicle status fields to mysql table (the upcoming stop data is omitted from the database dump for now). Fully dockerized, runs on scheduler 1x per minute. Data storage requirments ~ 1-2 Gb/day (guesstimate).


# install (docker)

1. clone the repo

    `git clone https://github.com/anthonymobile/nycbuswatcher.git`
    
2. obtain an API key from http://bustime.mta.info/wiki/Developers/Index/ and put it in .env

    `echo 'API_KEY = fasjhfasfajskjrwer242jk424242' > .env`
    
3. build and run the images

    `docker-compose up -d --build`


# install (manual)

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


# future development
Feel free to jump in and submit some pull requests. 

## big
- front end / web dashboard
    - geometry: OneBusAway API ( don't handle/store the GTFS locally)
    - data: our internal API, historical location + arrival
    - renderer: kepler.js or dash/plotly

## medium
- additional data scrapers
    - add parsing for the MonitoredCall portion of API response for each bus (currently discarded)
    - stop monitoring—[SIRIStopMonitoring](http://bustime.mta.info/wiki/Developers/SIRIStopMonitoring) reports info on individual stops, 1 at a time only.
    - route geometry from [OneBusAway API](http://bustime.mta.info/wiki/Developers/OneBusAwayRESTfulAPI) (much easier than working with the GTFS) on:
        - Full information about each stop covered by MTA Bus Time (e.g. the lat/lon coordinates, stop name, list of routes serving that stop)
        - The stops served by a given route
        - The physical geometry for a given route (for mapping and geographic calculations)
        - The schedule of trips serving a given stop or route (repeat: schedule, having nothing to do with the real-time data)
        - The stops or routes near a given location

## little
- bundle and archive the compressed files in /data for cold storage (to avoid future issues on huge # of files)
- add ability to (batch) re-process archived files through parser, db_dump

## not sure
- integrate/automate push daily CSV to qri.io