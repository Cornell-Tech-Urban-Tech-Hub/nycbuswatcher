# NYCBusWatcher
- v2.0 July 2021
- Anthony Townsend <atownsend@cornell.edu>


## Description

NYCBusWatcher is a fully-containerized set of Python scripts that fetches, parses, and redistributes bulk data of bus position records from the NYC Transit BusTime API. For speed and scalability, there is no database. Everything is done with serialized data stored in static files for speed, scalability, and economy.

## Quickstart

The easiesy way to use NYCBusWatcher is to simply pull data from our public API, powered by FastAPI. This API serves up batches of bus observations in either JSON or GeoJSON format, bundled in hourly increments per route (hereafter referred to as 'shipments'). This allows data users to quickly pull large amounts of data without the overhead of a database. Several APIs are provided for discovering what shipments are available (e.g. date coverage, and route coverage). Please suggest other APIs.

- [API home page]((https://api.buswatcher.org)), redirects to /docs for now.
- [API docs](https://api.buswatcher.org/docs), includes test capabilities.
- [Alt API docs (redoc)](https://api.buswatcher.org/redoc), easier reading.

## Run Your Own Service


1. Clone the repo.

    `git clone https://github.com/anthonymobile/nycbuswatcher.git
   && cd nycbuswatcher`
    
    
2. [Obtain a BusTime API key](http://bustime.mta.info/wiki/Developers/Index/) and put it in .env (quotes not needed, but no spaces)

    ```txt
    API_KEY=fasjhfasfajskjrwer242jk424242
    ```

3. If you want to use the gandi dyndns updater, add these three keys to .env and make sure to uncomment the appropriate section in `docker-compose.yml`
    - GANDI_API_KEY=rwer242jk424242
    - GANDI_DOMAIN=buswatcher.org
    - GANDI_SUBDOMAINS=api, www, thisistheapiforreal


4. Bring the stack up.

    ```
    export COMPOSE_PROJECT_NAME=nycbuswatcher2 # (optional, if running alongside another nycbuswatcher deployment)
    docker-compose up -d --build
    ```

## How It Works


### Acquire
The main daemon that fetches 200+ individual JSON feeds from the MTA BusTime API asynchronously, every minute, parses and dumps both the full response and a set of pickled `BusObservation` class instances to disk. Once per hour, these files are reprocessed—the raw responses are tar'ed into cold storage, and the pickles are serialized into a single JSON file for each hour, each route. 

### API
The API serves these hourly, per route JSON files full of serialized `BusObservation` instances. There's no database, and no queries or data processing at all to serve API responses. Endpoint routes are converted into a `DateRoutePointer` instance, which is how `acquire.py` manages data internally (and uses several classes to convert to filepaths in the `data/` folder).


## Reprocessor

Utilities for migrating data from older versions.

#### archive_reprocessor.py

Copy your *.gz files to `./archive` exec into the `nycbuswatcher_reprocessor` container:
- `docker exec -it nycbuswatcher_reprocessor_1 /bin/bash`

and run the script:
- `python archive_reprocessor.py -d [sqlite, mysql] ./archives`

The script will look in `./archives` or any `<datadir>` for any files in the form of `daily-YYYY-MM-DD.gz` and starting form the earliest date does the following:

1. Unzips the archive to the current folder with the name structure `daily-YYYY-MM-DD.json.gz`
2. Begins loading the JSON as a stream, pulling out each `Siri` response, which represents a single route for a single point in time.
3. Parses each `MonitoredVehicleJourney` into a `BusObservation` class instance, and adds that to a database session. The session is committed after each `Siri` response is parsed.
4. Writes each day's data to a single `daily-YYYY-MM-DD.sqlite3` file.

#### archive_db2shipments.py

This script will dump from a mysql database to `shipment` files.
