# NYCBusWatcher
- v2.0 July 2021
- Anthony Townsend <atownsend@cornell.edu>


## Description

NYCBusWatcher is a fully-containerized set of Python scripts that fetches, parses, and redistributes bulk data of bus position records from the NYC Transit BusTime API. For speed and scalability, there is no database. Everything is done with serialized data stored in static files.

## Quickstart

The easiest way to use NYCBusWatcher is to simply pull data from our public API, powered by FastAPI. This API serves up batches of bus observations in either JSON or GeoJSON format, bundled in hourly increments per route (hereafter referred to as 'shipments'). This allows data users to quickly pull large amounts of data without the overhead of a database. Several APIs are provided for discovering what shipments are available (e.g. date coverage, and route coverage). Please suggest other APIs.

- [API home page]((https://api.buswatcher.org)), redirects to /docs for now.
- [API docs](https://api.buswatcher.org/docs), includes test capabilities.
- [Alt API docs (redoc)](https://api.buswatcher.org/redoc), easier reading.

## Run Your Own Service


1. Clone the repo.

    `git clone https://github.com/anthonymobile/nycbuswatcher.git
   && cd nycbuswatcher`
    
    
2. [Obtain a BusTime API key](http://bustime.mta.info/wiki/Developers/Index/) and save it in .env in the root of the repo (quotes not needed, but no spaces)

    ```txt
    API_KEY=fasjhfasfajskjrwer242jk424242
    ```

3. Bring the stack up.

    ```
    export COMPOSE_PROJECT_NAME=nycbuswatcher2 # (optional, if running alongside another nycbuswatcher deployment)
    docker-compose up -d --build
    ```

## How It Works


### Acquire
The main daemon that fetches 200+ individual JSON feeds from the MTA BusTime API asynchronously, every minute, parses and dumps both the full response and a set of pickled `BusObservation` class instances to disk. Once per hour, these files are reprocessed—the raw responses are tar'ed into cold storage, and the pickles are serialized into a single JSON file for each hour, each route. 

### API
The API serves these hourly, per route JSON files full of serialized `BusObservation` instances. There's no database, and no queries or data processing at all to serve API responses. Endpoint routes are converted into a `DateRoutePointer` instance, which is how `acquire.py` manages data internally (and uses several classes to convert to filepaths in the `data/` folder).

