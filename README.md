# NYCBusWatcher
- v2.1 October 2021
- Anthony Townsend <atownsend@cornell.edu>


## Description

NYCBusWatcher is a fully-containerized set of Python scripts that fetches, parses, and redistributes bulk data of bus position records from the NYC Transit BusTime API. Starting with version 2.1 it uses a mongodb backend containing 2 collections: ``siri_responses`` which keeps a copy of the full SIRI response from the MTA API, and ```buses``` which only retains the ```["VehicleActivity"]``` portion of the responses. The ```["RecordedAtTime"]``` string field in these records has been cast as a Javascript datetime object to make it easier to query based on datetime ranges.

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

## Run in A Development Environment

1. Run a mongo database locally on the standard port (27017)

2. Build a conda environment with the ```environment.yml``` file:
   
   ```conda env -f environment.yml```
   
2. ```export PYTHON_ENV="development"``` 
   
3. Run the scraper with the following settings:

   ```-v```   Verbose ON, will print all debug messages
   ```-l```   Localhost ON, used in conjunction with ```export PYTHON_ENV="production"``` if you need to test some of those settings locally outside a docker environment.

   Ex.: ```python acquire.py -v -l``` n.b. the scraper will only run once and quit in development mode.

5. Run the API similarly:

   Ex.: ```python api.py -v ``` n.b. there's no localhost override mode for API, its not needed.

## How It Works

### Acquire
The main daemon that fetches 200+ individual JSON feeds from the MTA BusTime API asynchronously, every minute, parses and dumps both the full response and a subset to a mongo database.

### API
The API allows you to retrieve all observed buses on an hourly basis.