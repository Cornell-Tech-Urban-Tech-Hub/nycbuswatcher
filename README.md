# NYCBusWatcher
- v2.0 June 2021
- Anthony Townsend <atownsend@cornell.edu>

## Acquire
The main daemon that fetches 200+ individual JSON feeds from the MTA BusTime API asynchronously, every minute, parses and dumps both the full response and a set of pickled `BusObservation` class instances to disk.
Once per hour, these files are reprocessed—the raw responses are tar'ed into cold storage, and the pickles are serialized into a single JSON file for each hour, each route. 

## API

The API serves these hourly, per route JSON files full of serialized `BusObservation` instances. There's no database, and no queries or data processing at all to serve API responses. Endpoint routes are converted into a `DatePointer` instance, which is how `acquire.py` manages data internally (and uses several classes to convert to filepaths in the `data/` folder).

##### List of endpoints 
- `/docs`
- `/redocs`

## quick start 

1. clone the repo

    `git clone https://github.com/anthonymobile/nycbuswatcher.git
   && cd nycbuswatcher`
    
    
2. obtain API keys and put them in .env (quotes not needed apparently, no spaces)
    - http://bustime.mta.info/wiki/Developers/Index/

    ```txt
    API_KEY=fasjhfasfajskjrwer242jk424242
    ```

3. if you want to use the gandi dyndns updater, add these three keys to .env and make sure to uncomment the appropriate section in `docker-compose.yml`


4. build and run the stack

    ```
    export COMPOSE_PROJECT_NAME=nycbuswatcher2 # (optional, if running alongside another nycbuswatcher deployment)
    docker-compose up -d --build
    ```
