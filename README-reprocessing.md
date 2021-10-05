## Reprocessor

The repo contains a number of utilities for migrating data from older versions. New users won't need them.

#### archive_db2shipments.py

This script will dump from a mysql database to `shipment` files.


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

#### archive_shipment_rewriter.py

Fixes an bug in `archive_reprocessor.py` that casts float fields (lat, lon, bearing) as strings.

#### archive_siri2shipments.py

Converts a bulk, concatenated collection of SIRI responses (old archive format) into Shipment files in a Data Store structure.

#### make_glacier_indexes.py
#### make_shipment_indexes.py

very simple scripts that instantiate data structures and index the store/lake

#### make_route_histories.py

not implemented, for endpoint that bundles all data on a route for download
