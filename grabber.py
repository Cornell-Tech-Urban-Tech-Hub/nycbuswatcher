import argparse
import os
import time

from apscheduler.schedulers.background import BackgroundScheduler
import trio
from dotenv import load_dotenv

import Database as db
import Dumpers as dump
import GTFS2GeoJSON

from config import config


def get_db_args(): #todo refactor me into Database.py?

    if args.localhost is True: # todo fix this why does it ignore what's in config/development.py
        dbhost = 'localhost'
    elif os.environ['PYTHON_ENV'] == "development":
        dbhost = 'localhost'
    else:
        dbhost = config.config['dbhost']

    return (config.config['dbuser'],
            config.config['dbpassword'],
            dbhost,
            config.config['dbport'],
            config.config['dbname']
            )

def to_db(timestamp, feeds): #todo refactor me into Database.py?
    db_url=db.get_db_url(*get_db_args())
    db.create_table(db_url)
    session = db.get_session(*get_db_args())
    print('Dumping to {}'.format(db_url))
    num_buses = 0
    for route_bundle in feeds:
        for route_id,route_report in route_bundle.items():
            buses = db.parse_buses(timestamp, route_id, route_report.json(), db_url)
            for bus in buses:
                session.add(bus)
                num_buses = num_buses + 1
        session.commit()
    return num_buses


def async_grab_and_store():

    start = time.time()
    path_list = dump.get_path_list()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path)
        except ValueError as e :
            print ('{} from DNS issues'.format(e))
        feeds.append({route_id:r})

    async def main(path_list):
        from asks.sessions import Session

        if args.localhost is True:
            s = Session('http://bustime.mta.info', connections=5)
        else:
            s = Session('http://bustime.mta.info', connections=config.config['http_connections'])
        async with trio.open_nursery() as n:
            for path_bundle in path_list:
                for route_id,path in path_bundle.items():
                    n.start_soon(grabber, s, path, route_id )

    trio.run(main, path_list)
    timestamp = dump.to_file(feeds)
    dump.to_lastknownpositions(feeds)
    num_buses = to_db(timestamp, feeds)
    end = time.time()
    print('Fetched {} buses on {} routes in {:2f} seconds to gzipped archive and mysql database.\n'.format(
    num_buses,len(feeds),(end - start)))
    return


if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v1.11. March 2021. Anthony Townsend <atownsend@cornell.edu>')
    print('mode: {}'.format(os.environ['PYTHON_ENV']))

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    args = parser.parse_args()

    load_dotenv()

    # PRODUCTION = start main loop
    if os.environ['PYTHON_ENV'] == "production":
        interval = 60
        print('Scanning on {}-second interval.'.format(interval))
        scheduler = BackgroundScheduler()
        scheduler.add_job(async_grab_and_store, 'interval', seconds=interval, max_instances=2, misfire_grace_time=15)
        scheduler.add_job(GTFS2GeoJSON.update_route_map, 'cron', hour='2') #run at 2am daily
        scheduler.add_job(dump.rotate_files,'cron', hour='1') #run at 1 am daily
        scheduler.start()
        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        async_grab_and_store()



