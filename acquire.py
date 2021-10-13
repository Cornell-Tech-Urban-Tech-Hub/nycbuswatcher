import argparse
import logging
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from common.Models import *

if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v2.1 (mongo-db branch) October 2021. Anthony Townsend <atownsend@cornell.edu>')

    # loads the .env file
    load_dotenv()

    environment = os.environ['PYTHON_ENV']

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    # parser.add_argument('-a', action="store_true", dest="archive_mode", default=False, help="save full API responses in archive")
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        print ('VERBOSE ON')
    else:
        logging.basicConfig(level=logging.WARNING)
        print ('VERBOSE OFF, WARNINGS+ERRORS ONLY')

    # PRODUCTION = start main loop
    if environment == "production":

        scheduler = BackgroundScheduler()

        # every minute
        scan_interval_seconds = 60

        logging.debug('{} mode. Scanning on {}-second interval.'.format(os.environ['PYTHON_ENV'].capitalize(), scan_interval_seconds))
        scheduler.add_job(async_grab_and_store,
                          'interval',
                          args=[environment],
                          seconds=scan_interval_seconds,
                          misfire_grace_time=15)

        # Start the schedulers
        scheduler.start()

        try:
            while True:
                import time
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif environment == "development":
        async_grab_and_store(environment)
