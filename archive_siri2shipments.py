'''
run me in shell with

PYTHON_ENV='development' python3 archive_siri2shipments.py daily-2021-04-20.gz
output will show up in 'reprocessor' subfolder
takes about 12 mins on desktop mac per day

'''


import argparse, pathlib
import logging

from common.Models import *
import ijson, gzip, shutil
from fnmatch import fnmatch
import datetime
from pathlib import PurePath
from dateutil import parser

# archived response data should be in the ./reprocessor/input folder
# that folder is ignored by docker and git
# results of this process will output to

#################################################################################

cwd = Path.cwd()
topdir=PurePath('reprocessor')
input_path=topdir / 'input'
output_path=topdir / 'output'
lake = DataLake(output_path)
store = DataStore(output_path)

#################################################################################

# after https://www.aylakhan.tech/?p=27
def extract_responses(f):
    responses = ijson.items(f, 'Siri', multiple_values=True)
    try:
        for response in responses:
            yield response
    except Exception as e:
        logging.warning(e)

def get_daily_filelist(path):
    daily_filelist=[]
    include_list = ['daily*.gz']
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            if any(fnmatch(filename, pattern) for pattern in include_list):
                daily_filelist.append(filename)
    sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
    return sorted_daily_filelist

#################################################################################



if __name__=="__main__":

    # logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
    logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

    # parse arguments
    p = argparse.ArgumentParser(description='NYCbuswatcher shipment dumper, dumps from monthly SIRI response archives to shipments')
    # parser.add_argument('-m','--months', nargs='+', help='<Required> List of months to process (leading zero, no year, e.g. -m 10 11 12 01 = Oct 2020, Nov 2020, Dec 2020, and Jan 2021)', required=True)
    p.add_argument('filename')

    args = p.parse_args()

    # 1 make sure ./reprocessor exists
    pathlib.Path(topdir).mkdir(parents=True, exist_ok=True)

    # 2 unpack the archive into a temp file
    daily_filename_list = get_daily_filelist(input_path)

    # 3 parse each response file
    for daily_filename in daily_filename_list:
        time_started = datetime.datetime.now()
        logging.info('started at {}'.format(datetime.datetime.now()))

        gzipfile = input_path / daily_filename
        jsonfile = input_path / f'{daily_filename[:-3]}.json'

        # try to load the uncompressed JSON file from disk
        try:
            f = open(jsonfile, 'r')
            f.close()

        # if not exist, unzip it
        except:
            logging.info(f'Unzipping {input_path}/{daily_filename}')
            with gzip.open(gzipfile, 'rb') as f_in:
                with open(jsonfile, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        finally:

            logging.info('Parsing JSON responses and dumping to DataStore.Barrels')

            # open the day's JSON responses
            with open(jsonfile, 'r') as f:

                # separate the responses
                for siri_response in extract_responses(f):

                    #check if there's vehicle activity (e.g. this is not False)
                    try:
                        if not siri_response['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                            logging.warning("No VehicleActivity")
                            continue
                        else:
                            route_id = siri_response['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity'][0]['MonitoredVehicleJourney']['LineRef']

                            route_bundle = {route_id:{
                                'Siri':siri_response}
                            }
                            date_pointer = parser.parse(siri_response['ServiceDelivery']['ResponseTimestamp'])

                            # bug format is not correct format for make_barrels, which wants it as a Response object
                            store.make_barrels([route_bundle], DatePointer(date_pointer))
                    except:
                        logging.warning("Empty/invalid response (e.g. No such route)")

            #remove the json file
            try:
                os.path.exists(jsonfile)
                os.remove(jsonfile)
            except:
                pass

            # close
            time_finished = datetime.datetime.now()
            logging.info('finished at {}'.format(time_finished))
            logging.info('time elapsed: {}'.format(time_finished-time_started))


        #todo call store.render_shipments()

