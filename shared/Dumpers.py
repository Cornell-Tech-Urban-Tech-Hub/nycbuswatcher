import json
import pickle
import time
from pathlib import Path
import datetime as dt
import trio

from shared.config import config
import shared.Helpers as help

from glob import glob
import geojson
import tarfile
import sys
import os

from shared.BusObservation import BusObservation

pathmap = {
    'barrel':'data/barrels',
    'puddle':'data/puddles',
    'lake': 's3',
    'store':'static'
}

class GenericFolder():

    def __init__(self, date_dict, kind = None):

        self.path = self.get_path(pathmap[kind],date_dict) # generate the path to this barrel
        self.checkpath(self.path) # make sure my folder exists, if not create it

    def get_path(self, prefix, date_dict):
        folderpath = Path.cwd() / prefix / date_dict['year'] / date_dict['month'] / date_dict['day'] / date_dict['hour']
        self.checkpath(folderpath)
        return folderpath

    def checkpath(self,path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

# a Puddle is a folder holding raw JSON responses for a single route, single hour
class Puddle(GenericFolder):

    def __init__(self, date_dict):
        super().__init__(date_dict, kind='puddle')
        # self.path is inherited from GenericFolder

    # dump each response to data/puddle/YYYY/MM/DD/HH/route_id/response_2021-04-03T12:12:12.json
    def put_responses(self, feeds, timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                try:
                    route_data = route_data.json()

                    # create each route folder and the pickle filepath
                    folder = self.path / str(route)
                    self.checkpath(folder)
                    filename = 'response_{}_{}.json'.format(route,timestamp)
                    filepath = folder / filename

                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)

                except Exception as e: # no vehicle activity?
                    print (e)
                    pass
        return

    def list_responses(self, date_dict):
        puddle_path=Path.cwd() / 'data' / date_dict['year'] / date_dict['month'] / date_dict['day'] / date_dict['hour']
        folders = Path('data/').glob('**')
        return folders #todo check its just the route subfolders

    def get_responses(self):
        #
        return

    def render_puddle_to_storage(self):
        #
        return

# a DataLake instance represents all of the Puddles
class DataLake():

    def __init__(self):
        self.puddles = self.gen_puddle_list()
        pass

    # builds a list of paths to puddles — e.g. hourly route folders of JSON files
    def gen_puddle_list(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        return [p for p in dirs]

    # todo test
    def render_puddles(self):
        puddles_to_render = self.gen_puddle_list()
        for puddlepath in puddles_to_render:
            lake = Path.cwd() / pathmap['lake']
            lake.mkdir(parents=True, exist_ok=True)
            files_to_archive = glob.glob(puddlepath, recursive=True) # recursive unneeded?
            route=puddlepath.parts[-1] # grab last part of path
            outfile=lake / 'lake_{}.tar.gz'.format(route)
            with tarfile.open(outfile, "w:gz") as tar:
                for file in files_to_archive:
                    tar.add(file)
            # remove everything in the response directory 
            # race issues?(what if something is writing to it during the run?)
            for file in files_to_archive:
                try:
                    os.remove(file)
                except:
                    pass
            return

# each Barrel instance represents a collection of pickles parsed from a collection of feeds
class Barrel(GenericFolder):

    def __init__(self, date_dict):
        super().__init__(date_dict, kind='barrel')
        # self.path is inherited from GenericFolder

    # dump each pickle to data/barrel/YYYY/MM/DD/HH/route_id/barrel_2021-04-03T12:12:12.dat
    def put_pickles(self,feeds,timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                pickles = []
                try:
                    route_data = route_data.json()

                    # create each route folder and the pickle filepath
                    folder = self.path / str(route)
                    self.checkpath(folder)
                    filename = 'barrel_{}_{}.dat'.format(route,timestamp)
                    filepath = folder / filename

                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)
                    with open(filepath, "wb") as f:
                        pickle.dump(pickles, f)
                except KeyError: # no VehicleActivity?
                    print ('KeyError: NoVehicleActivity - route {}'.format(route))
                    pass
                except json.JSONDecodeError: # no response?
                    print ('JSONDecodeError: No/Bad API response? - route {}'.format(route))
                    pass
        return

    def list_pickles(self,date_tuple):
        #
        return

    def get_pickles(self):
        #
        return

    def render_pickles_to_static(self):
        '''
        def render_barrel(self):

            # # todo make sure we are not rendering the current hour barrel, which is still in use
            # more complete = compare year/month/date/hour
            # self.path.split('/')[-1] # compare the last item in the path (the hour) to the current hour
            current_datepath = '{}/{}/{}/{}'(datetime.datetime.now().year,
                                      datetime.datetime.now().month,
                                      datetime.datetime.now().day,
                                      datetime.datetime.now().hour)
            barrel_datepath = self.path.split('/')[2:].join('/')
            if current_datepath == barrel_datepath: # compare self.path starting with the 3rd element
                print ('stopping: i wasnt able to exclude the current hour folder. add some logic to avoid this, or run the grabber now to trigger it')
                sys.exit()
                continue # get out of here
            else:
                # if ok, continue to render the barrel
                picklefiles=[x for x in glob("*.dat") if x.is_file()] #bug checkme

                pickle_array = []
                for picklefile in picklefiles:
                    # print('checking {}'.format(picklefile))
                    with open(picklefile, 'rb') as pickle_file:
                        barrel = pickle.load(pickle_file)
                        for p in barrel:
                            pickle_array.append(p)
                        # print ('added {} to route pickle'.format(picklefile))

                #make a template for valid JSON
                json_container = dict()

                #iterate over pickle_array and insert into JSON
                serial_array=[]
                for p in pickle_array:
                    serial_array.append(p.to_serial())

                json_container['buses'] = serial_array


                static_path=route_folder.replace('data/barrel','static')+'/'
                checkpath(static_path)

                rendered_file='rendered_barrel_{}.json'.format(route) #todo add date to filename?
                with open(static_path+'/'+rendered_file, 'w') as f:
                    json.dump(json_container, f)

                print ('rendered {} pickles from {} files in {} barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), route, static_path+rendered_file))

                print ('pickled {}'.format([p for p in picklefiles]))


        def render_pickles(self,data):
            get_pickles()
            j = jsonify
            with open(self.file_name, ‘w’) as f
            json.dump(j,f)
            return


        #     #bug debugged to here-------------------------------------------------------------

        #         #todo figure out how to stop it from reprocessing old files
        #         #option 1=delete them
        #         #option 2=check if there is a rendered_barrel_{route}.json in static/2021/6/1/22/Q4/ already and if so, dont render
        #
        #         #remove everything in the barrel directory (but what if something is writing to it during the run?)
        #         #     for file in yesterday_gz_files:
        #         #         try:
        #         #             os.remove(file)
        #         #         except:
        #         #             pass
        #     return
        '''

        return

# each Stage instance represents a collection of static JSON files rendered from a pickle Barrel
class Stage(GenericFolder):

    def __init__(self, date_tuple):
        super().__init__(date_tuple, kind='static')
        # self.path is inherited from GenericFolder



# a DataStore instance represents all of the Barrels and Stages
class DataStore():
    def __init__(self):
        # self.barrels= None #todo automatically crawl the whole tree
        # todo calculate some metadata about the whole data store and keep it here (array of dates and hours covered, # of records, etc.)
        pass

    # trigger each barrel to render itself to static
    def render_barrels(self):
        for barrel in self.barrels:
            barrel.render_pickles_to_static() #todo logic to handle duplicates and missings should be in the barrel (e.g. it should manage itself)
            print('All old Barrels are empty.')
            return

def async_grab_and_store(localhost_flag):

    start = time.time()
    SIRI_request_urlpaths = help.get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path)
            feeds.append({route_id:r}) # UnboundLocalError: local variable 'r' referenced before assignment
        except Exception as e :
            print ('{} from DNS issues'.format(e))

    async def main(path_list):
        from asks.sessions import Session

        if localhost_flag is True:
            s = Session('http://bustime.mta.info', connections=5)
        else:
            s = Session('http://bustime.mta.info', connections=config.config['http_connections'])
        async with trio.open_nursery() as n:
            for path_bundle in path_list:
                for route_id,path in path_bundle.items():
                    n.start_soon(grabber, s, path, route_id )

    trio.run(main, SIRI_request_urlpaths)

    # dump to the various locations
    timestamp = dt.datetime.now()

    date_dict = {'year': timestamp.year,
                 'month': timestamp.month,
                 'day': timestamp.day,
                 'hour': timestamp.hour
                 }

    Barrel(date_dict).put_pickles(feeds,timestamp)
    Puddle(date_dict).put_puddles(feeds, timestamp)

    # report results to console
    num_buses = help.num_buses(feeds)
    end = time.time()
    print('Fetched {} BusObservations on {} routes in {:2f} seconds to a pickle Barrel and a json Lake.\n'.format(num_buses,len(feeds),(end - start)))
    return