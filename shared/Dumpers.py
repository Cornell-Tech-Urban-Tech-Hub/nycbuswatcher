import datetime
import sys
import os
import glob
import datetime as dt
import json
import pickle
import geojson
import tarfile
import os.path
from pathlib import Path
from glob import glob

from shared.BusObservation import BusObservation

class GenericFolder():

    def __init__(self, date_tuple, kind = None):
        pathmap = {
            'barrel':'data/barrel',
            'puddle':'data/puddle',
            'static':'static'
        }
        self.path = self.get_path(pathmap[kind],date_tuple) # generate the path to this barrel
        self.checkpath(self.path) # make sure my folder exists, if not create it

    def get_path(self, prefix, date_tuple):
        year, month, day, hour = date_tuple
        folderpath = Path.cwd() / prefix / str(year) / str(month) / str(day) / str(hour)
        self.checkpath(folderpath)
        return folderpath

    def checkpath(self,path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

# a DataStore instance represents all of the Barrels and Stages
class DataStore():
    def __init__(self):
        self.barrels= None #todo automatically crawl the whole tree
        pass

# each Barrel instance represents a collection of pickles parsed from a collection of feeds
class Barrel(GenericFolder):

    def __init__(self, date_tuple):
        super().__init__(date_tuple, kind='barrel')
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
                    # print ('i didnt dump barrel file for {}'.format(route_id.split('_')[1]))
                    pass
        return

    def list_pickles(self):
        #
        return

    def get_pickles(self):
        #
        return

    def render_pickles_to_static(self):
        #
        return


# each Stage instance represents a collection of static JSON files rendered from a pickle Barrel
class Stage(GenericFolder):

    def __init__(self, date_tuple):
        super().__init__(date_tuple, kind='static')
        # self.path is inherited from GenericFolder


# a DataLake instance represents all of the Puddles
class DataLake():
    def __init__(self):
        self.puddles = None #todo automatically crawl the whole tree
        pass

# each Puddle instance represents a collection of pickles parsed from a collection of feeds
class Puddle(GenericFolder):

    def __init__(self, date_tuple):
        super().__init__(date_tuple, kind='puddle')
        # self.path is inherited from GenericFolder

    def put_responses(self,feed):
        #
        return

    def list_responses(self):
        #
        return

    def get_responses(self):
        #
        return

    def tar_responses_to_storage(self):
        #
        return



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

class ResponseStore(GenericFolder):

    def __init__(self, feeds, timestamp, kind='lake'):
        super().__init__(kind)
        self.make_lakes(feeds, timestamp)

    # dump each pickle to data/barrel/YYYY/MM/DD/HH/route_id/response_2021-04-03T12:12:12.json
    def make_lakes(self, feeds, timestamp):
        for route_report in feeds:
            # dump each route's response as a raw JSON file
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                try:
                    route_data = route_data.json()
                    folder = self.path+route
                    self.checkpath(folder)
                    file = '{}/response_{}_{}.json'.format(folder,route,timestamp)
                    with open(file, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f)
                except Exception as e: # no vehicle activity?
                    print (e)
                    pass
        return

    # def get_lakes(self):
    #     return # lake_array
    #
    # def render_lakes(self):  #todo
    #
    #     #todo use the path crawling algorithm like above

        # bundle up everything in ./data/responses/*.json into a tarball into ./data/archive
        # https://programmersought.com/article/77402568604/
        #
        # tarballpath = './data/archive'
        # raw_response_path = ''.join(['data/',get_dumppaths()['responsepath'],'**/*.json'])
        #
        # files_to_archive = glob.glob(raw_response_path, recursive=True)
        #
        # outfile=
        # with tarfile.open(outfile, "w:gz") as tar:
        #     for file in yesterday_gz_files:
        #         tar.add(file)
        #
        #     print ('made a tarball of {} files from {} into {}'.format(len(yesterday_gz_files), yesterday, outfile))

        #     #remove everything in the response directory (but what if something is writing to it during the run?)
        #
        #     for file in yesterday_gz_files:
        #         try:
        #             os.remove(file)
        #         except:
        #             pass
        # return

'''