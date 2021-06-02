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
from glob import glob

from shared.BusObservation import BusObservation


class DumpFolder():

    def __init__(self, kind = None):
        self.root_dir = 'data/{}'
        self.kind=kind
        self.path = self.get_datepath(self.root_dir.format(self.kind)) # generate the path to this barrel
        self.checkpath(self.path) # make sure my folder exists, if not create it

    def get_datepath(self, path_prefix):
        now = datetime.datetime.now()
        dumppath= '{}/{}/{}/{}/{}/'.format(
            path_prefix,
            str(now.year),
            str(now.month),
            str(now.day),
            str(now.hour)
        )
        self.checkpath(dumppath)
        return dumppath

    def checkpath(self,path):
        # make sure any paths we return exist, or create them
        check = os.path.isdir(path)
        if not check:
            os.makedirs(path)
            # print("created folder : ", path)
        else:
            pass
        return


class FeedBarrel(DumpFolder):

    def __init__(self, feeds, timestamp, kind='barrel'):
        super().__init__(kind)
        self.make_pickles(feeds, timestamp)

    # dump each pickle to data/barrel/YYYY/MM/DD/HH/route_id/barrel_2021-04-03T12:12:12.dat
    def make_pickles(self, feeds, timestamp):

        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                pickles=[]
                try:
                    route_data = route_data.json()
                    folder = self.path+route
                    self.checkpath(folder)
                    file = '{}/barrel_{}_{}.dat'.format(folder,
                                                        route,
                                                        timestamp
                                                        )

                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)
                    with open(file, "wb") as f:
                        pickle.dump(pickles, f)
                except KeyError: # no VehicleActivity?
                    # print ('i didnt dump barrel file for {}'.format(route_id.split('_')[1]))
                    pass
        return

    # def get_pickles(self):
    #     return # pickle_array
    #
    # def render_pickles(self,data):
    #     get_pickles()
    #     j = jsonify
    #     with open(self.file_name, ‘w’) as f
    #     json.dump(j,f)
    #     return
    #
    # def render_barrel():
    #
    #     rootdir='data/barrel/'
    #
    #     hour_folder_pathlist = []
    #
    #     depth = 3
    #     for root,dirs,files in os.walk(rootdir):
    #         if root[len(rootdir):].count(os.sep) < depth:
    #             for d in dirs:
    #                 hour_folder_pathlist.append(os.path.join(root,d))
    #
    #     # remove the first 3 which are
    #     # 'data/barrel/YYYY/'
    #     # 'data/barrel/YYYY/M/D'
    #     # 'data/barrel/YYYY/M/D/HH'
    #     hour_folder_pathlist = hour_folder_pathlist[3:]
    #
    #     # setup the exclude path for the current hour folder
    #     now = datetime.datetime.now()
    #     excluded_path = ('{}{}/{}/{}/{}').format(rootdir, str(now.year), str(now.month), str(now.day), str(now.hour))
    #
    #     # verify that we ARE NOT rendering the folder for the current hour (maybe run this every 30 or 45 minutes then to ensure an offset?)
    #     # makesure that excludepath is not IN hour_folder_pathlist and remove it
    #     try:
    #         hour_folder_pathlist.remove(excluded_path)
    #     except:
    #         print ('stopping: i wasnt able to exclude the current hour folder. add some logic to avoid this, or run the grabber now to trigger it')
    #         sys.exit()
    #         pass
    #
    #     #bug debugged to here-------------------------------------------------------------
    #
    #     # make sure we ARE rendering any folders that did not get rendered earlier
    #     for hour_folder in hour_folder_pathlist:
    #
    #         route_folder_list = [(x.name,x.path) for x in os.scandir(hour_folder) if x.is_dir()]
    #         for route, route_folder in route_folder_list:
    #
    #             # load all the pickle files into a single array
    #             picklefile_list = glob("{}*.dat".format(route_folder+'/'))
    #
    #             pickle_array = []
    #             for picklefile in picklefile_list:
    #                 # print('checking {}'.format(picklefile))
    #                 with open(picklefile, 'rb') as pickle_file:
    #                     barrel = pickle.load(pickle_file)
    #                     for p in barrel:
    #                         pickle_array.append(p)
    #                     # print ('added {} to route pickle'.format(picklefile))
    #
    #             #make a template for valid JSON
    #             json_container = dict()
    #
    #             #iterate over pickle_array and insert into JSON
    #             serial_array=[]
    #             for p in pickle_array:
    #                 serial_array.append(p.to_serial())
    #
    #             json_container['buses'] = serial_array
    #
    #
    #             static_path=route_folder.replace('data/barrel','static')+'/'
    #             checkpath(static_path)
    #
    #             rendered_file='rendered_barrel_{}.json'.format(route) #todo add date to filename?
    #             with open(static_path+'/'+rendered_file, 'w') as f:
    #                 json.dump(json_container, f)
    #
    #             print ('rendered {} pickles from {} files in {} barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), route, static_path+rendered_file))
    #
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

#bug debug me
class FeedLake(DumpFolder):

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
                    file = '{}/response_{}_{}.json'.format(folder,
                                                        route,
                                                        timestamp
                                                        )

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
