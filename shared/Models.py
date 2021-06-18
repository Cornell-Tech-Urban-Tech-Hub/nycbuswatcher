import os
import json
import pickle
import tarfile
import pandas as pd

from datetime import date, datetime
from pathlib import Path, PurePath
from glob import glob
from uuid import uuid4

from shared.BusObservation import BusObservation
import shared.config.config as config

pathmap = {
    'glacier':'data/lake/glaciers',
    'lake':'data/lake',
    'puddle':'data/lake/puddles',
    'shipment':'data/store/shipments',
    'store':'data/store',
    'barrel':'data/store/barrels',
    'dashboard':'data/dashboard.csv'
    }


class DatePointer():

    def __init__(self,timestamp):
        self.timestamp = timestamp
        self.year = timestamp.year
        self.month = timestamp.month
        self.day = timestamp.day
        self.hour = timestamp.hour
        self.purepath = self.get_purepath()

    def get_purepath(self):
        return PurePath(str(self.year),
                     str(self.month),
                     str(self.day),
                     str(self.hour)
                     )

    def __repr__(self):
        return ('-'.join([str(self.year),
                              str(self.month),
                              str(self.day),
                              str(self.hour)
                              ]))


class DateRoutePointer(DatePointer):

    def __init__(self,timestamp,route=None):
        super().__init__(timestamp)
        self.route = route
        self.purepath = PurePath(self.purepath, self.route) #extend self.purepath inherited from parent.super()

    def __repr__(self):
        return ('-'.join([str(self.year),
                              str(self.month),
                              str(self.day),
                              str(self.hour),
                              self.route]))


class GenericStore():

    def __init__(self, kind=None):
        self.path = self.get_path(pathmap[kind])
        self.uid = uuid4().hex
        # print('+instance::GenericStore::of kind {} at {} with uid {}'.format(kind,self.path,self.uid))


    def get_path(self, prefix):
        folderpath = Path.cwd() / prefix
        Path(folderpath).mkdir(parents=True, exist_ok=True)
        return folderpath

    def path_to_DateRoutePointer(self, apath, route):
        parts = apath.split('/')
        pointer = DateRoutePointer(datetime(year=int(parts[-5]),
                                    month=int(parts[-4]),
                                    day=int(parts[-3]),
                                    hour=int(parts[-2])
                                    ),
                                route)
        return pointer


class GenericFolder():

    def __init__(self, date_pointer, kind=None):
        self.date_pointer=date_pointer
        self.path = self.get_path(pathmap[kind], date_pointer)
        # print('+instance::GenericFolder::of kind {} at {} from pointer {}'.format(kind,self.path,date_pointer))

    def get_path(self, prefix, date_pointer):
        folderpath = Path.cwd() / prefix / date_pointer.purepath
        Path(folderpath).mkdir(parents=True, exist_ok=True)
        return folderpath

    # after https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    def delete_folder(self):
        def rm_tree(pth):
            for child in pth.glob('*'):
                if child.is_file():
                    child.unlink()
                else:
                    rm_tree(child)
            pth.rmdir()
        rm_tree(self.path)
        return


class DataLake(GenericStore):

    def __init__(self):
        super().__init__(kind='lake')
        #dont init self.puddles but call self.scan_puddles() as needed

    def make_puddles(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                puddle_date_pointer=DateRoutePointer(date_pointer.timestamp, route)
                try:
                    folder = Puddle(puddle_date_pointer).path
                    filename = 'drop_{}_{}.json'.format(puddle_date_pointer.route, puddle_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    route_data = route_data.json()
                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)
                except Exception as e: # no vehicle activity?
                    print (e)
                    pass
        return

    def scan_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        puddles = []
        for d in dirs:
            rt=d.split('/')[-1]
            p=Puddle(self.path_to_DateRoutePointer(d,rt))
            puddles.append(p)
        print('rescanned::DataLake uid {}::found {} Puddles at {}'.format(self.uid, len(puddles),str(Path.cwd() / pathmap['puddle'])))
        return puddles

    def list_expired_puddles(self):
        expired_puddles = []
        for puddle in self.scan_puddles():
            bottom_of_hour = DatePointer(datetime.now())
            bottom_of_hour.route=puddle.route
            if str(puddle.date_pointer) != str(bottom_of_hour):
                expired_puddles.append(puddle)
        return expired_puddles

    def freeze_puddles(self):
        puddles_to_archive = self.list_expired_puddles()
        if len(puddles_to_archive) == 0:
            print('no expired puddles to freeze')
            return
        for puddle in puddles_to_archive:
            puddle.freeze_myself_to_glacier()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['lake']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return


class Puddle(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='puddle')
        if self.date_pointer.route is False:
            raise Exception ('tried to instantiate Puddle because you called __init__ without a value in DatePointer.route')
        self.route = self.date_pointer.route


    def freeze_myself_to_glacier(self):
        drops_to_freeze=[x for x in self.path.glob('*.json') if x.is_file()]
        try:
            outfile = Glacier(self.date_pointer)
        except OSError:
            # future write handler for partially rendered puddles(maybe a different filename, or move it to a lost+found?)
            return
        with tarfile.open(outfile.filepath, "w:gz") as tar:
                for drop in drops_to_freeze:
                    tar.add(drop, arcname=drop.name.replace(':','-')) # tar doesnt like colons
        print ('froze {} drops to Glacier at {}'.format(len(drops_to_freeze), outfile.path))
        self.delete_folder()
        return


class Glacier(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='glacier')
        self.route = self.date_pointer.route
        self.exist, self.filepath = self.check_exist()
        if self.exist == True:
            raise OSError ('there is already an archive at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'glacier_{}.tar.gz'. \
            format(self.date_pointer)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)


class DataStore(GenericStore):

    def __init__(self):
        super().__init__(kind='store')
        # dont init self.barrels and self.shipments instead call them as needed with self.scan_barrels() and self.scan_shipments()
        # future other metadata -- array of dates and hours covered, total # of records, etc.

    def make_barrels(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                barrel_date_pointer=DateRoutePointer(date_pointer.timestamp, route)
                pickles = []
                try:
                    folder = Barrel(barrel_date_pointer).path
                    filename = 'pickle_{}_{}.dat'.format(barrel_date_pointer.route, barrel_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    route_data = route_data.json()
                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)
                    with open(filepath, "wb") as f:
                        pickle.dump(pickles, f)
                except KeyError:
                    # future find a way to filter these out to reduce overhead
                    # this is almost always the result of a route that doesn't exist, so why is it in the OBA response?
                    pass
                except json.JSONDecodeError:
                    # this is probably no response?
                    print ('JSONDecodeError: No/Bad API response? - route {}'.format(route))
                    pass
        return

    def scan_barrels(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['barrel']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        barrels = []
        for d in dirs:
            rt=d.split('/')[-1]
            b = Barrel(self.path_to_DateRoutePointer(d,rt))
            barrels.append(b)
        print('rescanned::DataStore uid {}::found {} Barrels at {}'.format(self.uid, len(barrels),str(Path.cwd() / pathmap['barrel'])))
        return barrels

    def list_expired_barrels(self):
        expired_barrels = []
        for puddle in self.scan_barrels():
            bottom_of_hour = DatePointer(datetime.now())
            bottom_of_hour.route=puddle.route
            if str(puddle.date_pointer) != str(bottom_of_hour):
                expired_barrels.append(puddle)
        return expired_barrels  # future sort list from oldest to newest

    def render_barrels(self):
        barrels_to_archive = self.list_expired_barrels()
        if len(barrels_to_archive) == 0:
            print('no expired barrels to render')
            return
        for barrel in barrels_to_archive:
            barrel.render_myself_to_shipment()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['store']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return


    def list_routes_in_store(self, date_pointer_query):
        routes = []
        self.shipments = self.scan_shipments() #reload here Just In Case
        dp1=date_pointer_query
        for shipment in self.shipments:
            dp2=shipment.date_pointer
            if dp1.year == dp2.year:
                if dp1.month == dp2.month:
                    if dp1.day == dp2.day:
                        if dp1.hour == dp2.hour:
                            routes.append((shipment.route, shipment.url)) #todo change shipment.filepath to a URL
        return routes

    def dump_dashboard(self):
        dashboard=[]
        for b in self.scan_barrels():
            dashboard.append(
                ('Barrel',
                 b.date_pointer.route,
                 str(b.date_pointer),
                 b.date_pointer.year,
                 b.date_pointer.month,
                 b.date_pointer.day,
                 b.date_pointer.hour,
                 b.count_pickles())
            )
        for s in self.scan_shipments():
            dashboard.append(
                ('Shipment',
                 s.date_pointer.route,
                 str(s.date_pointer),
                 s.date_pointer.year,
                 s.date_pointer.month,
                 s.date_pointer.day,
                 s.date_pointer.hour,
                 s.count_buses())
            )
        dashboard_data=pd.DataFrame(dashboard, columns=['kind', 'route', 'datepointer_as_str', 'year', 'month', 'day', 'hour', 'num_buses'])
        dashboard_data.to_csv(Path.cwd() / Path(pathmap['dashboard']),index=False)
        return

    def scan_shipments(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['shipment']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        shipments = []
        for d in dirs:
            rt=d.split('/')[-1]
            s = Shipment(self.path_to_DateRoutePointer(d,rt))
            s.path = s.path / PurePath (s.route)
            shipments.append(s)
        return shipments

    def find_route_shipments(self,route):
        result = []
        shipments = self.scan_shipments()
        for s in shipments:
            if s.route == route:
                result.append(s)
        return result



class Barrel(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='barrel')
        if self.date_pointer.route is False:
            raise Exception ('tried to instantiate Barrel because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route

    def render_myself_to_shipment(self):
        pickles_to_render=[x for x in self.path.glob('*.dat') if x.is_file()]
        pickle_array = []
        for picklefile in pickles_to_render:
            with open(picklefile, 'rb') as pickle_file:
                barrel = pickle.load(pickle_file)
                for p in barrel:
                    pickle_array.append(p)
        serial_array=[]
        for p in pickle_array:
            serial_array.append(p.to_serial())
        json_container = dict()
        json_container['buses'] = serial_array

        try:
            outfile = Shipment(self.date_pointer)
        except OSError:
            return

        with open(outfile.filepath, 'w') as f:
            json.dump(json_container, f, indent=4)
        print ('wrote {} pickles to Shipment at {}'.format(len(pickle_array), outfile.filepath))
        self.delete_folder()
        return

    def count_pickles(self):
        pickles_to_count=[x for x in self.path.glob('*.dat') if x.is_file()]
        pickle_count = 0
        for picklefile in pickles_to_count:
            with open(picklefile, 'rb') as pickle_file:
                barrel = pickle.load(pickle_file)
                pickle_count = pickle_count + len(barrel)
        return pickle_count


class Shipment(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='shipment')
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        self.url = config.config['shipment_api_url'].format(str(date_pointer.year),
                                                            str(date_pointer.month),
                                                            str(date_pointer.day),
                                                            str(date_pointer.hour),
                                                            date_pointer.route)
        try:
            if self.exist == True:
                pass
        except OSError:
            print ('!!error::Shipment::skipping! there is already a Shipment at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'shipment_{}.json'.\
            format(self.date_pointer)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    def load_file(self):
        with open(self.filepath, "r") as f:
            return json.load(f)

    def to_FeatureCollection(self):
        geojson = {'type': 'FeatureCollection', 'features': []}
        shipment = self.load_file()
        for bus in shipment['buses']:
            feature = {'type': 'Feature',
                       'properties': {},
                       'geometry': {'type': 'Point',
                                    'coordinates': []}}
            feature['geometry']['coordinates'] = [bus['lon'], bus['lat']]
            for k, v in bus.items():
                if isinstance(v, (datetime, date)):
                    v = v.isoformat()
                feature['properties'][k] = v
            geojson['features'].append(feature)
        return geojson

    def count_buses(self):
        data = self.load_file()
        return len(data['buses'])
