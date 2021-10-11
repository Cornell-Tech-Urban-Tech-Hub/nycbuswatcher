import os
import json
import pickle
import tarfile
from dateutil import parser
import inspect
import logging
from collections import defaultdict
from decimal import Decimal
from shutil import copy

from datetime import date, datetime
from pathlib import Path, PurePath
from glob import glob
from uuid import uuid4

from pymongo import MongoClient

from common.Helpers import timer

import common.config.config as config

pathmap = {
    'glacier':'data/lake/glaciers',
    'lake':'data/lake',
    'puddle':'data/lake/puddles',
    'shipment':'data/store/shipments',
    'store':'data/store',
    'barrel':'data/store/barrels',
    'dashboard':'data/dashboard.csv',
    'history':'data/history'
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

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


class WorkDir():

    def __init__(self, cwd):
        self.cwd = cwd


class GenericStore(WorkDir):

    def __init__(self, cwd, kind=None):
        super().__init__(cwd)
        self.path = self.get_path(pathmap[kind])
        self.uid = uuid4().hex

    def get_path(self, prefix):
        folderpath = self.cwd / prefix
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


class GenericFolder(WorkDir):

    def __init__(self, cwd, date_pointer, kind=None):
        super().__init__(cwd)
        self.date_pointer=date_pointer
        self.path = self.get_path(pathmap[kind], date_pointer)
        # logging.debug('+instance::GenericFolder::of kind {} at {} from pointer {}'.format(kind,self.path,date_pointer))

    def get_path(self, prefix, date_pointer):
        folderpath = self.cwd / prefix / date_pointer.purepath
        Path(folderpath).mkdir(parents=True, exist_ok=True)
        return folderpath

    # after https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    # all these trapped FileNotFound errors can be ignored since they mean its already been deleted
    def delete_folder(self):
        def rm_tree(pth):
            for child in pth.glob('*'):
                if child.is_file():
                    try:
                        child.unlink()
                    except FileNotFoundError as e:
                        pass
                        # logging.error(f'{e}')
                else:
                    try:
                        rm_tree(child)
                    except FileNotFoundError as e:
                        pass
                        # logging.error(f'{e}')
            try:
                pth.rmdir()
            except FileNotFoundError as e:
                pass
                # logging.error(f'{e}')
        rm_tree(self.path)
        return


class RouteHistory(WorkDir):

    def __init__(self, cwd, route, kind='history'):
        super().__init__(cwd)
        self.cwd = cwd
        self.route = route
        self.path = self.get_path(pathmap[kind])
        self.url = config.config['history_api_url'].format(route)
        self.make_route_history()

    def get_path(self, prefix):
        folderpath = self.cwd / prefix
        Path(folderpath).mkdir(parents=True, exist_ok=True)
        return folderpath

    def make_route_history(self):
        lake = DataLake(self.cwd)
        store = DataStore(self.cwd)

        # populate the list of paths in the route history

        filepaths_in_history=[]
        for glacier in lake.scan_glaciers():
            if glacier.route == self.route:
                filepaths_in_history.append(glacier.filepath)
        for shipment in store.scan_shipments():
            if shipment.route == self.route:
                filepaths_in_history.append(shipment.filepath)

        # check if we can write the outfile
        try:
            outfile = self.path / f'route_history_{self.route}.tar.gz'
        except OSError:
            return

        # dump them all into a tar.gz route history file and return the path to the file
        with tarfile.open(outfile, "w:gz") as tar:
            for file in filepaths_in_history:
                tar.add(file, arcname=file.name.replace(':','-')) # tar doesnt like colons
        logging.debug ('froze {} Glaciers and Shipments  to RouteHistory at {}'.format(len(filepaths_in_history), outfile))

        return self.url

@timer
def make_route_histories():
    for route in DataLake(Path.cwd()).list_unique_routes():
        RouteHistory(Path.cwd(), route, kind='history').make_route_history()
        logging.debug(f'Wrote RouteHistory for {route}')
    return

class DataLake(GenericStore):

    def __init__(self, cwd):
        super().__init__(cwd, kind='lake')
        #dont init self.puddles but call self.scan_puddles() as needed

    def pickle_myself(self):
            filepath=self.path / 'DataLake.pickle'
            with open(filepath, "wb") as f:
                pickle.dump(self, f)

    def make_puddles(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                puddle_date_pointer=DateRoutePointer(date_pointer.timestamp, route)
                try:
                    folder = Puddle(self.cwd, puddle_date_pointer).path
                    filename = 'drop_{}_{}.json'.format(puddle_date_pointer.route, puddle_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    route_data = route_data.json()
                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)
                except Exception as e: # no vehicle activity?
                    logging.error (e)
                    pass
        return

    def scan_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(self.cwd / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        puddles = []
        for d in dirs:
            rt=d.split('/')[-1]
            p=Puddle(self.cwd, self.path_to_DateRoutePointer(d,rt))
            puddles.append(p)
        logging.debug('rescanned::DataLake uid {}::found {} Puddles at {}'.format(self.uid, len(puddles),str(self.cwd / pathmap['puddle'])))
        return puddles

    def list_expired_puddles(self):
        expired_puddles = []
        for puddle in self.scan_puddles():
            bottom_of_hour = DatePointer(datetime.now())
            if (puddle.date_pointer.year, puddle.date_pointer.month,
                puddle.date_pointer.day, puddle.date_pointer.hour) \
                != \
                (bottom_of_hour.year, bottom_of_hour.month,
                 bottom_of_hour.day,bottom_of_hour.hour):
                    expired_puddles.append(puddle)
        return sorted(expired_puddles, key=lambda p: p.date_pointer.timestamp)

    def freeze_puddles(self):
        puddles_to_archive = self.list_expired_puddles()
        if len(puddles_to_archive) == 0:
            logging.debug('no expired puddles to freeze')
            return
        for puddle in puddles_to_archive:
            puddle.freeze_myself_to_glacier()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(self.cwd / pathmap['lake']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return

    def scan_glaciers(self):
        files = glob('{}/*/*/*/*/*'.format(self.cwd / pathmap['glacier']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        glaciers = []
        for d in dirs:
            rt=d.split('/')[-1]
            g = Glacier(self.cwd, self.path_to_DateRoutePointer(d,rt))
            g.path = g.path / PurePath (g.route)
            glaciers.append(g)
        return glaciers


    def make_glacier_indexes(self):

        # 1 populate the shipments list
        self.glaciers = self.scan_glaciers()
        # logging.warning(f'scanned {len(self.glaciers)} glaciers')

        #  2 sort all the glaciers into a dict of lists
        # { 'M15': [Shipment, Shipment, Shipment...],
        #   'Bx44': [Shipment, Shipment, Shipment...]}
        glaciers_grouped=defaultdict(list)
        for g in self.glaciers:
            glaciers_grouped[g.route].append(g)

        for route, glaciers_list in glaciers_grouped.items():

            # 3 sort each route's shipments by year,month,day,hour
            glaciers_list_sorted = sorted(glaciers_list, key = lambda i: (i.date_pointer.year,i.date_pointer.month,i.date_pointer.day,i.date_pointer.hour))
            folderpath = self.path / PurePath('glaciers/indexes')
            Path(folderpath).mkdir(parents=True, exist_ok=True)

            outfile = folderpath / PurePath(f'glacier_index_{route.upper()}.json')
            Path(folderpath).mkdir(parents=True, exist_ok=True)

            # make the shipment_insert
            glacier_insert = []

            for s in glaciers_list_sorted:
                the_pointer = {'route': s.date_pointer.route,
                               'year': s.date_pointer.year,
                               'month': s.date_pointer.month,
                               'day': s.date_pointer.day,
                               'hour': s.date_pointer.hour,
                               'url' : s.url}
                glacier_insert.append(the_pointer)

            json_container = {"route":route.upper(), "glaciers": glacier_insert}

            with open(outfile, "w") as f:
                json.dump(json_container, f, indent=4)
            logging.debug ('wrote Glacier index for {route} to {outfile}')

        return

    def list_unique_routes(self):

        glaciers_grouped=defaultdict(list)
        for g in self.scan_glaciers():
            glaciers_grouped[g.route].append(g)
        routelist = [route for (route, glaciers_list) in glaciers_grouped.items()]

        return sorted(list(set(routelist)))


class Puddle(GenericFolder):

    def __init__(self, cwd, date_pointer):
        super().__init__(cwd, date_pointer, kind='puddle')
        if self.date_pointer.route is False:
            raise Exception ('tried to instantiate Puddle because you called __init__ without a value in DatePointer.route')
        self.route = self.date_pointer.route


    def freeze_myself_to_glacier(self):
        drops_to_freeze=[x for x in self.path.glob('*.json') if x.is_file()]
        try:
            outfile = Glacier(self.cwd, self.date_pointer)
        except OSError:
            # future write handler for partially rendered puddles(maybe a different filename, or move it to a lost+found?)
            return
        with tarfile.open(outfile.filepath, "w:gz") as tar:
                for drop in drops_to_freeze:
                    tar.add(drop, arcname=drop.name.replace(':','-')) # tar doesnt like colons
        logging.debug ('froze {} drops to Glacier at {}'.format(len(drops_to_freeze), outfile.path))
        self.delete_folder()
        return


class Glacier(GenericFolder):

    def __init__(self, cwd, date_pointer):
        super().__init__(cwd, date_pointer, kind='glacier')
        self.route = self.date_pointer.route
        self.exist, self.filepath = self.check_exist()
        try:
            if self.exist == True:
                pass
        except OSError:
            logging.error ('!!error::Glacier::skipping! there is already a Glacier at {}'.format(self.filepath))
        self.url = config.config['glacier_api_url'].format(str(date_pointer.year),
                                                            str(date_pointer.month),
                                                            str(date_pointer.day),
                                                            str(date_pointer.hour),
                                                            date_pointer.route)

    def check_exist(self):
        filepath = self.path / 'glacier_{}.tar.gz'. \
            format(self.date_pointer)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)


class DataStore(GenericStore):

    def __init__(self, cwd):
        super().__init__(cwd, kind='store')

    def make_barrels(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                barrel_date_pointer=DateRoutePointer(date_pointer.timestamp, route)
                pickles = []
                try:
                    folder = Barrel(self.cwd, barrel_date_pointer).path
                    filename = 'pickle_{}_{}.dat'.format(barrel_date_pointer.route, barrel_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    try:

                        # we are running reprocessor
                        if type(route_data) is dict:
                            # route_data = json.dumps(route_data, cls=DecimalEncoder, indent=4)
                            pass
                        else:
                            route_data = route_data.json()
                    except AttributeError: #this means we are running the shipment dumper
                        pass
                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)
                    with open(filepath, "wb") as f:
                        pickle.dump(pickles, f)
                        logging.debug (f'dumped {len(pickles)} pickles to barrel for {route}')
                except KeyError:
                    # future find a way to filter these out to reduce overhead
                    # this is almost always the result of a route that doesn't exist, so why is it in the OBA response?
                    pass
                except json.JSONDecodeError:
                    # this is probably no response?
                    logging.error ('JSONDecodeError: No/Bad API response? - route {}'.format(route))
                    pass
        return

    def scan_barrels(self):
        files = glob('{}/*/*/*/*/*'.format(self.cwd / pathmap['barrel']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        barrels = []
        for d in dirs:
            rt=d.split('/')[-1]
            b = Barrel(self.cwd, self.path_to_DateRoutePointer(d,rt))
            barrels.append(b)
        logging.debug('rescanned::DataStore uid {}::found {} Barrels at {}'.format(self.uid, len(barrels),str(self.cwd / pathmap['barrel'])))
        return barrels

    def list_expired_barrels(self):
        expired_barrels = []
        for barrel in self.scan_barrels():
            bottom_of_hour = DatePointer(datetime.now())
            if (barrel.date_pointer.year, barrel.date_pointer.month,
                barrel.date_pointer.day, barrel.date_pointer.hour) \
                    != \
                    (bottom_of_hour.year, bottom_of_hour.month,
                     bottom_of_hour.day,bottom_of_hour.hour):
                        expired_barrels.append(barrel)
        return sorted(expired_barrels, key=lambda b: b.date_pointer.timestamp)

    def render_barrels(self):
        barrels_to_archive = self.list_expired_barrels()
        if len(barrels_to_archive) == 0:
            logging.debug('no expired barrels to render')
            return
        for barrel in barrels_to_archive:
            barrel.render_myself_to_shipment()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(self.cwd / pathmap['store']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return

    def scan_shipments(self):
        files = glob('{}/*/*/*/*/*'.format(self.cwd / pathmap['shipment']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        shipments = []
        for d in dirs:
            rt=d.split('/')[-1]
            s = Shipment(self.cwd, self.path_to_DateRoutePointer(d,rt))
            s.path = s.path / PurePath (s.route)
            shipments.append(s)
        return shipments

    def make_shipment_indexes(self):

        # 1 populate the shipments list
        self.shipments = self.scan_shipments()

        #  2 sort all the shipments into a dict of lists
        # { 'M15': [Shipment, Shipment, Shipment...],
        #   'Bx44': [Shipment, Shipment, Shipment...]}
        shipments_grouped=defaultdict(list)
        for s in self.shipments:
            shipments_grouped[s.route].append(s)

        for route, shipment_list in shipments_grouped.items():

            # 3 sort each route's shipments by year,month,day,hour
            shipment_list_sorted = sorted(shipment_list, key = lambda i: (i.year,i.month,i.day,i.hour))
            folderpath = self.path / PurePath('shipments/indexes')
            Path(folderpath).mkdir(parents=True, exist_ok=True)

            outfile = folderpath / PurePath(f'shipment_index_{route.upper()}.json')
            Path(folderpath).mkdir(parents=True, exist_ok=True)

            # make the shipment_insert
            shipment_insert = []

            for s in shipment_list_sorted:
                the_pointer = {'route': s.route,
                               'year': s.year,
                               'month': s.month,
                               'day': s.day,
                               'hour': s.hour,
                               'url' : s.url}
                shipment_insert.append(the_pointer)

            json_container = {"route":route.upper(), "shipments": shipment_insert}

            # check if outfile exists, delete it
            if os.path.exists(outfile):
                os.remove(outfile)
            else:
                pass
            with open(outfile, "w") as f:
                json.dump(json_container, f, indent=4)
            logging.debug ('wrote Shipment index for {route} to {outfile}')

        return


class Barrel(GenericFolder):

    def __init__(self, cwd, date_pointer):
        super().__init__(cwd, date_pointer, kind='barrel')
        if self.date_pointer.route is False:
            raise Exception ('tried to instantiate Barrel because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route

    def render_myself_to_shipment(self):
        pickles_to_render=[x for x in self.path.glob('*.dat') if x.is_file()]
        pickle_array = []
        for picklefile in pickles_to_render:
            # make sure the pickle file isnt empty
            if os.path.getsize(picklefile) > 0:
                with open(picklefile, 'rb') as pickle_file:
                    try:
                        barrel = pickle.load(pickle_file)
                    except Exception as e:
                        logging.error ('error {} in {}'.format(e, inspect.stack()[0][3]) )
                        continue
                    for p in barrel:
                        pickle_array.append(p)
        pickle_array.sort(key= lambda i: (i.timestamp, i.trip_id))
        serial_array=[]
        try:
            for p in pickle_array:
                serial_array.append(p.to_serial())
        except TypeError: # empty list
            return
        json_container = dict()
        json_container['buses'] = serial_array

        try:
            outfile = Shipment(self.cwd, self.date_pointer)
        except OSError:
            return

        with open(outfile.filepath, 'w') as f:
            json.dump(json_container, f, indent=4)
        logging.debug ('wrote {} pickles to Shipment at {}'.format(len(pickle_array), outfile.filepath))
        self.delete_folder()
        return

    def count_pickles(self):
        pickles_to_count=[x for x in self.path.glob('*.dat') if x.is_file()]
        pickle_count = 0
        for picklefile in pickles_to_count:
            try:
                with open(picklefile, 'rb') as pickle_file:
                    barrel = pickle.load(pickle_file)
                    pickle_count = pickle_count + len(barrel)
            except Exception as e:
                logging.debug ('error {} in {}'.format(e, inspect.stack()[0][3]) )
                pass
        return pickle_count


class Shipment(GenericFolder):

    def __init__(self, cwd, date_pointer):
        super().__init__(cwd, date_pointer, kind='shipment')
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        self.url = config.config['shipment_api_url'].format(str(date_pointer.year),
                                                            str(date_pointer.month),
                                                            str(date_pointer.day),
                                                            str(date_pointer.hour),
                                                            date_pointer.route)
        self.year, self.month, self.day, self.hour = date_pointer.year, \
                                                     date_pointer.month,\
                                                     date_pointer.day, \
                                                     date_pointer.hour
        try:
            if self.exist == True:
                pass
        except OSError:
            logging.error ('!!error::Shipment::skipping! there is already a Shipment at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'shipment_{}.json'.\
            format(self.date_pointer)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    def load_file(self):
        if self.check_exist()[0] is True:
            with open(self.filepath, "r") as f:
                return f.read()
        else:
            response = {'type': 'Shipment',
                        'Status': 'False',
                        'Request': {
                            "Year": str(self.year),
                            "Month": str(self.month),
                            "Day": str(self.day),
                            "Hour":str(self.hour),
                            "Route": self.route
                        }
                    }

            return json.dumps(response, indent=4)

    def backup_file(self):
        backup_filepath = self.filepath.with_suffix('.bak')
        copy(self.filepath, backup_filepath) # n.b. will overwrite
        logging.warning(f"backed up shipment {self.filepath} to {self.filepath.with_suffix('.bak')}")
        return

    def restore_backup_file(self):
        backup_filepath = self.filepath.with_suffix('.bak')
        copy(backup_filepath, self.filepath) # n.b. will overwrite
        logging.warning(f"restored backup shipment {self.filepath.with_suffix('.bak')} to {self.filepath} ")
        return

    def to_FeatureCollection(self):

        if self.check_exist()[0] is True:

            geojson = {'type': 'FeatureCollection', 'features': []}
            shipment = json.loads(self.load_file())
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


        else:
            response = { "type": "Shipment as GeoJSON",
                         "status": False,
                         "Request": {
                             "Year": str(self.year),
                             "Month": str(self.month),
                             "Day": str(self.day),
                             "Hour":str(self.hour),
                             "Route": self.route
                         }
                         }

            return response


def count_buses(self):
        data = self.load_file()
        return len(data['buses'])


class BusObservation():

    def parse_buses(self, monitored_vehicle_journey):
        lookup = {'route_long':['LineRef'],
                  'direction':['DirectionRef'],
                  'service_date': ['FramedVehicleJourneyRef', 'DataFrameRef'],
                  'trip_id': ['FramedVehicleJourneyRef', 'DatedVehicleJourneyRef'],
                  'gtfs_shape_id': ['JourneyPatternRef'],
                  'route_short': ['PublishedLineName'],
                  'agency': ['OperatorRef'],
                  'origin_id':['OriginRef'],
                  'destination_id':['DestinationRef'],
                  'destination_name':['DestinationName'],
                  'next_stop_id': ['MonitoredCall','StopPointRef'], #<-- GTFS of next stop
                  'next_stop_eta': ['MonitoredCall','ExpectedArrivalTime'], # <-- eta to next stop
                  'next_stop_d_along_route': ['MonitoredCall','Extensions','Distances','CallDistanceAlongRoute'], # <-- The distance of the stop from the beginning of the trip/route
                  'next_stop_d': ['MonitoredCall','Extensions','Distances','DistanceFromCall'], # <-- The distance of the stop from the beginning of the trip/route
                  'alert': ['SituationRef', 'SituationSimpleRef'],
                  'lat':['VehicleLocation','Latitude'],
                  'lon':['VehicleLocation','Longitude'],
                  'bearing': ['Bearing'],
                  'progress_rate': ['ProgressRate'],
                  'progress_status': ['ProgressStatus'],
                  'occupancy': ['Occupancy'],
                  'vehicle_id':['VehicleRef'], #use this to lookup if articulated or not https://en.wikipedia.org/wiki/MTA_Regional_Bus_Operations_bus_fleet
                  'gtfs_block_id':['BlockRef'],
                  'passenger_count': ['MonitoredCall', 'Extensions','Capacities','EstimatedPassengerCount']
                  }
        buses = []
        try:
            setattr(self,'timestamp',parser.isoparse(monitored_vehicle_journey['RecordedAtTime']))
            for k,v in lookup.items():
                try:
                    if len(v) == 2:
                        val = monitored_vehicle_journey['MonitoredVehicleJourney'][v[0]][v[1]]
                        setattr(self, k, val)
                    elif len(v) == 4:
                        val = monitored_vehicle_journey['MonitoredVehicleJourney'][v[0]][v[1]][v[2]][v[3]]
                        setattr(self, k, val)
                    else:
                        val = monitored_vehicle_journey['MonitoredVehicleJourney'][v[0]]
                        setattr(self, k, val)
                except LookupError:
                    pass
                except Exception as e:
                    pass
            buses.append(self)
        except KeyError: #no VehicleActivity?
            pass
        return buses


    def to_serial(self):

        def serialize(obj):
            # Recursively walk object's hierarchy.

            if isinstance(obj, (bool, int, float)):
                return obj

            elif isinstance(obj, dict):
                obj = obj.copy()
                for key in obj:
                    obj[key] = serialize(obj[key])
                return obj

            elif isinstance(obj, list):
                return [serialize(item) for item in obj]

            elif isinstance(obj, tuple):
                return tuple(serialize([item for item in obj]))

            elif hasattr(obj, '__dict__'):
                return serialize(obj.__dict__)

            else:
                # return repr(obj) # Don't know how to handle, convert to string
                return str(obj) # avoids single quotes around strings

        # return json.dumps(serialize(self))
        return serialize(self)

    def __repr__(self):
        output = ''
        # output = None
        for var, val in vars(self).items():
            if var == '_sa_instance_state':
                continue
            else:
                output = output + ('{} {} '.format(var,val))
        return output

    def __init__(self,route,monitored_vehicle_journey):
        self.route = route
        self.parse_buses(monitored_vehicle_journey)

    # alternate constructor
    # # add this https://stackoverflow.com/questions/6383914/is-there-a-way-to-instantiate-a-class-without-calling-init/6384228#6384228
    # call this class method with for row in results: BusObservation.Load(row)
    @classmethod
    def Load(cls, table_row):

        row_dict = \
        {
            "MonitoredVehicleJourney": {
                "LineRef": f'{table_row.route_long}',
                "DirectionRef": f'{table_row.direction}',
                "FramedVehicleJourneyRef": {
                    "DataFrameRef": f'{table_row.service_date}',
                    "DatedVehicleJourneyRef": f'{table_row.trip_id}'
                },
                "JourneyPatternRef": f'{table_row.gtfs_shape_id}',
                "PublishedLineName": f'{table_row.route_short}',
                "OperatorRef": f'{table_row.agency}',
                "OriginRef": f'{table_row.origin_id}',
                "DestinationName": f'{table_row.destination_name}',
                "OriginAimedDepartureTime": "2021-07-13T17:57:00.000-04:00",
                "SituationRef": [], # how to parse -- 'alert': ['SituationRef', 'SituationSimpleRef']
                "VehicleLocation": {
                    "Longitude": f'{table_row.lon}',
                    "Latitude": f'{table_row.lat}'
                },
                "Bearing": f'{table_row.bearing}',
                "ProgressRate": f'{table_row.progress_rate}',
                "ProgressStatus": f'{table_row.progress_status}',
                "BlockRef": f'{table_row.gtfs_block_id}',
                "VehicleRef": f'{table_row.vehicle_id}',
                "MonitoredCall": {
                    "Extensions": {
                        "Capacities": {
                            "EstimatedPassengerCount": f'{table_row.passenger_count}',
                            "DistanceFromCall": f'{table_row.next_stop_d}',
                            "CallDistanceAlongRoute": f'{table_row.next_stop_d_along_route}'
                        }
                    }
                }
            },
            "RecordedAtTime": f'{table_row.timestamp.isoformat()}'
        }

        return cls(
            table_row.route_short,
            row_dict
        )

# with help from https://realpython.com/introduction-to-mongodb-and-python/
class MongoLake():

    def __init__(self):
        self.uid = uuid4().hex


    def store_feeds(self, feeds):
        with MongoClient(host="localhost", port=27017) as client: # bug will have to configure hostname whether we are development or production
            db = client.nycbuswatcher # db name is 'buswatcher'
            response_db = db["siri_archive"] # raw responses
            buses_db = db["buses"] # only the ['MonitoredVehicleJourney'] dicts

            # iterate over each route
            for route_report in feeds:
                for route_id, response in route_report.items():

                    # dump the response to archive
                    response_db.insert_one(response.json())

                    # make a dict with the response
                    response_json = response.json()

                    # bug this isnt parsing properly all the time
                    """
                    DEBUG:root:SIRI API response for MTA NYCT_SIM6 was 4 buses.
                    DEBUG:root:SIRI API response for MTA NYCT_SIM6 was NO VEHICLE ACTIVITY.
                    DEBUG:root:SIRI API response for MTA NYCT_SIM8 was 5 buses.
                    DEBUG:root:SIRI API response for MTA NYCT_SIM8 was NO VEHICLE ACTIVITY.
                    DEBUG:root:SIRI API response for MTA NYCT_B47 was 12 buses.
                    DEBUG:root:SIRI API response for MTA NYCT_B47 was NO VEHICLE ACTIVITY.
                    """

                    # parse the response and dump each monitored vehicle journey to collection 'buses'
                    try:
                        error_condition = response_json['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['ErrorCondition']
                        logging.debug(f"SIRI API response for {route_id} was {error_condition['OtherError']['ErrorText']}")
                        continue
                    except:

                        # todo find smarter way to catch empty VehicleActivity—any error here could cause except to fire
                        try:
                            vehicle_activity = response_json['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
                            logging.debug(f"SIRI API response for {route_id} was {len(vehicle_activity)} buses.")

                            buses=[]
                            for v in vehicle_activity:
                                v['RecordedAtTime'] = parser.isoparse(v['RecordedAtTime'])
                                # v['RecordedAtTimeAsDatetime'] = parser.isoparse(v['RecordedAtTime'])
                                logging.debug(f"Route {v['MonitoredVehicleJourney']['LineRef']} Bus {v['MonitoredVehicleJourney']['VehicleRef'] } recorded at {v['RecordedAtTimeAsDatetime']}")
                                buses.append(v)
                            buses_db.insert_many(buses)

                        except:
                            logging.debug(f'SIRI API response for {route_id} was NO VEHICLE ACTIVITY.')
                            continue

        return

    # query db for all buses on a route in history
    def get_route_history(self, passed_route):

        with MongoClient(host="localhost", port=27017) as client: # bug will have to configure hostname whether we are development or production
            db = client.nycbuswatcher # db name is 'buswatcher'
            Collection = db["buses"] # collection name is 'buses'

            lineref_prefix = "MTA NYCT_"
            lineref = lineref_prefix + passed_route

            query = f'{{ "Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity.MonitoredVehicleJourney.LineRef" : "{lineref}" }}'

            # todo rewrite this as a generator
            # concatenate all the VehicleMonitoring reports with their timestamps
            buses = []

            for route_report in Collection.find(json.loads(query)):

                for bus in route_report['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    monitored_vehicle_journey = bus['MonitoredVehicleJourney']
                    monitored_vehicle_journey['RecordedAtTime'] = bus['RecordedAtTime']
                    buses.append(monitored_vehicle_journey)

            # make a geojson out of it
            response = { "scope": "history",
                         "query": {
                             "lineref": lineref
                         },
                         "results": buses
                         }
            return json.dumps(response, indent=4)

    # query db for all buses on a route for a specific hour (like the old Shipment)
    def get_dateroute_query(self, date_route_pointer):

        with MongoClient(host="localhost", port=27017) as client: # bug will have to configure hostname whether we are development or production
            db = client.nycbuswatcher # db name is 'buswatcher'
            Collection = db["buses"] # collection name is 'buses'

            lineref_prefix = "MTA NYCT_"
            lineref = lineref_prefix + date_route_pointer.route

            # todo rewrite this as a generator?
            # todo add a date query
            # todo https://medium.com/nerd-for-tech/how-to-prepare-a-python-date-object-to-be-inserted-into-mongodb-and-run-queries-by-dates-and-range-bc0da03ea0b2
            query = f'{{ "Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity.MonitoredVehicleJourney.LineRef" : "{lineref}" }}'

            # concatenate all the VehicleMonitoring reports with their timestamps
            buses = []

            for route_report in Collection.find(json.loads(query)):

                for bus in route_report['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    monitored_vehicle_journey = bus['MonitoredVehicleJourney']
                    monitored_vehicle_journey['RecordedAtTime'] = bus['RecordedAtTime']
                    buses.append(monitored_vehicle_journey)



            # make a geojson out of it
            response = { "scope": "hour",
                         "query": {
                             date_route_pointer
                         },
                         "results": buses
                         }

            return json.dumps(response, indent=4)


