from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Date, DateTime, Integer, String, Float, Index
from sqlalchemy.ext.declarative import declarative_base

import datetime
from fnmatch import fnmatch

import os
import dateutil.parser as parser

###### DATABASE INIT ######

def get_session(db_url):
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def get_db_url(dest, daily_filename):
    if dest == 'sqlite':
        return f'sqlite:///archives/{daily_filename}.sqlite3' #bug
    elif dest == 'mysql':
        return 'mysql://nycbuswatcher:bustime@localhost:3306/buses'  #bug
    # todo w/in the docker stack
    # return 'mysql://{}:{}@{}:{}/{}'.format('reprocessor', 'reprocessor', 'db', '3306', 'reprocessor')

def create_table(db_url):
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)

###### HELPERS ######

def get_daily_filelist(path):
    daily_filelist=[]
    include_list = ['daily*.gz']
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            if any(fnmatch(filename, pattern) for pattern in include_list):
                daily_filelist.append(filename)
    sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
    return sorted_daily_filelist


def parse_bus(bus,server_timestamp):

    lookup = {'route_long':['LineRef'],
              'direction':['DirectionRef'],
              'service_date': ['FramedVehicleJourneyRef', 'DataFrameRef'],
              'trip_id': ['FramedVehicleJourneyRef', 'DatedVehicleJourneyRef'],
              'gtfs_shape_id': ['JourneyPatternRef'],
              'route_simple': ['PublishedLineName'],
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


    bus_observation = BusObservation(server_timestamp)

    setattr(bus_observation,'timestamp',parser.isoparse(bus['RecordedAtTime']))

    # todo optimize me -- some kind of lookup table for the vals—tuples or lists that can be joined back up to determine val
    for k, v in lookup.items():
        try:
            if len(v) == 2:
                val = bus['MonitoredVehicleJourney'][v[0]][v[1]]
            elif len(v) == 4:
                val = bus['MonitoredVehicleJourney'][v[0]][v[1]][v[2]][v[3]]
            else:
                val = bus['MonitoredVehicleJourney'][v[0]]
            setattr(bus_observation, k, val)
        except:
            pass

    return bus_observation


def parse_response(siri_response):
    buses = []
    try:
        # timestamp=siri_response['ServiceDelivery']['ResponseTimestamp']
        vehicleActivity=siri_response['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']

        # # this works
        # for bus in vehicleActivity:
        # 	bus_observation = parse_bus(bus,timestamp)
        # 	buses.append(bus_observation)

        server_timestamp = siri_response['ServiceDelivery']['ResponseTimestamp']

        # and so does this, probably faster
        buses = [ parse_bus(bus,server_timestamp) for bus in vehicleActivity]

    except KeyError: #no VehicleActivity?
        pass
    return buses # returns a list of BusObservation objects



###### OBJECT MODEL ######

Base = declarative_base()

class BusObservation(Base):
    __tablename__ = "buses_reprocessed"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True) # is now RecordedAtTime from SIRI
    server_timestamp = Column(DateTime)
    route_simple=Column(String(31))
    route_long=Column(String(127))
    direction=Column(String(1))
    service_date=Column(String(31), index=True) #future check inputs and convert to Date
    trip_id=Column(String(63), index=True)
    gtfs_shape_id=Column(String(31))
    route_short=Column(String(31), index=True)
    agency=Column(String(31))
    origin_id=Column(String(31))
    destination_id=Column(String(31))
    destination_name=Column(String(127))
    next_stop_id=Column(String(63))
    next_stop_eta=Column(String(63)) #future change to datetime?
    next_stop_d_along_route=Column(Float)
    next_stop_d=Column(Float)
    alert=Column(String(127))
    lat=Column(Float)
    lon=Column(Float)
    bearing=Column(Float)
    progress_rate=Column(String(31))
    progress_status=Column(String(31))
    occupancy=Column(String(31))
    vehicle_id=Column(String(31))
    gtfs_block_id=Column(String(63))
    passenger_count=Column(String(31))

    def __repr__(self):
        output = ''
        for var, val in vars(self).items():
            if var == '_sa_instance_state':
                continue
            else:
                output = output + ('{} {} '.format(var,val))
        return output

    def __init__(self,server_timestamp):
        self.server_timestamp = datetime.datetime.fromisoformat(server_timestamp)

Index('index_servicedate_routeshort', BusObservation.service_date, BusObservation.route_short)
Index('index_routeshort_timestamp', BusObservation.route_short, BusObservation.timestamp)