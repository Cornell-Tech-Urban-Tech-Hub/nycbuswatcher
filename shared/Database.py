from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Date, DateTime, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

import datetime

# v1.0 to v1.11 manual migration
# ALTER TABLE buses ADD(passenger_count varchar(31));
#

# v1.11 to v1.2 manual migration
# started 2021-04-18 11:14am
# ALTER TABLE buses
# ADD COLUMN next_stop_id varchar(63),
# ADD COLUMN next_stop_eta varchar(63),
# ADD COLUMN next_stop_d_along_route float,
# ADD COLUMN next_stop_d float;



Base = declarative_base()

def create_table(db_url):
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)

def get_db_url(dbuser,dbpassword,dbhost,dbport,dbname):
    return 'mysql://{}:{}@{}:{}/{}'.format(dbuser,dbpassword,dbhost,dbport,dbname)

def get_session(dbuser,dbpassword,dbhost,dbport,dbname):
    engine = create_engine(get_db_url(dbuser,dbpassword,dbhost,dbport,dbname), echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def parse_buses(route, data, db_url):
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
        timestamp = data['Siri']['ServiceDelivery']['ResponseTimestamp']
        for b in data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
            bus = BusObservation(route,timestamp)
            for k,v in lookup.items():
                try:
                    if len(v) == 2:
                        val = b['MonitoredVehicleJourney'][v[0]][v[1]]
                        setattr(bus, k, val)
                    elif len(v) == 4:
                        val = b['MonitoredVehicleJourney'][v[0]][v[1]][v[2]][v[3]]
                        setattr(bus, k, val)
                    else:
                        val = b['MonitoredVehicleJourney'][v[0]]
                        setattr(bus, k, val)
                except LookupError:
                    pass
                except Exception as e:
                    pass
            buses.append(bus)
    except KeyError: #no VehicleActivity?
        pass
    return buses


class BusObservation(Base):
    __tablename__ = "buses"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    route_simple=Column(String(31)) #this is the route name passed through from the command line, may or may not match route_short
    route_long=Column(String(127))
    direction=Column(String(1))
    service_date=Column(String(31)) #future check inputs and convert to Date
    trip_id=Column(String(63))
    gtfs_shape_id=Column(String(31))
    route_short=Column(String(31))
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

    def __init__(self,route,timestamp):
        self.route_simple = route
        self.timestamp = datetime.datetime.fromisoformat(timestamp)