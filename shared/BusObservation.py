import dateutil.parser
import json

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
            setattr(self,'timestamp',dateutil.parser.isoparse(monitored_vehicle_journey['RecordedAtTime']))
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


#todo move this to the main script, so we feed one monitored_vehicle_journey to the class to make itself


# # these will all be definied dynamically when we do the parsing above
#
# id: str
# timestamp: datetime
#
# server_timestamp =
# route_simple=
# route_long=
# direction=Column(String(1))
# service_date=Column(String(31), index=True) #future check inputs and convert to Date
# trip_id=Column(String(63), index=True)
# gtfs_shape_id=Column(String(31))
# route_short=Column(String(31), index=True)
# agency=Column(String(31))
# origin_id=Column(String(31))
# destination_id=Column(String(31))
# destination_name=Column(String(127))
# next_stop_id=Column(String(63))
# next_stop_eta=Column(String(63)) #future change to datetime?
# next_stop_d_along_route=Column(Float)
# next_stop_d=Column(Float)
# alert=Column(String(127))
# lat=Column(Float)
# lon=Column(Float)
# bearing=Column(Float)
# progress_rate=Column(String(31))
# progress_status=Column(String(31))
# occupancy=Column(String(31))
# vehicle_id=Column(String(31))
# gtfs_block_id=Column(String(63))
# passenger_count=Column(String(31))
