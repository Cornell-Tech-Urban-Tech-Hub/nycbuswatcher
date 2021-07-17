from collections import defaultdict
from sqlalchemy import (Column, Date, Integer, MetaData, Table, String, create_engine, select, insert)

# database setup
# define the lookup table
metadata = MetaData()
lookup = Table(
	'buses_lookup', metadata,
	Column('origin_table', String(length=127), nullable=False, index=True),
	Column('id', Integer),
	Column('service_date', Date),
)

import argparse
parser = argparse.ArgumentParser(description='NYCbuswatcher shipment dumper, dumps fromv1 database table to shipment json files')
# parser.add_argument('datadir', type=str, help="Path to dump to.")
parser.add_argument('-l', '--localhost', action='store_true', help="Run on localhost (otherwise uses docker container hostname 'db')")
args = parser.parse_args()

if args.localhost == True:
	engine = create_engine('mysql://nycbuswatcher:bustime@localhost:3306/buses')
else:
	engine = create_engine('mysql://nycbuswatcher:bustime@db:3306/buses')
metadata.create_all(bind=engine)


# load the DataStore
from common.Models import *

#create datalist as list of DatePointer objects by hour
import datetime as dt
date1 = '2020-10-16'
date2 = '2021-05-31'
datelist = []
start = dt.datetime.strptime(date1, '%Y-%m-%d')
end = dt.datetime.strptime(date2, '%Y-%m-%d')
step = dt.timedelta(days=1)
while start <= end:
	for hr in range(24):
		datelist.append(
			DatePointer(
				start+dt.timedelta(hours=hr)
			)
		)
	start += step

# define tablespace
tablespace=['buses_reprocessed_2020_10',
			'buses_reprocessed_2020_11',
			'buses_reprocessed_2020_12_a',
			'buses_reprocessed_2020_12_b',
			'buses_reprocessed_2021_01_a',
			'buses_reprocessed_2021_01_b',
			'buses_reprocessed_2021_02_a',
			'buses_reprocessed_2021_02_b',
			'buses_reprocessed_2021_03_a',
			'buses_reprocessed_2021_03_b',
			'buses_reprocessed_2021_04_a',
			'buses_reprocessed_2021_04_b',
			'buses_reprocessed_dumped_2021_05',
			'buses_reprocessed_dumped_2021_06'
			]

# merge all the tables with one query

# n.b. this is faster than using a lookup table! but the ids are no longer unique

# already ran this query on the server by hand
# nohup docker exec -i nycbuswatcher_db_1 mysql -unycbuswatcher -pbustime buses -e "CREATE TABLE buses_reprocessed_merged AS SELECT * FROM buses_reprocessed_2020_10 UNION SELECT * FROM buses_reprocessed_2020_11 UNION SELECT * FROM buses_reprocessed_2020_12_a UNION SELECT * FROM buses_reprocessed_2020_12_b UNION SELECT * FROM buses_reprocessed_2021_01_a UNION SELECT * FROM buses_reprocessed_2021_01_b UNION SELECT * FROM buses_reprocessed_2021_02_a UNION SELECT * FROM buses_reprocessed_2021_02_b UNION SELECT * FROM buses_reprocessed_2021_03_a UNION SELECT * FROM buses_reprocessed_2021_03_b UNION SELECT * FROM buses_reprocessed_2021_04_a UNION SELECT * FROM buses_reprocessed_2021_04_b UNION SELECT * FROM buses_reprocessed_copied_2021_05 UNION SELECT * FROM buses_reprocessed_copied_2021_06;" &


merge_tables_query = \
"""
CREATE TABLE IF NOT EXISTS buses_reprocessed_merged
AS
SELECT * FROM buses_reprocessed_2020_10 UNION
SELECT * FROM buses_reprocessed_2020_11 UNION
SELECT * FROM buses_reprocessed_2020_12_a UNION
SELECT * FROM buses_reprocessed_2020_12_b UNION
SELECT * FROM buses_reprocessed_2021_01_a UNION
SELECT * FROM buses_reprocessed_2021_01_b UNION
SELECT * FROM buses_reprocessed_2021_02_a UNION
SELECT * FROM buses_reprocessed_2021_02_b UNION
SELECT * FROM buses_reprocessed_2021_03_a UNION
SELECT * FROM buses_reprocessed_2021_03_b UNION
SELECT * FROM buses_reprocessed_2021_04_a UNION
SELECT * FROM buses_reprocessed_2021_04_b UNION
SELECT * FROM buses_reprocessed_dumped_2021_05 UNION
SELECT * FROM buses_reprocessed_dumped_2021_06
"""

with engine.connect() as conn:
	if args.localhost == True:
		conn.execute('DROP TABLE buses_reprocessed_merged')
		conn.execute(merge_tables_query)
		print (f'merged {len(tablespace)} tables into table:buses_reprocessed_merged')
	else:
		print('running on cornellbus server, did not attempt to merge all tables in table:buses_reprocessed_merged')


# create a datastore in cwd
store = DataStore(Path.cwd() / 'tmp')

# 2. then iterate over dates from 10-15-2020 to 04-30-2021 through that, pulling records that match from the right tables
# make a shipment for each
# concatenate them into a shipment file and dump it

for date in datelist:
	date_str=date.timestamp.strftime('%Y-%m-%d')
	#todo might be more consistent to have both conditions query against timestamp
	query = f"""SELECT * FROM buses_reprocessed_merged WHERE service_date='{date_str}' AND HOUR(timestamp) = {date.hour}"""

	with engine.connect() as conn:
		results = conn.execute(query)

		import itertools
		rows = [x for x in results]

		# iterate over the groups
		for route_long, route_group in itertools.groupby(rows, lambda x: x.route_long):

			data = defaultdict()
			data['VehicleActivity'] = []

			for bus in route_group:
				# process the route_group into a feed and feed it to store.make_barrels

				monitored_vehicle_journey = \
					{
						"MonitoredVehicleJourney": {
							"LineRef": f'{bus.route_long}',
							"DirectionRef": f'{bus.direction}',
							"FramedVehicleJourneyRef": {
								"DataFrameRef": f'{bus.service_date}',
								"DatedVehicleJourneyRef": f'{bus.trip_id}'
							},
							"JourneyPatternRef": f'{bus.gtfs_shape_id}',
							"PublishedLineName": f'{bus.route_short}',
							"OperatorRef": f'{bus.agency}',
							"OriginRef": f'{bus.origin_id}',
							"DestinationName": f'{bus.destination_name}',
							"OriginAimedDepartureTime": "2021-07-13T17:57:00.000-04:00",
							"SituationRef": [], #bug how to parse -- 'alert': ['SituationRef', 'SituationSimpleRef']
							"VehicleLocation": {
								"Longitude": f'{bus.lon}',
								"Latitude": f'{bus.lat}'
							},
							"Bearing": f'{bus.bearing}',
							"ProgressRate": f'{bus.progress_rate}',
							"ProgressStatus": f'{bus.progress_status}',
							"BlockRef": f'{bus.gtfs_block_id}',
							"VehicleRef": f'{bus.vehicle_id}',
							"MonitoredCall": {
								"Extensions": {
									"Capacities": {
										"EstimatedPassengerCount": f'{bus.passenger_count}',
										"DistanceFromCall": f'{bus.next_stop_d}',
										"CallDistanceAlongRoute": f'{bus.next_stop_d_along_route}'
									}
								}
							}
						},
						"RecordedAtTime": f'{bus.timestamp.isoformat()}' #bug compare with standard shipment
					}

				data['VehicleActivity'].append(monitored_vehicle_journey)

			fake_response =  {"Siri" :{
								"ServiceDelivery" : {
									"VehicleMonitoringDelivery" : []
													}
									}
							}


			fake_response['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'].append(data)

			feeds = [{ route_long: fake_response} ]
			store.make_barrels(feeds, date)

	store.render_barrels()
