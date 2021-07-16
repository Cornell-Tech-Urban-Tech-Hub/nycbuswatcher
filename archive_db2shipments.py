
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
store = DataStore(Path.cwd())

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
merge_tables_query = \
"""
CREATE TABLE buses_reprocessed_all
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
	conn.execute("DROP TABLE buses_reprocessed_all") #bug this is dangerous
	conn.execute(merge_tables_query)
print (f'merged {len(tablespace)} tables into table:buses_reprocessed_all')


# 2. then iterate over dates from 10-15-2020 to 04-30-2021 through that, pulling records that match from the right tables
# make a shipment for each
# concatente them into a shipment file and dump it

for date in datelist:
	date_str=date.timestamp.strftime('%Y-%m-%d')
	#todo might be more consistent to have both conditions query against timestamp
	query = f"""SELECT * FROM buses_reprocessed_all WHERE service_date='{date_str}' AND HOUR(timestamp) = {date.hour}"""
	print(query)
				
	with engine.connect() as conn:
		print('parsing buses')
		results = conn.execute(query)
		for row in results:
			bus = BusObservation.Load(row)
			print (bus)

# make barrels

# todo dump each route-hours buses together

