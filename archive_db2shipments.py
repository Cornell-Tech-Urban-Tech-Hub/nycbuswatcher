import datetime as dt
from sqlalchemy import (
	Column, Date, Integer, MetaData, Table, Text, create_engine, select, insert)

# database setup
# define the lookup table
metadata = MetaData()
lookup = Table(
	'buses_lookup', metadata,
	Column('origin_table', Text, nullable=False, index=True),
	Column('id', Integer),
	Column('service_date', Date),
)
engine = create_engine('mysql://nycbuswatcher:bustime@db:3306/buses')
metadata.create_all(bind=engine)


# load the DataStore
from common.Models import *
store = DataStore(Path.cwd())

# create datelist
date1 = '2020-10-16'
date2 = '2021-05-31'
datelist = []
start = dt.datetime.strptime(date1, '%Y-%m-%d')
end = dt.datetime.strptime(date2, '%Y-%m-%d')
step = dt.timedelta(days=1)
while start <= end:
	datelist.append(start.date())
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
			'buses_dumped_2021_05',
			'buses_dumped_2021_06'
			]



# 1. loop over all the tables and create a new lookup table with the following columns
# tablename, record_id, timestamp

for table in tablespace:

	query = f'SELECT "{table}", id, service_date FROM {table}'

	with engine.connect() as conn:
		results = conn.execute(query)

		for row in results:
			conn.execute(
				insert(lookup).values(
					origin_table=table, id=row.id, service_date=row.service_date
				)
			)

# 2. then iterate over dates from 10-15-2020 to 04-30-2021 through that, pulling records that match from the right tables, concatente them into a shipment file and dump oit







make_single_table_sql="""
# use a union query https://support.microsoft.com/en-us/office/use-a-union-query-to-combine-multiple-queries-into-a-single-result-1f772ec0-cc73-474d-ab10-ad0a75541c6e

CREATE_TABLE buses_reprocessed_2020_10_thru_2021_04

AS

SELECT *
FROM buses_reprocessed_2020_10

UNION

SELECT *
FROM buses_reprocessed_2020_11

UNION

SELECT *
FROM buses_reprocessed_2020_12_a

UNION

SELECT *
FROM buses_reprocessed_2020_12_b

UNION

SELECT *
FROM buses_reprocessed_2021_01_a

UNION

SELECT *
FROM buses_reprocessed_2021_01_b

UNION

SELECT *
FROM buses_reprocessed_2021_02_a

UNION

SELECT *
FROM buses_reprocessed_2021_02_b

UNION

SELECT *
FROM buses_reprocessed_2021_03_a

UNION

SELECT *
FROM buses_reprocessed_2021_03_b

UNION

SELECT *
FROM buses_reprocessed_2021_04_a

UNION

SELECT *
FROM buses_reprocessed_2021_04_b

;

"""
