from datetime import date, datetime, timedelta
from dateutil import parser


#--------------- HELPER FUNCTIONS ---------------

def unpack_query_results(query):
    return [dict(zip(tuple(query.keys()), i)) for i in query.cursor]

def sparse_unpack_for_livemap(query):
    unpacked = [dict(zip(tuple(query.keys()), i)) for i in query.cursor]
    sparse_results = []
    for row in unpacked:
        sparse_results.append('something')
    return unpacked

def iso_to_datestr(timestamp):
    return str(timestamp).split('T',maxsplit=1)[0]

def add_1hr_to_iso(interval_start):
    one_hr = timedelta(hours=1)
    interval_end = parser.isoparse(interval_start.replace(" ", "+", 1)) + one_hr
    return interval_end.isoformat()

def query_builder(parameters):
    query_suffix = ''
    for field, value in parameters.items():
        if field == 'output':
            continue
        elif field == 'start':
            query_suffix = query_suffix + '{} >= "{}" AND {} >= "{}" AND '\
                .format('service_date',
                        iso_to_datestr(value),
                        'timestamp',
                        parser.isoparse(value.replace(" ", "+", 1))
                        )
            interval_start = value
            continue

        elif field == 'end':

            # if request is for more than one hour, truncate it to 1 hour
            interval_end = value
            interval_length = parser.isoparse(interval_end .replace(" ", "+", 1)) - \
                              parser.isoparse(interval_start.replace(" ", "+", 1))
            if interval_length > timedelta(hours=1):
                value = add_1hr_to_iso(interval_start)

            else:
                pass
            query_suffix = query_suffix + '{} <= "{}" AND {} < "{}" AND '\
                .format('service_date',
                        iso_to_datestr(value),
                        'timestamp',
                        parser.isoparse(value.replace(" ", "+", 1))
                        )

            continue
        elif field == 'route_short':
            query_suffix = query_suffix + '{} = "{}" AND '.format('route_short', value)
            continue
        else:
            query_suffix = query_suffix + '{} = "{}" AND '.format(field,value)
    query_suffix=query_suffix[:-4] # strip tailing ' AND'
    return query_suffix

def results_to_FeatureCollection(results):
    geojson = {'type': 'FeatureCollection', 'features': []}
    for row in results['observations']:
        feature = {'type': 'Feature',
                   'properties': {},
                   'geometry': {'type': 'Point',
                                'coordinates': []}}
        feature['geometry']['coordinates'] = [row['lon'], row['lat']]
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                v = v.isoformat()
            feature['properties'][k] = v
        geojson['features'].append(feature)
    return geojson