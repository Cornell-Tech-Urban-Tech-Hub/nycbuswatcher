from datetime import date, datetime, timedelta
from dateutil import parser


#--------------- HELPER FUNCTIONS ---------------

def iso_to_datestr(timestamp):
    return str(timestamp).split('T',maxsplit=1)[0]

def add_1hr_to_iso(interval_start):
    one_hr = timedelta(hours=1)
    interval_end = parser.isoparse(interval_start.replace(" ", "+", 1)) + one_hr
    return interval_end.isoformat()

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