import os
import datetime as dt
import requests
import geopandas as gpd
import pickle
from shared.config import config

def get_OBA_routelist():
    url = "http://bustime.mta.info/api/where/routes-for-agency/MTA%20NYCT.json?key=" + os.getenv("API_KEY")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 503: # response is bad, so go to exception and load the pickle
            raise Exception(503, "503 error code fetching route definitions. OneBusAway API probably overloaded.")
        else: # response is good, so save it to pickle and proceed
            with open(('data/routes-for-agency.pickle'), "wb") as pickle_file:
                pickle.dump(response,pickle_file)
    except Exception as e: # response is bad, so load the last good pickle
        with open(('data/routes-for-agency.pickle'), "rb") as pickle_file:
            response = pickle.load(pickle_file)
        print("Route URLs loaded from pickle cache.")
    finally:
        routes = response.json()
        now=dt.datetime.now()
        # print('Found {} routes at {}.'.format(len(routes['data']['list']),now.strftime("%Y-%m-%d %H:%M:%S")))

    return routes

def get_SIRI_request_urlpaths():
    SIRI_request_urlpaths = []
    routes=get_OBA_routelist()

    for route in routes['data']['list']:
        SIRI_request_urlpaths.append({route['id']:"/api/siri/vehicle-monitoring.json?key={}&VehicleMonitoringDetailLevel=calls&LineRef={}".format(os.getenv("API_KEY"), route['id'])})

    return SIRI_request_urlpaths


def num_buses(feeds):
    num_buses=0
    for route_report in feeds:
        for route_id,route_data in route_report.items():
            try:
                route_data = route_data.json()
                for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    num_buses = num_buses + 1
            except: # no vehicle activity?
                pass
    return num_buses



# def get_buses_gdf():
#
#     # # livemap version
#     # api_url = config.config['api_livemap_url']
#
#     # playback an hour version
#     end=dt.datetime.today().isoformat()
#     start=(dt.datetime.today()-dt.timedelta(hours=1)).isoformat() # this usually times out gunicorn if set to 24 hours
#     api_url = config.config['api_base_url'] + ( 'buses?start='+ start) + ( '&end='+ end )
#
#     # get the gdf
#     buses_gdf = remoteGeoJSONToGDF(api_url)
#
#     # clean up the passenger_count column
#     buses_gdf['passenger_count'] = buses_gdf['passenger_count'].astype('float')
#     buses_gdf['passenger_count'] = buses_gdf['passenger_count'].fillna(0)
#
#     #  make buses_gdf['timestamp'] format prettier?
#
#     return buses_gdf
#
# def remoteGeoJSONToGDF(url, display = False):
#     """https://maptastik.medium.com/remote-geojson-to-geodataframe-19c3c1282a64
#     """
#     r = requests.get(url)
#     data = r.json()
#     gdf = gpd.GeoDataFrame.from_features(data['features'])
#     if display:
#         gdf.plot()
#     return gdf
#
# def remote_geojson_to_coords(geojson_url):
#     # after https://community.plotly.com/t/how-to-plot-a-scattermapbox-with-pandas/33393
#
#     data=requests.get(geojson_url).json()
#     points = []
#
#     for  feature in data['features']:
#         if feature['geometry']['type'] == 'Polygon':
#             points.extend(feature['geometry']['coordinates'][0])
#             points.append([None, None]) # mark the end of a polygon
#
#         elif feature['geometry']['type'] == 'MultiPolygon':
#             for polyg in feature['geometry']['coordinates']:
#                 points.extend(polyg[0])
#                 points.append([None, None]) #end of polygon
#         elif feature['geometry']['type'] == 'LineString':
#             points.extend(feature['geometry']['coordinates'])
#             points.append([None, None])
#         else: pass
#     lons, lats = zip(*points)
#     return lons, lats
