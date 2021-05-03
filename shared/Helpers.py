import datetime as dt
import requests
import geopandas as gpd

from shared.config import config

def get_buses_gdf():

    # # livemap version
    # api_url = config.config['api_livemap_url']

    # playback an hour version
    end=dt.datetime.today().isoformat()
    start=(dt.datetime.today()-dt.timedelta(hours=1)).isoformat() #bug this usually times out gunicorn if set to 24 hours
    api_url = config.config['api_base_url'] + ( 'buses?start='+ start) + ( '&end='+ end )

    # get the gdf
    buses_gdf = remoteGeoJSONToGDF(api_url)

    # clean up the passenger_count column
    buses_gdf['passenger_count'] = buses_gdf['passenger_count'].astype('float')
    buses_gdf['passenger_count'] = buses_gdf['passenger_count'].fillna(0)

    # todo make buses_gdf['timestamp'] format prettier?

    return buses_gdf

def remoteGeoJSONToGDF(url, display = False):
    """https://maptastik.medium.com/remote-geojson-to-geodataframe-19c3c1282a64
    """
    r = requests.get(url)
    data = r.json()
    gdf = gpd.GeoDataFrame.from_features(data['features'])
    if display:
        gdf.plot()
    return gdf

def remote_geojson_to_coords(geojson_url):
    # after https://community.plotly.com/t/how-to-plot-a-scattermapbox-with-pandas/33393

    data=requests.get(geojson_url).json()
    points = []

    for  feature in data['features']:
        if feature['geometry']['type'] == 'Polygon':
            points.extend(feature['geometry']['coordinates'][0])
            points.append([None, None]) # mark the end of a polygon

        elif feature['geometry']['type'] == 'MultiPolygon':
            for polyg in feature['geometry']['coordinates']:
                points.extend(polyg[0])
                points.append([None, None]) #end of polygon
        elif feature['geometry']['type'] == 'LineString':
            points.extend(feature['geometry']['coordinates'])
            points.append([None, None])
        else: pass
    lons, lats = zip(*points)
    return lons, lats
