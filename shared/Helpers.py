import datetime as dt
import requests
import geopandas as gpd

from shared.config import config

def get_buses_gdf():

    # # livemap version
    # api_url = config.config['api_livemap_url']

    # playback a day version
    end=dt.datetime.today().isoformat()
    start=(dt.datetime.today()-dt.timedelta(hours=24)).isoformat()
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

# OLD METHODS TRYING TO ADD ROUTE MAP

# # api doc https://plotly.com/python-api-reference/generated/plotly.graph_objects.Figure.html
# # search for "add_scattermapbox"
# route_json = requests.get(routemap_url).json()
#
# # UNPACK THE COORDS FOR THE MULTILINESTRING
# coords = list()
# for feature in route_json["features"]:
#     for multiline in feature["geometry"]["coordinates"]:
#         try:
#             lat = np.array(multiline)[:, 1]
#             lon = np.array(multiline)[:, 0]
#             multiline_coords = [lat, lon]
#         except Exception as e:
#             # print('I got an error on but kept going: {}'.format(feature['properties']['route_id']))
#             pass
#         coords.append(multiline_coords)
# coord_array = np.array(coords, dtype=object)
#
# # https://community.plotly.com/t/how-do-i-plot-a-line-on-a-map-from-a-geojson-file/33320/2
# fig.add_scattermapbox(
#             lat=coord_array[:, 1],
#             lon=coord_array[:, 0],
#             mode="lines",
#             line={'width':8,
#                   'color':"#000"
#             }
# )


# def get_route_map():
#     fig = go.Figure(data=[go.Scattermapbox(lat=[0], lon=[0])])
#     fig.update_layout(
#         margin={"r": 100, "t": 0, "l": 0, "b": 0},
#         mapbox=go.layout.Mapbox(
#             layers=[{
#                 'sourcetype': 'geojson',
#                 'source': routemap_url,
#                 'type': 'line',
#                 'color': '#000000',
#                 'line' : {'width': 2},
#                 'opacity': 0.5
#             }]
#         )
#     )
#     return fig