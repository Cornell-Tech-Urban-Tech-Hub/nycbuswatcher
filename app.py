## config
# api_url = "http://nyc.buswatcher.org/api/v1/nyc/livemap"
api_url = "http://127.0.01:5000/api/v1/nyc/livemap"
routemap_url = "http://nyc.buswatcher.org/static/route_shapes_nyc.geojson"
JACOBS_LOGO = "/assets/jacobs.png"

#imports
import requests
import json
import numpy as np
import geopandas as gpd

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.express as px

## instantiate the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.config.suppress_callback_exceptions = True

### helpers
def remoteGeoJSONToGDF(url, display = False):
    """https://maptastik.medium.com/remote-geojson-to-geodataframe-19c3c1282a64
    """
    r = requests.get(url)
    data = r.json()
    gdf = gpd.GeoDataFrame.from_features(data['features'])
    if display:
        gdf.plot()
    return gdf


### content sections
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Map", href="")),
        dbc.NavItem(dbc.NavLink("Code", href="/faq")),
        dbc.NavItem(dbc.NavLink("FAQ", href="https://github.com/Cornell-Tech-Urban-Tech-Hub/nycbuswatcher")),
        # html.A(dbc.Row([dbc.Col(html.Img(src=JACOBS_LOGO, height="100px"))],
        # ))

        ],

    brand="NYCBuswatcher",
    brand_href="#",
    color="black",
    dark=True,
)

# todo 1 add a route filter selector with callback


# todo stand up a 2nd app on another page for the historical viewer with animated playback?

# todo add a datetime selector and callback + change api call to get a time period data from the datetime API endpoint (or use last hour)
# todo style base map and controls
# ----- using https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html?highlight=scatter_mapbox

# todo add zoom control


def get_route_map():
    fig = go.Figure(data=[go.Scattermapbox(lat=[0], lon=[0])])
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        mapbox=go.layout.Mapbox(
            layers=[{
                'sourcetype': 'geojson',
                'source': routemap_url,
                'type': 'line',
                'color': '#000000',
                'line' : {'width': 2},
                'opacity': 0.5
            }]
        )
    )
    return fig


def get_bus_map():
    buses_gdf = remoteGeoJSONToGDF(api_url)


    # todo 1 is there a way to further manipulate this figure using the https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scattermapbox.html#plotly.graph_objects.Scattermapbox API?
    fig = px.scatter_mapbox(buses_gdf,
                            lat=buses_gdf.geometry.y,
                            lon=buses_gdf.geometry.x,
                            size='passengers',
                            # todo how to make the buses with zero passengers appear?
                            size_max=30,
                            color='passengers',
                            # color_continuous_scale=['#23bf06','#e55e5e'],
                            color_continuous_scale=[(0, "black"), (0.5, "green"), (1, "red")],
                            hover_name="lineref",
                            hover_data=["trip_id",
                                        "vehicleref",
                                        "next_stop_id",
                                        "next_stop_eta",
                                        "next_stop_d_along_route",
                                        "next_stop_d"],
                            # todo write labels for rest of hover fields
                            labels={
                                'trip_id':'GTFS Trip Identifier'
                            },
                            opacity=0.8,
                            zoom=11)


    # # todo 1 add route map layer
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
    #                   'color':"#F00"
    #             }
    # )

    #
    # fig.update_layout(
    #     margin={"r": 0, "t": 0, "l": 0, "b": 0},
    #     mapbox=go.layout.Mapbox(
    #         style="stamen-terrain",
    #         zoom=10,
    #         center_lat=40.5,
    #         center_lon=-105.08,
    #     )
    # )
    # fig.show()


    fig.update_layout(mapbox_style="carto-positron")     # fig.update_layout(mapbox_style="stamen-toner") #todo comment this out to see the route_map?
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig


app.layout = \
    html.Div([
        navbar,
        dcc.Graph(id='map',
                  figure=get_bus_map(),
                  style={
                          'height': '100vh',
                      }
                  ),
        dcc.Interval(
            id='30-second-interval',
            interval=30000,  # milliseconds
            n_intervals=0
        ),
    ])

# based on https://towardsdatascience.com/python-for-data-science-advanced-guide-to-plotly-dash-interactive-visualizations-8586b0895032
@app.callback(Output('map','figure'),[Input('30-second-interval', 'n_intervals')])
def update_layout(n):
    figure=get_bus_map()
    return figure


if __name__ == "__main__":
    app.run_server(debug=True)