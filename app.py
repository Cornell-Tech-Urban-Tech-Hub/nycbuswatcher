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


# todo add a route filter selector with callback
# todo add a datetime selector and callback + change api call to get a time period data from the datetime API endpoint (or use last hour)
# todo style base map and controls
# ----- using https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html?highlight=scatter_mapbox
# todo customize popup fields and text
# todo add zoom control


# todo debug this
# https://community.plotly.com/t/how-do-i-plot-a-line-on-a-map-from-a-geojson-file/33320/2
# https://plotly.com/python/lines-on-mapbox/


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
    fig = px.scatter_mapbox(buses_gdf,
                            lat=buses_gdf.geometry.y,
                            lon=buses_gdf.geometry.x,
                            size='passengers',
                            size_max=40,
                            color='passengers',
                            # color_continuous_scale=['#23bf06','#e55e5e'],
                            color_continuous_scale=[(0, "black"), (0.5, "green"), (1, "red")],
                            range_color=[0,0], #todo how to make the buses with zero passengers appear?
                            hover_name="lineref",
                            hover_data=["trip_id","vehicleref","next_stop_id","next_stop_eta","next_stop_d_along_route","next_stop_d"],
                            # animation_group="vehicleref",
                            # animation_frame="lineref",
                            title='Passenger Counts: Active Buses',
                            zoom=11)

    # todo add route map layer, need to unpack the routemap geojson
    # route_json = requests.get(routemap_url).json()
    # gdf = gpd.GeoDataFrame.from_features(requests.get(routemap_url).json()['features'])
    # fig.add_scattermapbox(gdf,
    #             mode="lines",
    #             line=dict(width=8, color="#F00")
    #         )

    fig.update_layout(mapbox_style="carto-positron")     # fig.update_layout(mapbox_style="stamen-toner")
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