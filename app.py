## config
# api_url = "http://nyc.buswatcher.org/api/v1/nyc/livemap"
api_url = "http://127.0.01:5000/api/v1/nyc/livemap"
JACOBS_LOGO = "/assets/jacobs.png"


# todo adapt this starter template from https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-spatial-clustering
# todo and pull my data from http://nyc.buswatcher.org/api/v1/nyc/livemap


# todo add stamen
# todo add the base map and styling
# todo add popups with bus metadata (route, trip, bus id)
# todo add zoom
# todo add a route filter dropdown with callback

import requests
import json
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
        html.A(dbc.Row([dbc.Col(html.Img(src=JACOBS_LOGO, height="100px"))],
                       ))],

    brand="NYCBuswatcher",
    brand_href="#",
    color="black",
    dark=True,
)

def get_map():

    buses_gdf = remoteGeoJSONToGDF(api_url)

    # todo use this page https://plotly.com/python/scattermapbox/

    fig = px.scatter_mapbox(buses_gdf,
                            lat=buses_gdf.geometry.y,
                            lon=buses_gdf.geometry.x,
                            hover_name="lineref",
                            hover_data=["trip_id", "vehicleref"],
                            zoom=10,
                            height=900)

    fig.update_layout(mapbox_style="stamen-toner")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

    return fig


app.layout = \
    html.Div(
        [navbar,
         dcc.Graph(figure=get_map())]

    )





if __name__ == "__main__":
    app.run_server(debug=True)