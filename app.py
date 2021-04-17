## config
# api_url = "http://nyc.buswatcher.org/api/v1/nyc/livemap"
api_url = "http://127.0.01:5000/api/v1/nyc/livemap"
JACOBS_LOGO = "/assets/jacobs.png"


# todo style base map and controls
# ----- using https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html?highlight=scatter_mapbox
# todo customize popup fields and text
# todo add zoom control
# todo add a route filter selector with callback
# todo change api call to get a time period data from the datetime API endpoint (or use last hour)
# todo add a datetime selector and callback

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
        # html.A(dbc.Row([dbc.Col(html.Img(src=JACOBS_LOGO, height="100px"))],
        # ))

        ],

    brand="NYCBuswatcher",
    brand_href="#",
    color="black",
    dark=True,
)

def get_map():

    buses_gdf = remoteGeoJSONToGDF(api_url)

    fig = px.scatter_mapbox(buses_gdf,
                            lat=buses_gdf.geometry.y,
                            lon=buses_gdf.geometry.x,
                            size='passengers',
                            color='passengers',
                            color_continuous_scale=['#23bf06','#e55e5e'],
                            # animation_frame='timestamp',
                            hover_name="lineref",
                            hover_data=["trip_id","vehicleref"],
                            zoom=11)

    fig.update_layout(mapbox_style="stamen-toner")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

    return fig


app.layout = \
    html.Div([
        navbar,
        dcc.Graph(id='map',
                      figure=get_map(),
                      style={
                          'height': '100vh'
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
    figure=get_map() #todo debug this?
    return figure


if __name__ == "__main__":
    app.run_server(debug=True)