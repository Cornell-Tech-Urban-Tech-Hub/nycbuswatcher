import requests

import geopandas as gpd

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.express as px

from shared.config import config

api_url = config.config['api_url']
routemap_url = "http://nyc.buswatcher.org/static/route_shapes_nyc.geojson"
JACOBS_LOGO = "/assets/jacobs.png"


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

# future add a route filter selector with callback
# future stand up a 2nd app on another page for the historical viewer with animated playback?
# future add a datetime selector and callback + change api call to get a time period data from the datetime API endpoint (or use last hour)


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

    # scattermapbox api—not that many options for customization
    # https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html
    # https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scattermapbox.html#plotly.graph_objects.Scattermapbox

    fig = px.scatter_mapbox(buses_gdf,
        lat=buses_gdf.geometry.y,
        lon=buses_gdf.geometry.x,
        size='passengers', # todo VIP how to make the buses with zero passengers appear? can see the hover data
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
        labels={
            'trip_id':'Trip (GTFS): ',
            'vehicleref':'Vehicle (GTFS): ',
            'next_stop_id':'Next Stop (GTFS): ',
            'next_stop_eta': 'ETA: ',
            'next_stop_d':'Distance to Next Stop (m): ',
            'next_stop_d_along_route': 'Distance traveled on route (m): '
        },
        opacity=0.8,
        zoom=11)


    # # todo VIP make route map layer display
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

    # fig.update_layout(mapbox_style="white-bg")
    fig.update_layout(mapbox_style="carto-positron")
    # fig.update_layout(mapbox_style="stamen-toner")
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