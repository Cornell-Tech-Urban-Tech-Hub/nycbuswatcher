import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px


from shared.Helpers import *

# config
routemap_url = "http://nyc.buswatcher.org/static/route_shapes_nyc.geojson"
JACOBS_LOGO = "/assets/jacobs.png"

## instantiate the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server
app.config.suppress_callback_exceptions = True


#####################################################################################################################
# CONTENT SECTIONS
#####################################################################################################################


#####################################################################################################################
# navbar
#####################################################################################################################

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Map", href="")),
        dbc.NavItem(dbc.NavLink("FAQ", href="/faq")), #bug make me work
        dbc.NavItem(dbc.NavLink("Code", href="https://github.com/Cornell-Tech-Urban-Tech-Hub/nycbuswatcher")),
        html.A(dbc.Row([dbc.Col(html.Img(src=JACOBS_LOGO, height="100px"))],
        ))

        ],

    brand="NYCBuswatcher",
    brand_href="#",
    color="black",
    dark=True,
)


#####################################################################################################################
# plotly express timelapse of last 24 hours (v1, no callbacks yet)
#####################################################################################################################
def get_bus_map():
    try:
        buses_gdf=get_buses_gdf()
        # plotly express scattermapbox api—not that many options for customization
        # https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html
        fig = px.scatter_mapbox(buses_gdf,
            lat=buses_gdf.geometry.y,
            lon=buses_gdf.geometry.x,
            size='passenger_count', # todo VIP how to make the buses with zero passengers appear? can see the hover data but not symbol
            size_max=30,
            color='passenger_count',
            # color_continuous_scale=['#23bf06','#e55e5e'],
            animation_frame='timestamp',
            color_continuous_scale=[(0, "black"), (0.5, "green"), (1, "red")],
            hover_name="route_short",
            hover_data=["trip_id",
                        "vehicle_id",
                        "next_stop_id",
                        "next_stop_eta",
                        "next_stop_d_along_route",
                        "next_stop_d"],
            labels={
                'trip_id':'Trip (GTFS): ',
                'vehicle_id':'Vehicle (GTFS): ',
                'next_stop_id':'Next Stop (GTFS): ',
                'next_stop_eta': 'ETA: ',
                'next_stop_d':'Distance to Next Stop (m): ',
                'next_stop_d_along_route': 'Distance traveled on route (m): '
            },
            opacity=0.8,
            zoom=11)

        # todo supress the plotly hover

        # todo VIP make route map layer display

        # fig.update_layout(mapbox_style="white-bg")
        # fig.update_layout(mapbox_style="carto-positron")
        fig.update_layout(mapbox_style="stamen-toner")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig
    except:
        return


#####################################################################################################################
# plotly low-level with routemap (v0.1, WIP)
#####################################################################################################################

def get_bus_map2():

    try:
        # plotly full more customization
        # https://plotly.com/python/reference/scattermapbox/
        # https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scattermapbox.html#plotly.graph_objects.Scattermapbox
        buses_gdf = get_buses_gdf()
        # after https://medium.com/analytics-vidhya/introduction-to-interactive-geoplots-with-plotly-and-mapbox-9249889358eb
        # set the geo=spatial data
        bus_data = [go.Scattermapbox(
            lat=buses_gdf['lat'],
            lon=buses_gdf['lon'],
            customdata=buses_gdf['passenger_count'],
            mode='markers',
            marker=dict(
                size=4,
                color='gold',
                opacity=.8,
            ),
        )]

        # set the layout to plot
        bus_layout = go.Layout(autosize=True,
                           mapbox=dict(
                                       bearing=30,
                                       pitch=60,
                                       zoom=13,
                                       center=dict(lat=40.721319,
                                                   lon=-73.987130),
                            ),

                           # title="Bus locations in New York"
                               )

        fig = go.Figure(data=bus_data, layout=bus_layout)

        # bug seems to be working, but all the maps are in the wrong layer order
        # method 2 https://community.plotly.com/t/how-do-i-plot-a-line-on-a-map-from-a-geojson-file/33320

        fig.update_layout(
            mapbox=go.layout.Mapbox(
                layers=[{
                    'sourcetype': 'geojson',
                    'source': requests.get(routemap_url).json(),
                    'type': 'line',
                }]
            )
        )

        fig.update_layout(mapbox_style="stamen-toner")
        # fig.update_layout(mapbox_style="white-bg")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig
    except:
        return


#####################################################################################################################
# MAIN LAYOUT
#####################################################################################################################

app.layout = \
    html.Div([
        navbar,
        dcc.Graph(id='map',
                  figure=get_bus_map(),
                  # figure=get_bus_map2(),
                  style={
                          'height': '90vh',
                      }
                  ),

        #####################################################################################################################
        # AUTO REFRESH FOR LIVEMAP VERSION (30 seconds)
        #####################################################################################################################
        # dcc.Interval(
        #     id='30-second-interval',
        #     interval=30000,  # milliseconds
        #     n_intervals=0
        # ),

        #####################################################################################################################
        # AUTO REFRESH FOR ANIMATION VERSION (5 minutes)
        #####################################################################################################################
        dcc.Interval(
            id='5-minute-interval',
            interval=500000,  # milliseconds
            n_intervals=0
        ),

    ])

#####################################################################################################################
# CALLBACKS
#####################################################################################################################

#####################################################################################################################
# AUTO REFRESH FOR LIVEMAP VERSION
#####################################################################################################################
# # based on https://towardsdatascience.com/python-for-data-science-advanced-guide-to-plotly-dash-interactive-visualizations-8586b0895032
# @app.callback(Output('map','figure'),[Input('30-second-interval', 'n_intervals')])
# def update_layout(n):
#     figure=get_bus_map()
#     return figure

@app.callback(Output('map','figure'),[Input('5-minute-interval', 'n_intervals')])
def update_layout(n):
    figure=get_bus_map()
    return figure


if __name__ == "__main__":
    app.run_server(debug=True)