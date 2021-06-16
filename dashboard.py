import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, ClientsideFunction

import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dt
import pathlib

import shared.config.config as config


app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

server = app.server
app.config.suppress_callback_exceptions = True

# Path
BASE_PATH = pathlib.Path(__file__).parent.resolve()
DATA_PATH = BASE_PATH.joinpath("data").resolve()

# Read data
url=config.config['base_api_url']+'/dashboard'
df=pd.read_csv(url)
# url = config.config['base_api_url']+'/dashboard'
# df = pd.read_json('https://financialmodelingprep.com/api/v3/company-key-metrics/AAPL?period=quarter', orient='records').set_index('route', inplace=True)

route_list = df["route"].unique()
kind = df["kind"].unique().tolist()

# Date
# Format checkin Time
df["Check-In Time"] = df["Check-In Time"].apply(
    lambda x: dt.strptime(x, "%Y-%m-%d %I:%M:%S %p")  #todo update
)  # String -> Datetime

# Insert weekday and hour of checkin time
df["Days of Wk"] = df["hour"] = df["Check-In Time"]  #todo update
df["Days of Wk"] = df["Days of Wk"].apply(
    lambda x: dt.strftime(x, "%A")
)  # Datetime -> weekday string

df["hour"] = df["hour"].apply(
    lambda x: dt.strftime(x, "%I %p")


)  # Datetime -> int(hour) + AM/PM

day_list = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

check_in_duration = df["Check-In Time"].describe() #todo update

# Register all departments for callbacks
all_departments = df["Department"].unique().tolist()  #todo update
wait_time_inputs = [
    Input((i + "_wait_time_graph"), "selectedData") for i in all_departments
]
score_inputs = [Input((i + "_score_graph"), "selectedData") for i in all_departments]


def description_card():
    """

    :return: A Div containing dashboard title & descriptions.
    """
    return html.Div(
        id="description-card",
        children=[
            html.H5("DataStore Dashboard"),
            html.H3("NYCBusWatcher"),
            html.Div(
                id="intro",
                children="Browse and download data from our archives of New York City bus positions. Dashboard updates every 5 minutes.",
            ),
        ],
    )


def generate_control_card():
    """

    :return: A Div containing controls for graphs.
    """
    return html.Div(
        id="control-card",
        children=[
            html.P("Select Route"),
            dcc.Dropdown(
                id="route-select",  #todo update
                options=[{"label": i, "value": i} for i in route_list],
                value=route_list[0],
            ),
            html.Br(),
            html.P("Select Date Range"),
            dcc.DatePickerRange(
                id="date-picker-select",
                start_date=dt(2021, 6, 1),
                end_date=dt(2022, 12, 31),
                min_date_allowed=dt(2021, 6, 1),
                max_date_allowed=dt(2022, 12, 31),
                initial_visible_month=dt(2021, 6, 1),
            ),
            html.Br(),
            html.Br(),
            html.P("Object Kind"),
            dcc.Dropdown(
                id="kind-select",
                options=[{"label": i, "value": i} for i in kind],
                value=kind[:],
                multi=True,
            ),
            html.Br(),
            html.Div(
                id="reset-btn-outer",
                children=html.Button(id="reset-btn", children="Reset", n_clicks=0),
            ),
        ],
    )


def generate_object_count_heatmap(start, end, route, hm_click, kind, reset):
    """
    :param: start: start date from selection.
    :param: end: end date from selection.
    :param: clinic: clinic from selection.
    :param: hm_click: clickData from heatmap.
    :param: admit_type: admission type from selection.
    :param: reset (boolean): reset heatmap graph if True.

    :return: Patient volume annotated heatmap.
    """

    #todo filter by kind, tally by route and date, hour

    filtered_df = df[
        (df["route"] == route) & (df["kind"].isin(kind))
    ]
    filtered_df = filtered_df.sort_values("Check-In Time").set_index("Check-In Time")[
        start:end  #todo update
    ]

    x_axis = [datetime.time(i).strftime("%I %p") for i in range(24)]  # 24hr time list
    y_axis = day_list

    hour_of_day = ""
    weekday = ""
    shapes = []

    if hm_click is not None:
        hour_of_day = hm_click["points"][0]["x"]
        weekday = hm_click["points"][0]["y"]

        # Add shapes
        x0 = x_axis.index(hour_of_day) / 24
        x1 = x0 + 1 / 24
        y0 = y_axis.index(weekday) / 7
        y1 = y0 + 1 / 7

        shapes = [
            dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=x0,
                x1=x1,
                y0=y0,
                y1=y1,
                line=dict(color="#ff6347"),
            )
        ]

    # Get z value : sum(number of records) based on x, y,

    z = np.zeros((7, 24))
    annotations = []

    for ind_y, day in enumerate(y_axis):
        filtered_day = filtered_df[filtered_df["Days of Wk"] == day]
        for ind_x, x_val in enumerate(x_axis):
            sum_of_record = filtered_day[filtered_day["hour"] == x_val][  #todo update -- should this be sum of num_buses?
                "Number of Records"
            ].sum()
            z[ind_y][ind_x] = sum_of_record

            annotation_dict = dict(
                showarrow=False,
                text="<b>" + str(sum_of_record) + "<b>",
                xref="x",
                yref="y",
                x=x_val,
                y=day,
                font=dict(family="sans-serif"),
            )
            # Highlight annotation text by self-click
            if x_val == hour_of_day and day == weekday:
                if not reset:
                    annotation_dict.update(size=15, font=dict(color="#ff6347"))

            annotations.append(annotation_dict)

    # Heatmap
    hovertemplate = "<b> %{y}  %{x} <br><br> %{z} BusObservations/JSONbuses"  #todo update

    data = [
        dict(
            x=x_axis,
            y=y_axis,
            z=z,
            type="heatmap",
            name="",
            hovertemplate=hovertemplate,
            showscale=False,
            colorscale=[[0, "#caf3ff"], [1, "#2c82ff"]],
        )
    ]

    layout = dict(
        margin=dict(l=70, b=50, t=50, r=50),
        modebar={"orientation": "v"},
        font=dict(family="Open Sans"),
        annotations=annotations,
        shapes=shapes,
        xaxis=dict(
            side="top",
            ticks="",
            ticklen=2,
            tickfont=dict(family="sans-serif"),
            tickcolor="#ffffff",
        ),
        yaxis=dict(
            side="left", ticks="", tickfont=dict(family="sans-serif"), ticksuffix=" "
        ),
        hovermode="closest",
        showlegend=False,
    )
    return {"data": data, "layout": layout}


# def generate_table_row(id, style, col1, col2, col3):
#     """ Generate table rows.
#
#     :param id: The ID of table row.
#     :param style: Css style of this row.
#     :param col1 (dict): Defining id and children for the first column.
#     :param col2 (dict): Defining id and children for the second column.
#     :param col3 (dict): Defining id and children for the third column.
#     """
#
#     return html.Div(
#         id=id,
#         className="row table-row",
#         style=style,
#         children=[
#             html.Div(
#                 id=col1["id"],
#                 style={"display": "table", "height": "100%"},
#                 className="two columns row-department",
#                 children=col1["children"],
#             ),
#             html.Div(
#                 id=col2["id"],
#                 style={"textAlign": "center", "height": "100%"},
#                 className="five columns",
#                 children=col2["children"],
#             ),
#             html.Div(
#                 id=col3["id"],
#                 style={"textAlign": "center", "height": "100%"},
#                 className="five columns",
#                 children=col3["children"],
#             ),
#         ],
#     )
#
#
# def generate_table_row_helper(department):  #todo update
#     """Helper function.
#
#     :param: department (string): Name of department.
#     :return: Table row.
#     """
#     return generate_table_row(
#         department, #todo update
#         {},
#         {"id": department + "_department", "children": html.B(department)},  #todo update
#         {
#             "id": department + "wait_time",  #todo update
#             "children": dcc.Graph(
#                 id=department + "_wait_time_graph",
#                 style={"height": "100%", "width": "100%"},
#                 className="wait_time_graph",
#                 config={
#                     "staticPlot": False,
#                     "editable": False,
#                     "displayModeBar": False,
#                 },
#                 figure={
#                     "layout": dict(
#                         margin=dict(l=0, r=0, b=0, t=0, pad=0),
#                         xaxis=dict(
#                             showgrid=False,
#                             showline=False,
#                             showticklabels=False,
#                             zeroline=False,
#                         ),
#                         yaxis=dict(
#                             showgrid=False,
#                             showline=False,
#                             showticklabels=False,
#                             zeroline=False,
#                         ),
#                         paper_bgcolor="rgba(0,0,0,0)",
#                         plot_bgcolor="rgba(0,0,0,0)",
#                     )
#                 },
#             ),
#         },
#         {
#             "id": department + "_patient_score",  #todo update
#             "children": dcc.Graph(
#                 id=department + "_score_graph",
#                 style={"height": "100%", "width": "100%"},
#                 className="patient_score_graph",
#                 config={
#                     "staticPlot": False,
#                     "editable": False,
#                     "displayModeBar": False,
#                 },
#                 figure={
#                     "layout": dict(
#                         margin=dict(l=0, r=0, b=0, t=0, pad=0),
#                         xaxis=dict(
#                             showgrid=False,
#                             showline=False,
#                             showticklabels=False,
#                             zeroline=False,
#                         ),
#                         yaxis=dict(
#                             showgrid=False,
#                             showline=False,
#                             showticklabels=False,
#                             zeroline=False,
#                         ),
#                         paper_bgcolor="rgba(0,0,0,0)",
#                         plot_bgcolor="rgba(0,0,0,0)",
#                     )
#                 },
#             ),
#         },
#     )
#
#
# def initialize_table():
#     """
#     :return: empty table children. This is intialized for registering all figure ID at page load.
#     """
#
#     # header_row
#     header = [
#         generate_table_row(
#             "header",
#             {"height": "50px"},
#             {"id": "header_department", "children": html.B("Department")},  #todo update
#             {"id": "header_wait_time_min", "children": html.B("Wait Time Minutes")},  #todo update
#             {"id": "header_care_score", "children": html.B("Care Score")},  #todo update
#         )
#     ]
#
#     # department_row
#     rows = [generate_table_row_helper(department) for department in all_departments]
#     header.extend(rows)
#     empty_table = header
#
#     return empty_table
#
#
# def generate_patient_table(figure_list, departments, wait_time_xrange, score_xrange):
#     """
#     :param score_xrange: score plot xrange [min, max].
#     :param wait_time_xrange: wait time plot xrange [min, max].
#     :param figure_list:  A list of figures from current selected metrix.
#     :param departments:  List of departments for making table.
#     :return: Patient table.
#     """
#     # header_row
#     header = [
#         generate_table_row(
#             "header",
#             {"height": "50px"},
#             {"id": "header_department", "children": html.B("Department")},  #todo update
#             {"id": "header_wait_time_min", "children": html.B("Wait Time Minutes")},  #todo update
#             {"id": "header_care_score", "children": html.B("Care Score")},  #todo update
#         )
#     ]
#
#     # department_row
#     rows = [generate_table_row_helper(department) for department in departments]
#     # empty_row
#     empty_departments = [item for item in all_departments if item not in departments]
#     empty_rows = [
#         generate_table_row_helper(department) for department in empty_departments
#     ]
#
#     # fill figures into row contents and hide empty rows
#     for ind, department in enumerate(departments):
#         rows[ind].children[1].children.figure = figure_list[ind]
#         rows[ind].children[2].children.figure = figure_list[ind + len(departments)]
#     for row in empty_rows[1:]:
#         row.style = {"display": "none"}
#
#     # convert empty row[0] to axis row
#     empty_rows[0].children[0].children = html.B(
#         "graph_ax", style={"visibility": "hidden"}
#     )
#
#     empty_rows[0].children[1].children.figure["layout"].update(
#         dict(margin=dict(t=-70, b=50, l=0, r=0, pad=0))
#     )
#
#     empty_rows[0].children[1].children.config["staticPlot"] = True
#
#     empty_rows[0].children[1].children.figure["layout"]["xaxis"].update(
#         dict(
#             showline=True,
#             showticklabels=True,
#             tick0=0,
#             dtick=20,
#             range=wait_time_xrange,
#         )
#     )
#     empty_rows[0].children[2].children.figure["layout"].update(
#         dict(margin=dict(t=-70, b=50, l=0, r=0, pad=0))
#     )
#
#     empty_rows[0].children[2].children.config["staticPlot"] = True
#
#     empty_rows[0].children[2].children.figure["layout"]["xaxis"].update(
#         dict(showline=True, showticklabels=True, tick0=0, dtick=0.5, range=score_xrange)
#     )
#
#     header.extend(rows)
#     header.extend(empty_rows)
#     return header
#
#
# def create_table_figure(
#     department, filtered_df, category, category_xrange, selected_index
# ):
#     """Create figures.
#
#     :param department: Name of department.
#     :param filtered_df: Filtered dataframe.
#     :param category: Defining category of figure, either 'wait time' or 'care score'.
#     :param category_xrange: x axis range for this figure.
#     :param selected_index: selected point index.
#     :return: Plotly figure dictionary.
#     """
#     aggregation = {
#         "Wait Time Min": "mean",  #todo update
#         "Care Score": "mean",  #todo update
#         "Days of Wk": "first",  #todo update
#         "Check-In Time": "first",  #todo update
#         "Check-In Hour": "first",  #todo update
#     }
#
#     df_by_department = filtered_df[
#         filtered_df["Department"] == department  #todo update
#     ].reset_index()
#     grouped = (
#         df_by_department.groupby("Encounter Number").agg(aggregation).reset_index()  #todo update
#     )
#     patient_id_list = grouped["Encounter Number"]  #todo update
#
#     x = grouped[category]
#     y = list(department for _ in range(len(x)))
#
#     f = lambda x_val: dt.strftime(x_val, "%Y-%m-%d")
#     check_in = (
#         grouped["Check-In Time"].apply(f)  #todo update
#         + " "
#         + grouped["Days of Wk"]  #todo update
#         + " "
#         + grouped["Check-In Hour"].map(str)  #todo update
#     )
#
#     text_wait_time = (
#         "Patient # : "
#         + patient_id_list
#         + "<br>Check-in Time: "
#         + check_in
#         + "<br>Wait Time: "
#         + grouped["Wait Time Min"].round(decimals=1).map(str)
#         + " Minutes,  Care Score : "
#         + grouped["Care Score"].round(decimals=1).map(str)
#     )  #todo update
#
#     layout = dict(
#         margin=dict(l=0, r=0, b=0, t=0, pad=0),
#         clickmode="event+select",
#         hovermode="closest",
#         xaxis=dict(
#             showgrid=False,
#             showline=False,
#             showticklabels=False,
#             zeroline=False,
#             range=category_xrange,
#         ),
#         yaxis=dict(
#             showgrid=False, showline=False, showticklabels=False, zeroline=False
#         ),
#         paper_bgcolor="rgba(0,0,0,0)",
#         plot_bgcolor="rgba(0,0,0,0)",
#     )
#
#     trace = dict(
#         x=x,
#         y=y,
#         mode="markers",
#         marker=dict(size=14, line=dict(width=1, color="#ffffff")),
#         color="#2c82ff",
#         selected=dict(marker=dict(color="#ff6347", opacity=1)),
#         unselected=dict(marker=dict(opacity=0.1)),
#         selectedpoints=selected_index,
#         hoverinfo="text",
#         customdata=patient_id_list,
#         text=text_wait_time,
#     )
#
#     return {"data": [trace], "layout": layout}


app.layout = html.Div(
    id="app-container",
    children=[
        # Banner
        html.Div(
            id="banner",
            className="banner",
            children=[html.Img(src=app.get_asset_url("jacobs.png"))],
        ),
        # Left column
        html.Div(
            id="left-column",
            className="four columns",
            children=[description_card(), generate_control_card()]
            + [
                html.Div(
                    ["initial child"], id="output-clientside", style={"display": "none"}
                )
            ],
        ),
        # Right column
        html.Div(
            id="right-column",
            className="eight columns",
            children=[
                # Barrels Heatmap
                html.Div(
                    id="barrel_heatmap_card", #todo update
                    children=[
                        html.B("Barrels (# of BusObservations)"),
                        html.Hr(),
                        dcc.Graph(id="barrel_heatmap"), #todo update
                    ],
                ),


                # Patient Wait time by Department
                html.Div(
                    id="shipment_heatmap_card",
                    children=[
                        html.B("Shipments (# of Buses)"),
                        html.Hr(),
                        dcc.Graph(id="shipment_heatmap"), #todo update
                    ],
                ),
            ],
        ),
    ],
)


@app.callback(
    Output("barrel_heatmap", "figure"),
    [
        Input("date-picker-select", "start_date"),
        Input("date-picker-select", "end_date"),
        Input("route-select", "value"),
        Input("barrel_heatmap", "clickData"),
        Input("kind-select", "value"),
        Input("reset-btn", "n_clicks"),
    ],
)
def update_heatmap(start, end, route, hm_click, kind, reset_click):
    start = start + " 00:00:00"
    end = end + " 00:00:00"

    reset = False
    # Find which one has been triggered
    ctx = dash.callback_context

    if ctx.triggered:
        prop_id = ctx.triggered[0]["prop_id"].split(".")[0]
        if prop_id == "reset-btn":
            reset = True

    # Return to original hm(no colored annotation) by resetting
    return generate_object_count_heatmap(
        start, end, route, hm_click, kind, reset
    )


app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("wait_time_table", "children")] + wait_time_inputs + score_inputs,
)





# Run the server
if __name__ == "__main__":
    app.run_server(debug=True)
