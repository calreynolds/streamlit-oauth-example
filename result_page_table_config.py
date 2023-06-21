import dash_mantine_components as dmc
import dash_ag_grid as dag
import plotly_express as px
from dash import dcc
from dash_iconify import DashIconify
import plotly.graph_objects as go
from plotly.subplots import make_subplots


## example:
def raw_queries_table_config(df):
    columnDefs = [
        {
            "headerName": x,
            "field": x,
            "filter": True,
            "floatingFilter": True,
            "filterParams": {"buttons": ["apply", "reset"]},
        }
        for x in df.columns
    ]
    rowData = df.to_dict("records")
    sideBar = {}
    return columnDefs, rowData, sideBar


def create_accordion_item(title, children, acc_icon):
    return dmc.AccordionItem(
        value=title.lower().replace(" ", "-"),
        children=[
            dmc.AccordionControl(
                title,
                icon=DashIconify(
                    icon=acc_icon,
                    color="rgb(255 54 33)",
                    width=20,
                ),
            ),
            dmc.AccordionPanel(children),
        ],
    )


def create_ag_grid(df, ommitted_columns=None, custom_definitions=None):
    if ommitted_columns:
        df = df.drop(ommitted_columns, axis=1)
    if custom_definitions is None:
        columnDefs = [
            {
                "headerName": x,
                "field": x,
                "filter": True,
                "floatingFilter": True,
                "filterParams": {"buttons": ["apply", "reset"]},
            }
            for x in df.columns
        ]
        rowData = df.to_dict("records")
        sideBar = {
            "toolPanels": [
                {
                    "id": "columns",
                    "labelDefault": "Columns",
                    "labelKey": "columns",
                    "iconKey": "columns",
                    "toolPanel": "agColumnsToolPanel",
                },
                {
                    "id": "filters",
                    "labelDefault": "Filters",
                    "labelKey": "filters",
                    "iconKey": "filter",
                    "toolPanel": "agFiltersToolPanel",
                },
                {
                    "id": "filters 2",
                    "labelKey": "filters",
                    "labelDefault": "More Filters",
                    "iconKey": "menu",
                    "toolPanel": "agFiltersToolPanel",
                },
            ],
            "position": "right",
            "defaultToolPanel": "filters",
        }
    else:
        columnDefs, rowData, sideBar = custom_definitions

    return dag.AgGrid(
        enableEnterpriseModules=True,
        columnDefs=columnDefs,
        rowData=rowData,
        columnSize="sizeToFit",
        style={"height": "550px"},
        dashGridOptions={
            "rowSelection": "multiple",
            "sideBar": sideBar,
        },
        defaultColDef=dict(
            resizable=True,
            editable=True,
            sortable=True,
            autoHeight=True,
            width=150,
        ),
    )


def create_bar_chart(df, x, y, column_desc, title="", number_limit=None):
    if column_desc and number_limit and title:
        df = df.sort_values(column_desc, ascending=False)
        df = df.head(number_limit)
        fig = px.bar(
            df,
            x=x,
            y=y,
            color=y,
            color_continuous_scale=["rgb(255, 54, 33)", "rgb(27, 49, 57)"],
            title=title,
        )
    elif column_desc and title:
        df = df.sort_values(column_desc, ascending=False)
        fig = px.bar(
            df,
            x=x,
            y=y,
            color=y,
            color_continuous_scale=["rgb(255, 54, 33)", "rgb(27, 49, 57)"],
            title=title,
        )
    else:
        fig = px.bar(df, x=x, y=y)
    fig.update_layout(yaxis={"categoryorder": "total descending"})

    fig.update_layout(plot_bgcolor="white")
    fig.update_xaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_yaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )

    return fig


def cardinality_bar_chart(df, x, y, column_desc, title="", number_limit=None):
    df["TableAndColumn"] = df["TableName"] + "." + df["ColumnName"]
    if column_desc and number_limit and title:
        df = df.sort_values(column_desc, ascending=False)
        df = df.head(number_limit)
        fig = px.bar(
            df,
            x=x,
            y=y,
            color=y,
            color_continuous_scale=["rgb(255, 54, 33)", "rgb(27, 49, 57)"],
            title=title,
        )
    elif column_desc and title:
        df = df.sort_values(column_desc, ascending=False)
        fig = px.bar(
            df,
            x=x,
            y=y,
            color=y,
            color_continuous_scale=["rgb(255, 54, 33)", "rgb(27, 49, 57)"],
            title=title,
        )
    else:
        fig = px.bar(df, x=x, y=y)
    fig.update_layout(yaxis={"categoryorder": "total descending"})

    fig.update_layout(plot_bgcolor="white")
    fig.update_xaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )
    fig.update_yaxes(
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )

    return fig


def create_bar_line_query_daily_chart(df):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add bar chart (on secondary y-axis)
    fig.add_trace(
        go.Bar(
            x=df["QueryStartTime"],
            y=df["NumberOfQueries"],
            name="Number of Queries",
            marker_color=["rgb(27, 49, 57)"] * len(df["QueryStartTime"]),
        ),
        secondary_y=False,
    )

    # Add line plot (on primary y-axis)
    fig.add_trace(
        go.Scatter(
            x=df["QueryStartTime"], y=df["AverageRuntime"], name="Average Query Time"
        ),
        secondary_y=True,
    )

    # Add figure title
    fig.update_layout(title_text="Average Query Time and Number of Queries")
    fig.update_layout(plot_bgcolor="white")

    # Set x-axis title
    fig.update_xaxes(
        title_text="Date",
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Avg Query Time</b>", secondary_y=True)
    fig.update_yaxes(
        title_text="<b>Number of Queries</b>",
        secondary_y=False,
        mirror=True,
        ticks="outside",
        showline=True,
        linecolor="black",
        gridcolor="lightgrey",
    )

    return fig
