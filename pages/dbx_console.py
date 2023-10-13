import os
import dash
import json
import requests
from dash import html, callback, Input, Output, ctx, dcc, State
import dash_mantine_components as dmc
import subprocess
import sqlalchemy.exc
from configparser import ConfigParser

# import dash_dangerously_set_inner_html as ddsih
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/delta-optimizer/explorer", title="Explorer")

CATALOG = "main"
SCHEMA = "information_schema"

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


def layout():
    return html.Div(
        [
            dmc.Title("Data Explorer"),
            dmc.Divider(variant="solid"),
            dmc.Space(h=20),
            dmc.Group(
                position="left",
                children=[
                    dmc.Button(
                        "Refresh",
                        id="refresh-button-step4",
                        variant="default",
                    ),
                    html.Div(
                        id="engine-test-result-step4",
                        style={
                            "width": "300px",
                            "position": "relative",
                            "left": "20px",
                            "top": "0px",
                        },
                    ),
                    html.Div(
                        style={
                            "width": "300px",
                            "position": "relative",
                            "left": "350px",
                            "top": "0px",
                        },
                        children=[
                            dcc.Dropdown(
                                id="profile-dropdown-step4",
                                options=[],
                                value="Select Profile",
                                style={
                                    "width": "200px",
                                    "position": "relative",
                                    "left": "40px",
                                    "top": "0px",
                                },
                            ),
                        ],
                    ),
                ],
            ),
            dmc.Space(h=10),
            dmc.Space(h=10),
            dmc.Space(h=30),
            html.Div(id="load-optimizer-grid-step4"),
            dmc.Space(h=20),
            dmc.Space(h=20),
            dmc.Text(id="container-button-timestamp", align="center"),
            dcc.Store(id="table_selection_store4"),
            dcc.Store(id="table_selection_store_now4"),
            dcc.Store(id="schema_selection_store4"),
            dcc.Store(id="catalog_selection_store4"),
            dcc.Store(id="hostname-store4", storage_type="memory"),
            dcc.Store(id="path-store4", storage_type="memory"),
            dcc.Store(id="token-store4", storage_type="memory"),
            dcc.Interval(id="interval4", interval=86400000, n_intervals=0),
            html.Div(id="table_selection_output_now4"),
        ]
    )


@callback(
    [
        Output("hostname-store4", "data"),
        Output("token-store4", "data"),
        Output("path-store4", "data"),
    ],
    [Input("profile-dropdown-step4", "value")],
    prevent_initial_call=True,
)
def parse_databricks_config(profile_name):
    if profile_name:
        config = ConfigParser()
        file_path = os.path.expanduser("~/.databrickscfg")

        if os.path.exists(file_path):
            config.read(file_path)

            if config.has_section(profile_name):
                host = config.get(profile_name, "host")
                token = config.get(profile_name, "token")
                path = config.get(profile_name, "path")

                host = host.replace("https://", "")

                return host, token, path

    return None, None, None


@callback(
    Output("profile-dropdown-step4", "options"),
    Output("load-optimizer-grid-step4", "children"),
    [Input("refresh-button-step4", "n_clicks"), Input("interval4", "n_intervals")],
    State("profile-dropdown-step4", "value"),
)
def populate_profile_dropdown(n_clicks, n_intervals, profile_name):
    config = ConfigParser()
    file_path = os.path.expanduser("~/.databrickscfg")

    if not os.path.exists(file_path):
        return [], []

    config.read(file_path)
    options = []

    for section in config.sections():
        if (
            config.has_option(section, "host")
            and config.has_option(section, "path")
            and config.has_option(section, "token")
        ):
            options.append({"label": section, "value": section})

    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            big_engine = create_engine(engine_url)

            tables_stmt = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.TABLES ;"
            tables_in_db = pd.read_sql_query(tables_stmt, big_engine)

            columnDefs = [
                {
                    "headerName": "Table Name",
                    "field": "table_name",
                    "checkboxSelection": True,
                    "headerCheckboxSelection": True,
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {
                        "buttons": ["apply", "reset"],
                    },
                    "minWidth": 180,
                },
                {
                    "headerName": "Database Name",
                    "field": "table_schema",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Catalog Name",
                    "field": "table_catalog",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                },
                {
                    "headerName": "Creator",
                    "field": "created_by",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Created On",
                    "field": "created",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Last Altered By",
                    "field": "last_altered_by",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Last Altered On",
                    "field": "last_altered",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
            ]

            rowData = tables_in_db.to_dict("records")

            optimizer_grid = [
                dag.AgGrid(
                    id="optimizer-grid-step4",
                    enableEnterpriseModules=True,
                    columnDefs=columnDefs,
                    rowData=rowData,
                    style={"height": "550px"},
                    dashGridOptions={"rowSelection": "multiple", "sideBar": sideBar},
                    columnSize="sizeToFit",
                    defaultColDef=dict(
                        resizable=True,
                        editable=True,
                        sortable=True,
                        autoHeight=True,
                        width=90,
                    ),
                ),
                dmc.Space(h=20),
                dmc.SimpleGrid(
                    cols=3,
                    children=[
                        html.Div(
                            [
                                dmc.Text("Catalogs", align="center", weight=550),
                                dmc.Text(
                                    id="catalog_selection_output4", align="center"
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("DBs", align="center", weight=550),
                                dmc.Text(id="schema_selection_output4", align="center"),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("Tables", align="center", weight=550),
                                dmc.Text(id="table_selection_output4", align="center"),
                            ]
                        ),
                    ],
                ),
            ]

            return options, optimizer_grid

    return options, []


@callback(
    Output("engine-test-result-step4", "children"),
    Input("profile-dropdown-step4", "value"),
    [
        State("hostname-store4", "data"),
        State("path-store4", "data"),
        State("token-store4", "data"),
    ],
    prevent_initial_call=True,
)
def test_engine_connection(profile_name, host, path, token):
    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            # Modify the path to remove "/sql/1.0/warehouses/"
            # sql_warehouse = path.replace("/sql/1.0/warehouses/", "")
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            engine = create_engine(engine_url)

            try:
                # Test the engine connection by executing a sample query
                with engine.connect() as connection:
                    result = connection.execute("SELECT 1")
                    test_value = result.scalar()

                    if test_value == 1:
                        return dmc.LoadingOverlay(
                            dmc.Badge(
                                id="engine-connection-badge",
                                variant="dot",
                                color="green",
                                size="lg",
                                children=[
                                    html.Span(f"Connected to Workspace: {host} ")
                                ],
                            ),
                            loaderProps={
                                "variant": "dots",
                                "color": "orange",
                                "size": "xl",
                            },
                        )
            except sqlalchemy.exc.OperationalError as e:
                return dmc.LoadingOverlay(
                    dmc.Badge(
                        id="engine-connection-badge",
                        variant="dot",
                        color="red",
                        size="lg",
                        children=[html.Span(f"Engine Connection failed: {str(e)}")],
                    ),
                    loaderProps={
                        "variant": "dots",
                        "color": "orange",
                        "size": "xl",
                    },
                )

    return html.Div("Please select a profile.", style={"color": "orange"})


@callback(
    [
        Output("table_selection_output_now4", "children"),
        Output("table_selection_store_now4", "data"),
    ],
    [Input("table_selection_store4", "data")],
    State("profile-dropdown-step4", "value"),
    prevent_initial_call=True,
)
def create_ag_grid(selected_table, profile_name):
    config = ConfigParser()
    file_path = os.path.expanduser("~/.databrickscfg")

    if not os.path.exists(file_path):
        return [], []

    config.read(file_path)
    options = []

    for section in config.sections():
        if (
            config.has_option(section, "host")
            and config.has_option(section, "path")
            and config.has_option(section, "token")
        ):
            options.append({"label": section, "value": section})

    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            engine = create_engine(engine_url)
    stmt = f"select * from {selected_table}"
    df = pd.read_sql_query(stmt, engine)
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

    ag_grid_component = (
        dag.AgGrid(
            id="table-grid",
            enableEnterpriseModules=True,
            columnDefs=columnDefs,
            rowData=rowData,
            style={"height": "550px"},
            dashGridOptions={"rowSelection": "multiple", "sideBar": sideBar},
            columnSize="sizeToFit",
            defaultColDef=dict(
                resizable=True,
                editable=True,
                sortable=True,
                autoHeight=True,
                width=90,
            ),
        ),
    )

    return ag_grid_component, selected_table


@callback(
    Output("catalog_selection_output4", "children"),
    Output("catalog_selection_store4", "data"),
    Input("optimizer-grid-step4", "selectedRows"),
)
def catalog(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_catalog_unique = set(selected_catalog)
        selected_catalog_unique_list = list(selected_catalog_unique)
        final = ",".join(selected_catalog_unique_list)
        # json_tables = json.dumps(final)
        return ", ".join(selected_catalog_unique_list), final
    return "No selections", dash.no_update


@callback(
    Output("schema_selection_output4", "children"),
    Output("schema_selection_store4", "data"),
    Input("optimizer-grid-step4", "selectedRows"),
)
def schema(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_schema = [s["table_schema"] for s in selected]
        selected_schema_unique = set(selected_schema)
        selected_schema_unique_list = list(selected_schema_unique)
        final = [str("main." + i) for i in selected_schema_unique_list]
        final = ",".join(final)
        # json_tables = json.dumps(final)
        return ", ".join(selected_schema_unique_list), final
    return "No selections", dash.no_update


@callback(
    Output("table_selection_output4", "children"),
    Output("table_selection_store4", "data"),
    Input("optimizer-grid-step4", "selectedRows"),
)
def tables(selected):
    if selected:
        selected_tables = [s["table_name"] for s in selected]
        selected_schema = [s["table_schema"] for s in selected]
        final = [
            "main." + schema + "." + table
            for schema, table in zip(selected_schema, selected_tables)
        ]
        final = ",".join(final)
        # json_tables = json.dumps(final)
        return ", ".join(selected_tables), final
    return "No selections", dash.no_update
