import dash_mantine_components as dmc
import dash_ag_grid as dag


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


##################################################################
################## Most Recent Strategy Result ##################
##################################################################


##################################################################
################## Table Statistics ##################
##################################################################


##################################################################
################## Cardinality Sampling Statistics ##################
##################################################################


##################################################################
################## Raw Queries ##################
##################################################################


##################################################################
################## Most Expensive Queries ##################
##################################################################


##################################################################
################## Queries Over Time - general ##################
##################################################################


##################################################################
################## Top 10 Queries by Duration ##################
##################################################################


##################################################################
################## Top 10 Queries by Day ##################
##################################################################


##################################################################
################## Most Often Run Queries by Day ##################
##################################################################


##################################################################
################## Most Expensive Merge/Delete Operations ##################
##################################################################


def create_accordion_item(title, children):
    return dmc.AccordionItem(
        value=title.lower().replace(" ", "-"),
        children=[
            dmc.AccordionControl(title),
            dmc.AccordionPanel(children),
        ],
    )


def create_ag_grid(df, custom_definitions=None):
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
