import streamlit as st
import json
import pandas as pd
from st_aggrid import AgGrid
from sqlalchemy import create_engine, update, select, Table, MetaData, and_
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source
from dbt_functions import get_connection, create_single_station

# Page definition
st.set_page_config(page_title="Golden Layer RBCB Metadata Editor", layout="wide")

# Manufacturing lines configuration
ipn_list = dict()
station_list = list()
station = None
active_config = dict()
with open("ipn_config.json", "r") as jsonfile:
    config = json.load(jsonfile)
for c in config:
    if c["plant"] == "RBCB":
        ipn_list[c["name"]] = c

# Prepare buffer to store changes until save button hit
if "changes" not in st.session_state:
    st.session_state.changes = list()

# Main selection form
with st.sidebar:
    ipn = st.selectbox("IPN Code", options=ipn_list.keys(), index=None)
    if ipn is not None:
        schema, md_table_name = ipn_list[ipn]["md_table"].split(".")
        engine = get_connection(schema)
        conn = engine.connect()
        linegrp = st.selectbox("Line Group",
            options=ipn_list[ipn]["line_groups"].values(),
            index=None if len(ipn_list[ipn]["line_groups"]) > 1 else 0)
        if linegrp is not None:
            table = Table(md_table_name, MetaData(), autoload_with=engine)
            station_list = conn.execute(
                select(table.c.LOCATION_STATION)
                .distinct()
                .where(table.c.LINE_GROUP == linegrp)
                .order_by(table.c.LOCATION_STATION)
            )
            station = st.selectbox("Station Number", options=station_list, index=None)

if station is not None:
    df = pd.read_sql(table
                    .select()
                    .where(and_(table.c.LOCATION_STATION==station, table.c.LINE_GROUP==linegrp))
                    .order_by(table.c.PARAM_NAME),
                    conn)

    response = AgGrid(df, editable=True)

    if "eventData" in response.grid_response.keys():
        old_row = (df
                .loc[response.grid_response["eventData"]["rowIndex"]]
                .to_dict())
        new_row = (response
                .grid_response["eventData"]["data"])
        new_row.pop("__pandas_index")
        st.session_state.changes.append({
            "old_row": old_row,
            "new_row": new_row
        })

if len(st.session_state.changes):
    if st.button("Save changes", type="primary"):
        for ch in st.session_state.changes:
            conditions = [table.c[key] == value for key, value in ch["old_row"].items()]
            conn.execute(table
                .update()
                .where(and_(*conditions))
                .values(ch["new_row"]))
            conn.commit()
        st.text("The record has been updated. Initializing gold view update.")
        lgroupreversed = dict(zip(ipn_list[ipn]["line_groups"].values(), ipn_list[ipn]["line_groups"].keys()))
        st.code(
            create_single_station(
                conn,
                ipn_list[ipn]["schema"],
                ipn_list[ipn]["md_table"],
                ipn_list[ipn]["results_table"],
                ipn_list[ipn]["table_name_base"].format(lgroupkey=lgroupreversed[linegrp]),
                linegrp,
                station,
                group_pos=True,
                coalesce_tolerance=True,
            ),
            language="sql"
        )
        st.session_state.changes = list()

if len(st.session_state.changes):
    if st.button("Discard changes", type="secondary"):
        st.session_state.changes = list()

if len(st.session_state.changes):
    st.text("There are " + str(len(st.session_state.changes))
        + " unsaved changes in the buffer. Please click the button to save changes and update the golden view."
    )
