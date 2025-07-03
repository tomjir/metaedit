import os
import json
import pandas as pd
from sqlalchemy import text, create_engine
import streamlit as st


# Databricks setup and initialisation
server_hostname = "adb-5407587042408609.9.azuredatabricks.net"
http_path       = "/sql/1.0/warehouses/e5a56724925b98b1"
access_token    = os.environ["DATABRICKS_TOKEN"]
catalog         = "ps_xplatform_prod"

@st.cache_resource
def get_connection(schema):
    engine = create_engine(
        f"databricks://token:{access_token}@{server_hostname}?http_path={http_path}&catalog={catalog}&schema={schema}"
    )
    return engine


def coalesce(str1, str2):
  if str1 is None or str1 == '':
    return str2
  else:
    return str1


def first_upper(str1):
  return str1[0].capitalize() + str1[1:]


def generate_query(conn, az_schema, az_md_table, lgroup, station, az_results_table='silver_rbcb_results', group_pos=False, coalesce_tolerance=False):
  l1 = ''
  l2 = ''
  if group_pos:
    fv_start = 'first_value('
    fv_start_last = 'last_value('
    fv_end = ', true)'
    session_start = 'session_window.start as '
  else:
    fv_start = ''
    fv_start_last = ''
    fv_end = ''
    session_start = ''

  md_df = pd.read_sql(
      f'''SELECT * FROM {catalog}.{az_md_table} 
          WHERE LINE_GROUP = '{lgroup}' AND LOCATION_STATION IN ({station}) AND IS_ACTIVE = 1
          ORDER BY UPPER(PARAM_NAME_SANIT)''',
      conn
  )

  for row in md_df.iterrows():
    tolerance1 = ''
    tolerance2 = ''
  
    if row[1]['DATA_TYPE'] == 8:
      take = 'result_string'
      cast = 'STRING'
    else:
      take = 'result_value'
      cast = 'DOUBLE'

    if row[1]['IS_TOLERANCE']:
      tolerance1 = '      , CAST(PARAMETER_VALUES["' + row[1]['PARAM_NAME'] + '"]["lower_tolerance"] AS ' + cast + ') AS `'\
        + coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME_SANIT']) + ' [LT]`\n'\
        + '      , CAST(PARAMETER_VALUES["' + row[1]['PARAM_NAME'] + '"]["upper_tolerance"] AS ' + cast + ') AS `'\
        + coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME_SANIT']) + ' [UT]`\n'

      # New feature as of 2025/4/1
      if coalesce_tolerance and row[1]['OUTER_TRANSFORM'] is not None and row[1]['OUTER_TRANSFORM'][:8] == 'COALESCE':
        kolesk_lt = row[1]['OUTER_TRANSFORM'].replace('`,', ' [LT]`,').replace('`)', ' [LT]`)')
        kolesk_ut = row[1]['OUTER_TRANSFORM'].replace('`,', ' [UT]`,').replace('`)', ' [UT]`)')
      else:
        kolesk_lt = '`' + coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME']) + ' [LT]`'
        kolesk_ut = '`' + coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME']) + ' [UT]`'

      tolerance2 = '    , ' + fv_start + kolesk_lt + fv_end + ' AS `'\
        + first_upper(coalesce(row[1]['OUTER_ALIAS'], row[1]['PARAM_NAME_SANIT'])) + ' [LT]`\n'\
        + '    , ' + fv_start + kolesk_ut + fv_end + ' AS `'\
        + first_upper(coalesce(row[1]['OUTER_ALIAS'], row[1]['PARAM_NAME_SANIT'])) + ' [UT]`\n'

    if row[1]['OUTER_DATA_TYPE'] is not None:
      cast = row[1]['OUTER_DATA_TYPE']
    
    getval = 'PARAMETER_VALUES["' + row[1]['PARAM_NAME'] + '"]["' + take + '"]'
    if row[1]['INNER_TRANSFORM'] is not None:
      getval = row[1]['INNER_TRANSFORM'].replace('`' + row[1]['PARAM_NAME'] + '`', getval)

    if cast == 'STRING':
      l1 += '      , ' + getval + ' AS `'
    else:
      l1 += '      , CAST(' + getval + ' AS ' + cast + ') AS `'
    l1 += coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME_SANIT']) + '`\n'\
      + tolerance1

    if row[1]['IS_MART']:
      l2 += '    , ' + fv_start + coalesce(row[1]['OUTER_TRANSFORM'], '`' + coalesce(row[1]['INNER_ALIAS'], row[1]['PARAM_NAME_SANIT']) + '`')\
        + fv_end + ' AS `' + first_upper(coalesce(row[1]['OUTER_ALIAS'], row[1]['PARAM_NAME_SANIT'])) + '`\n'\
        + tolerance2

  az_create_qry = f'''
    SELECT {fv_start}LOCATION_RESULT_UID{fv_end} as LOCATION_RESULT_UID
        , {session_start}RESULT_DATE_LOCAL_TS
        , {fv_start}TYPE_NUMBER{fv_end} as TYPE_NUMBER
        , {fv_start}UNIQUEPART_ID{fv_end} as UNIQUEPART_ID
        , {fv_start}PROCESS_NUMBER{fv_end} as PROCESS_NUMBER
        , {fv_start}WORKCYCLE_COUNTER{fv_end} as WORKCYCLE_COUNTER
        , {fv_start_last}RESULT_STATE_DESCRIPTION{fv_end} as RESULT_STATE_DESCRIPTION
        , {fv_start}LOCATION_ID{fv_end} as LOCATION_ID
        , {fv_start}LINE{fv_end} as LINE
        , {fv_start}LOCATION_STATION{fv_end} as LOCATION_STATION
        , {fv_start}LOCATION_STATINDEX{fv_end} as LOCATION_STATINDEX
        , {fv_start}LOCATION_WORKPOSITION{fv_end} as LOCATION_WORKPOSITION
    ---------- LEVEL_2 ----------
    {l2}
    FROM (
    SELECT LOCATION_RESULT_UID
        , RESULT_DATE_LOCAL_TS
        , TYPE_NUMBER
        , UNIQUEPART_ID
        , PROCESS_NUMBER
        , WORKCYCLE_COUNTER
        , RESULT_STATE_DESCRIPTION
        , LOCATION_ID
        , LINE
        , LOCATION_STATION
        , LOCATION_STATINDEX
        , LOCATION_WORKPOSITION
    ---------- LEVEL_1 ----------
    {l1}
    from {catalog}.{az_schema}.{az_results_table} 
    where LINE_GROUP = '{lgroup}' and LOCATION_STATION IN ({station})
    ) level_1
    '''

  if group_pos:
    az_create_qry += "group by session_window(RESULT_DATE_LOCAL_TS, '60 minutes'), UNIQUEPART_ID, WORKCYCLE_COUNTER"

  return az_create_qry


# Create view for one station
def create_single_station(conn, az_schema, az_md_table, az_results_table, az_table_name_base_format, lgroup, station, station_alias=None, group_pos=True, coalesce_tolerance=False):
  # Use grouping of work-positions?
  gp_df = pd.read_sql(
      f"""select cast(count(distinct LOCATION_WORKPOSITION) as int) as cnt 
          from {catalog}.{az_schema}.{az_results_table}
          where LINE_GROUP = '{lgroup}' and LOCATION_STATION IN ({station})""",
      conn
  )
  group_pos = group_pos and (True if gp_df.loc[0, 'cnt'] > 1 else False)
  az_create_qry = generate_query(conn, az_schema, az_md_table, lgroup, station, az_results_table, group_pos, coalesce_tolerance)
  station_alias = station if station_alias is None else station_alias
  station_alias = station_alias.zfill(4)
  query = f"CREATE OR REPLACE VIEW {catalog}.{az_schema}.{az_table_name_base_format}{station_alias} AS {az_create_qry};"
  conn.execute(text(query))
  conn.commit()
  return query
