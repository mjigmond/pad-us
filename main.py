from typing import Union
import logging

import psycopg2
from fastapi import FastAPI
import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine, create_engine

from constants import (
    PAD_REQUESTS_SCHEMA, PAD_TABLES, PAD_COMPUTE_SCHEMA, PAD_SCHEMA, DB_ARGS, GROUPBY_OPTIONS
)

app = FastAPI()


@app.get("/")
def read_root():
    return {"Nothing here": "Really"}


@app.get("/percent-coverage/")
def percent_coverage(aoi_url: str, groupby: Union[str, None] = None) -> dict:
    """
    Generate percent coverage for an AOI (provided as a GeoJSON url) that intersects various
    PAD areas. The output can be a single percent cover, or it can be grouped by specific PAD
    attributes (see constants.GROUPBY_OPTIONS).
    Args:
        aoi_url: url to an AOI GeoJSON
        groupby: comma delimited fields to aggregate percent coverage by

    Returns:
    Return a dictionary of percent overlap as a total or by `groupby` fields

    Example API url required for this call:
    http://localhost:8000/percent-coverage/?aoi_url="https://www/to/a/GeoJSON/AOI.geojson"&groupby="featclass,mang_type"
    """
    engine = get_engine("pad_us")
    aoi_df = parse_geojson(aoi_url)
    if aoi_df.empty:
        return {"Failure": "Unable to process AOI GeoJSON"}
    aoi_df_hash = pd.util.hash_pandas_object(aoi_df).sum()
    aoi_past_request = past_request(aoi_df_hash)
    aoi_table_name = f"t_{aoi_df_hash}"
    if not aoi_past_request:
        aoi_df.to_postgis(aoi_table_name, engine, PAD_REQUESTS_SCHEMA)

    engine = get_engine("pad_us")
    aoi_df = parse_geojson(aoi_url)
    if aoi_df.empty:
        return {"Failure": "Unable to process AOI GeoJSON"}
    aoi_df_hash = pd.util.hash_pandas_object(aoi_df).sum()
    aoi_past_request = past_request(aoi_df_hash)
    aoi_table_name = f"t_{aoi_df_hash}"
    if not aoi_past_request:
        aoi_df.to_postgis(aoi_table_name, engine, PAD_REQUESTS_SCHEMA)

    create_compute_table(aoi_table_name)
    compute_overlap(aoi_table_name)

    agg_fields = get_groupby(groupby)

    """
    Final query should just return the data from the compute table
    aggregated using provided fields or just an overall percentage.
    """

    return


def get_engine(dbname: str) -> Engine:
    """
    Creates a DB sqlalchemy engine needed by Geopandas' sql methods
    Args:
        dbname: name of the database to connect to

    Returns:
    Sqlalchemy engine
    """
    return create_engine(f"postgresql://localhost:5432/{dbname}")


def parse_geojson(gjson_url: str) -> gpd.GeoDataFrame:
    """
    Parse a GeoJSON (url) and return a geodataframe
    Args:
        gjson_url: GeoJSON url

    Returns:
    Returns a non-empty geodataframe if parsing was successful, otherwise an empty one.
    """
    try:
        df = gpd.read_file(gjson_url)
        return df
    except:
        logging.warning(f"Failed to parse GeoJSON at {gjson_url}")
        return gpd.GeoDataFrame()
    

def past_request(hash: int) -> bool:
    """
    Using a dataframe hash check to see if we've already processed the dataframe
    into a PostGIS table.
    Args:
        hash: dataframe hash

    Returns:
    True if the dataframe already exists as a db table, False otherwise.
    """
    table_name = f"t_{hash}"
    table_exists = pd.read_sql((
        "select table_name from information_schema.tables"
        f" where table_name = '{table_name}' and table_schema = '{PAD_COMPUTE_SCHEMA}';"
    ))
    if table_exists.empty:
        return False
    return True


def create_compute_table(table_name: str):
    """
    Create the output table that will store the results of the overlap computation.
    Args:
        table_name: name of the table to be created
    """
    fqtn = f"{PAD_COMPUTE_SCHEMA}.{table_name}"
    with psycopg2.connect(**DB_ARGS) as conn:
        with conn.cursor() as cursor:
            cursor.execute((
                f"drop table if exists {fqtn};"
                f"create table {fqtn} "
                "(pad_name text, featclass text, mang_type text, des_tp text, "
                "overlap_fraction double precision);"
            ))


def compute_overlap(table_name: str):
    """
    Compute the overlap between the AOI and any pre-determined PAD tables (see constants.PAD_TABLES).
    The results from multiple table joins are combined into a single output table. The source PAD table
    is maintained in the `pad_name` attribute of the output table.
    Args:
        table_name: name of the table that will hold the output of the computation
    """
    fqtn = f"{PAD_COMPUTE_SCHEMA}.{table_name}"
    base_query = """
        (select 
            '{PAD}', pad.featclass, pad.mang_type, pad.des_tp,
            st_area(st_intersection(aoi.geom, pad.geom)::geography) / st_area(aoi.geom::geography) as overlap
        from
            {fqtn} aoi
        join
            {PAD_SCHEMA}.{PAD} pad
        on
            st_intersects(aoi.geom, pad.geom)
        )
    """
    union_query = "\nunion\n".join(
        [base_query.format(PAD=tbl, PAD_SCHEMA=PAD_SCHEMA, fqtn=fqtn) for tbl in PAD_TABLES]
    )
    with psycopg2.connect(**DB_ARGS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"insert into {fqtn} ({union_query});"
            )


def get_groupby(groupby: Union[str, None]) -> Union[set[str], None]:
    """
    Generate a set of columns to aggregate the percent cover by using the API query parameter `groupby`.
    The requested list is checked against a predefined list of groupby options (see constants.GROUPBY_OPTIONS).
    Args:
        groupby:

    Returns:
    Set of attributes to group by if they are valid, otherwise None.
    """
    if groupby is not None:
        groupby_fields = [f.lower() for f in groupby.split(",")]
        agg_fields = GROUPBY_OPTIONS.intersection(groupby_fields)
        if agg_fields:
            return agg_fields
    return None
