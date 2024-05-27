import json
import os
from pathlib import Path
import re

import geopandas as gpd
import requests

from dagster import asset

from tucson_data.resources import EsriRestServicesResource


DATA_DIR = Path("data")
DATA_DIR_SOURCE = DATA_DIR / "source"
DATA_DIR_PROCESSED = DATA_DIR / "processed"
DATA_DIR_OUT = DATA_DIR / "output"


pima_gis_data = EsriRestServicesResource(base_url="https://gisdata.pima.gov/arcgis1/rest/services/")
pag_region = EsriRestServicesResource(base_url="https://maps.pagregion.com/server/rest/services/")


def save_geojson(geojson_data: dict, path: Path) -> None:
    os.makedirs(path.parent, exist_ok=True)

    with open(path, "w") as f:
        json.dump(geojson_data, f)

@asset
def libraries() -> None:
    """
    Pima County Libraries

    See https://gisopendata.pima.gov/datasets/adab4088035540e6bf9babfccd0ee4e6_2/explore?showTable=true
    """
    libraries_geojson = pima_gis_data.get_geojson("GISOpenData/Community/MapServer/2") 
    libraries_geojson_path = DATA_DIR_SOURCE / "libraries" / "libraries.geojson"
    save_geojson(libraries_geojson, libraries_geojson_path)


@asset
def jurisdictional_boundaries() -> None:
    """
    Pima County jurisdiction boundaries

    See https://gisopendata.pima.gov/datasets/472d08edd7324a5bbda029a6d966095d_11/

    """
    boundaries_geojson = pima_gis_data.get_geojson("GISOpenData/Boundaries/MapServer/11") 
    boundaries_geojson_path = DATA_DIR_SOURCE / "jurisdictional_boundaries" / "jurisdictional_boundaries.geojson"
    save_geojson(boundaries_geojson, boundaries_geojson_path)

@asset(deps=[libraries, jurisdictional_boundaries])
def libraries_tucson() -> None:
    """Filter libraries to just those in Tucson"""
    boundaries = gpd.read_file(DATA_DIR_SOURCE / "jurisdictional_boundaries" / "jurisdictional_boundaries.geojson")
    libraries = gpd.read_file(DATA_DIR_SOURCE / "libraries" / "libraries.geojson")

    tucson = boundaries[boundaries["NAME"] == "TUCSON"]

    libraries_tucson = libraries.sjoin(tucson, how="inner", predicate="within")

    rename_cols = {}
    for col in libraries_tucson.columns:
        if col.endswith("_left"):
            col_new = re.sub(r"_left$", "", col)
            rename_cols[col] = col_new

    libraries_tucson = libraries_tucson.rename(columns=rename_cols)

    drop_cols = []
    for col in libraries_tucson.columns:
        if col not in libraries.columns:
            drop_cols.append(col)

    libraries_tucson = libraries_tucson.drop(columns=drop_cols)

    libraries_tucson_path = DATA_DIR_OUT / "libraries" / "libraries_tucson.geojson"
    libraries_tucson.to_file(libraries_tucson_path)

@asset(deps=[libraries_tucson])
def libraries_tucson_csv() -> None:
    """Convert libraries GeoJSON to CSV"""
    libraries_tucson_path = DATA_DIR_OUT / "libraries" / "libraries_tucson.geojson"
    libraries_tucson = gpd.read_file(libraries_tucson_path)

    libraries_tucson_csv_path = DATA_DIR_OUT / "libraries" / "libraries_tucson.csv"
    os.makedirs(libraries_tucson_path.parent, exist_ok=True)
    libraries_tucson.to_csv(libraries_tucson_csv_path, index=False)

@asset
def bicycle_routes() -> None:
    """Download bicycle route GeoJSON"""
    routes_geojson = pima_gis_data.get_geojson("GISOpenData/Transportation/MapServer/2") 
    routes_geojson_path = DATA_DIR_SOURCE / "bicycle_routes" / "bicycle_routes.geojson"
    save_geojson(routes_geojson, routes_geojson_path)

@asset(deps=[bicycle_routes, jurisdictional_boundaries])
def bicycle_routes_tucson() -> None:
    """Filter bicycle routes to just those in Tucson"""
    boundaries = gpd.read_file(DATA_DIR_SOURCE / "jurisdictional_boundaries" / "jurisdictional_boundaries.geojson")
    routes = gpd.read_file(DATA_DIR_SOURCE / "bicycle_routes" / "bicycle_routes.geojson")

    tucson = boundaries[boundaries["NAME"] == "TUCSON"]

    routes_tucson = routes.sjoin(tucson, how="inner", predicate="within")

    rename_cols = {}
    for col in routes_tucson.columns:
        if col.endswith("_left"):
            col_new = re.sub(r"_left$", "", col)
            rename_cols[col] = col_new

    routes_tucson = routes_tucson.rename(columns=rename_cols)

    drop_cols = []
    for col in routes_tucson.columns:
        if col not in routes.columns:
            drop_cols.append(col)

    routes_tucson = routes_tucson.drop(columns=drop_cols)

    routes_tucson_path = DATA_DIR_OUT / "bicycle_routes" / "bicycle_routes_tucson.geojson"
    os.makedirs(routes_tucson_path.parent, exist_ok=True)
    routes_tucson.to_file(routes_tucson_path)

@asset
def bikeways() -> None:
    bikeways_geojson = pag_region.get_geojson("/Bikemap/TucsonMetroBikeMap/MapServer/4") 
    bikeways_geojson_path = DATA_DIR_SOURCE / "bikeways" / "bikeways.geojson"
    save_geojson(bikeways_geojson, bikeways_geojson_path)

