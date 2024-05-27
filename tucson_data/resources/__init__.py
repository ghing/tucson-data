from pydantic import Field
import requests

from dagster import ConfigurableResource


class EsriRestServicesResource(ConfigurableResource):
    """Resource for fetching data from an ESRI Rest Services API"""
    base_url: str = Field(
        description=(
            "Base URL for API. Example: "
            "'https://gisdata.pima.gov/arcgis1/rest/services/'"        
        )
    )

    def get_geojson(self, path):
        base_url = self.base_url.rstrip("/")
        path = path.rstrip("/")
        endpoint_url = f"{self.base_url}/{path}/query"

        params = {
            "where": "1=1",
            "text": "",
            "objectIds": "",
            "time": "",
            "timeRelation": "esriTimeRelationOverlaps",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "",
            "spatialRel": "esriSpatialRelIntersects",
            "distance": "",
            "units": "esriSRUnit_Foot",
            "relationParam": "",
            "outFields": "*",
            "returnGeometry": "true",
            "returnTrueCurves": "false",
            "maxAllowableOffset": "",
            "geometryPrecision": "",
            "outSR": "",
            "havingClause": "",
            "returnIdsOnly": "false",
            "returnCountOnly": "false",
            "orderByFields": "",
            "groupByFieldsForStatistics": "",
            "outStatistics": "",
            "returnZ": "false",
            "returnM": "false",
            "gdbVersion": "",
            "historicMoment": "",
            "returnDistinctValues": "false",
            "resultOffset": 0,
            "resultRecordCount": "",
            "returnExtentOnly": "false", 
            "sqlFormat": "none",
            "datumTransformation": "",
            "parameterValues": "",
            "rangeValues": "",
            "quantizationParameters": "",
            "featureEncoding": "esriDefault",
            "f": "geojson",
        }
        continue_fetching = True
        geojson = {
            "type": "FeatureCollection",
            "features": [],
        } 

        while continue_fetching:
            data = requests.get(endpoint_url, params=params).json()
            geojson["features"] = geojson["features"] + data["features"]
            continue_fetching = data.get("exceededTransferLimit", False) 
            params["resultOffset"] += 2000

        return geojson

