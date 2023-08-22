# PAD-US Coverage Tool

This POC implements an analysis approach for determining PAD coverage for a specific AOI.
The tool uses the USGS [PAD-US database](https://maps.usgs.gov/padusdataexplorer/#/protected-areas). I downloaded the Geopackage version and loaded it into my local PostgreSQL+PostGIS database.

The tool implements (when complete) a basic REST API using FastAPI that accepts a GeoJSON URL which will be evaluated against different types of PAD areas. The percent coverage output can be aggregated per specific feature classes, manager types, or designation types.

The following URL request could be used to request percent coverage for an AOI:
```
http://localhost:8000/percent-coverage/?aoi_url="https://www/to/a/GeoJSON/AOI.geojson"&groupby="featclass,mang_type"
```

When functional, I expect the response will look something like:
```json
{
  "percent_coverage": {
    "total": 0.52,
    "featclass": [
      {"class_a": 0.05},
      {"class_b": 0.25},
      {"class_a": 0.15}
    ],
    "mang_types": [
      {"mang_a": 0.07}
    ]
  }
}
```
