# STAC Cache

A tool for politely making bulk requests for STAC metadata and storing it as geoparquet.

## Background

Many STAC-based workflows rely on making many requests to STAC APIs, which can be a challenge for the analyst who needs to rely on the STAC API, and the API maintainer who needs to handle traffic to the API.
There is at least one STAC client application ([`stacrs`](https://github.com/gadomski/stacrs)) that can be used to search through a STAC geoparquet archive like an API, which could reduce the burden on live APIs if there were more geoparquet archives of STAC metadata out there!
You can also use `duckdb` to run queries on a STAC geoparquet file.

The goal of this project is to make a STAC API -> STAC geoparquet pipeline, inspired by the [Cloud Native Geo blog post](https://cloudnativegeo.org/blog/2024/08/introduction-to-stac-geoparquet/).

## Installation

Install `uv`, then run:

```bash
uv sync
```

## Usage

### Example: cache HLS records for the circumpolar boreal region for July-August 2019, 2022, 2023

```bash
time uv run stac_cache.py \
  --stac-api=https://cmr.earthdata.nasa.gov/stac/LPCLOUD \
  --collections HLSS30_2.0 HLSL30_2.0 \
  --bbox=-180,30,180,80 \
  --start-date=2023-07-01 \
  --end-date=2023-08-31 \
  --limit=2000 \
  --output=hls_boreal_20230701-20230831 \
  --max-workers=2 \
  --x_chunk_size=120 \
  --y_chunk_size=10

time uv run stac_cache.py \
  --stac-api=https://cmr.earthdata.nasa.gov/stac/LPCLOUD \
  --collections HLSS30_2.0 HLSL30_2.0 \
  --bbox=-180,30,180,80 \
  --start-date=2022-07-01 \
  --end-date=2022-08-31 \
  --limit=2000 \
  --output=hls_boreal_20220701-20220831 \
  --max-workers=2 \
  --x_chunk_size=120 \
  --y_chunk_size=10

time uv run stac_cache.py \
  --stac-api=https://cmr.earthdata.nasa.gov/stac/LPCLOUD \
  --collections HLSS30_2.0 HLSL30_2.0 \
  --bbox=-180,30,180,80 \
  --start-date=2019-07-01 \
  --end-date=2019-08-31 \
  --limit=2000 \
  --output=hls_boreal_20190701-20190831 \
  --max-workers=2 \
  --x_chunk_size=120 \
  --y_chunk_size=10
```

### Example: cache sentinel-2-l2a records from Microsoft Planetary Computer

```bash
time uv run stac_cache.py \
  --stac-api=https://planetarycomputer.microsoft.com/api/stac/v1 \
  --collections sentinel-2-l2a \
  --bbox=-180,30,0,80 \
  --start-date=2023-07-01 \
  --end-date=2023-07-05 \
  --limit=1000 \
  --output=/tmp/sentinel-2 \
  --max-workers=4 \
  --x_chunk_size=60 \
  --y_chunk_size=10
```
