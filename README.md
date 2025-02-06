# STAC Cache

A tool for politely making bulk requests for STAC metadata and storing it as geoparquet.

## Background

Many STAC-based workflows rely on making many requests to STAC APIs, which can be a challenge for the analyst who needs to rely on the STAC API, and the API maintainer who needs to handle traffic to the API.
There is at least one STAC client application ([`stacrs`](https://github.com/gadomski/stacrs)) that can be used to search through a STAC geoparquet archive like an API, which could reduce the burden on live APIs if there were more geoparquet archives of STAC metadata out there!
You can also use `duckdb` to run queries on a STAC geoparquet file.

The goal of this project is to make a STAC API -> STAC geoparquet pipeline, inspired by the [Cloud Native Geo blog post](https://cloudnativegeo.org/blog/2024/08/introduction-to-stac-geoparquet/).
