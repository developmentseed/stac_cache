# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "backoff",
#     "httpx",
#     "pyarrow",
#     "pystac",
#     "stac_geoparquet",
#     "retry",
#     "s3fs",
#     "tqdm",
# ]
# ///

import argparse
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import backoff
import httpx
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pystac
import stac_geoparquet
from tqdm import tqdm


def parse_bbox_string(bbox_str: str) -> tuple[float, float, float, float]:
    """Parse a comma-separated bbox string into a tuple of floats."""
    try:
        bbox = tuple(map(float, bbox_str.split(",")))
        if len(bbox) != 4:
            raise ValueError
        xmin, ymin, xmax, ymax = bbox
        if not (
            -180 <= xmin <= 180
            and -180 <= xmax <= 180
            and -90 <= ymin <= 90
            and -90 <= ymax <= 90
        ):
            raise ValueError
        if xmin >= xmax or ymin >= ymax:
            raise ValueError
        return bbox
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Bbox must be in format 'xmin,ymin,xmax,ymax' with valid coordinates"
        )


def chunk_bbox(
    bbox: tuple[float, float, float, float],
    x_chunk_size: float = 180.0,
    y_chunk_size: float = 20.0,
) -> list[tuple[float, float, float, float]]:
    """Split a bounding box into smaller chunks."""
    xmin, ymin, xmax, ymax = bbox

    # Generate x and y coordinates for chunks
    x_chunks = []
    current_x = xmin
    while current_x < xmax:
        next_x = min(current_x + x_chunk_size, xmax)
        x_chunks.append((current_x, next_x))
        current_x = next_x

    y_chunks = []
    current_y = ymin
    while current_y < ymax:
        next_y = min(current_y + y_chunk_size, ymax)
        y_chunks.append((current_y, next_y))
        current_y = next_y

    # Create all bbox combinations
    return [(x1, y1, x2, y2) for (x1, x2) in x_chunks for (y1, y2) in y_chunks]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Search STAC API and save results to parquet file"
    )
    parser.add_argument(
        "--stac-api",
        help="STAC API endpoint",
    )
    parser.add_argument(
        "--collections",
        type=str,
        nargs="+",
        help="STAC collections to search",
    )
    parser.add_argument(
        "--bbox",
        type=parse_bbox_string,
        required=True,
        help="Bounding box in format 'xmin,ymin,xmax,ymax'",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        required=True,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--filter",
        type=str,
        help="CQL2 filter expression",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        help="CQL2 filter expression",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output parquet file path",
    )
    parser.add_argument(
        "--x-chunk-size",
        type=float,
        default=180.0,
        help="Size of x-axis chunks in degrees (default: 180.0)",
    )
    parser.add_argument(
        "--y-chunk-size",
        type=float,
        default=10.0,
        help="Size of y-axis chunks in degrees (default: 20.0)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of concurrent workers (default: 4)",
    )
    parser.add_argument(
        "--client",
        default="pystac-client",
        help="STAC search client - either stacrs or pystac-client",
    )
    return parser.parse_args()


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, Exception),
    max_tries=3,
    max_time=300,
)
async def search_stac_api(
    client: httpx.AsyncClient, api_url: str, params: Dict[Any, Any]
) -> dict:
    """Execute a STAC search using httpx."""
    search_url = f"{api_url}/search"

    # Initialize with first response
    response = await client.post(search_url, json=params)
    response.raise_for_status()
    result = response.json()

    # Store all features
    all_features = result.get("features", [])

    # Keep track of number of pages for logging
    page_count = 1

    while True:
        # Look for next link
        next_link = None
        for link in result.get("links", []):
            if link["rel"] == "next":
                next_link = link["href"]
                break

        if not next_link:
            break

        # Get next page
        response = await client.get(next_link)
        response.raise_for_status()
        result = response.json()

        # Add features from this page
        all_features.extend(result.get("features", []))
        page_count += 1

    print(f"Retrieved {len(all_features)} items across {page_count} pages")

    # Construct final result maintaining the original structure
    final_result = {
        "type": "FeatureCollection",
        "features": all_features,
        "links": result.get("links", []),  # Keep the links from the last page
        "context": result.get("context", {}),  # Keep any context from the last page
    }

    return final_result


async def process_search_batch(
    client: httpx.AsyncClient,
    api_url: str,
    search_params: Dict[str, Any],
    intermediate_dir: Path,
    semaphore: asyncio.Semaphore,
) -> list[Path]:
    """Process a batch of search parameters and save results."""
    all_files = []

    async with semaphore:
        try:
            result = await search_stac_api(client, api_url, search_params)

            if not result.get("features"):
                return []

            # Create filename from parameters
            chunk_file = (
                intermediate_dir
                / f"{search_params['datetime'][:10]}_{'_'.join(str(coord) for coord in search_params['bbox'])}.parquet"
            )

            # Convert to arrow and save
            items = pystac.ItemCollection(result["features"])
            rbr = stac_geoparquet.arrow.parse_stac_items_to_arrow(items)
            stac_geoparquet.arrow.to_parquet(rbr, chunk_file)

            all_files.append(chunk_file)

        except Exception as exc:
            print(f"Search with params {search_params} generated an exception: {exc}")

    return all_files


async def main():
    args = parse_args()

    if args.end_date < args.start_date:
        raise ValueError("End date must be after start date")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # generate set of bboxes and dates (same as before)
    bbox_combinations = chunk_bbox(
        args.bbox,
        x_chunk_size=args.x_chunk_size,
        y_chunk_size=args.y_chunk_size,
    )

    date_range = [
        args.start_date + timedelta(days=x)
        for x in range((args.end_date - args.start_date).days + 1)
    ]

    # create base search parameters
    base_params = {
        "collections": args.collections,
        "limit": args.limit,
    }

    if args.filter:
        base_params["filter"] = args.filter

    # create set of search parameters
    search_params = [
        {
            **base_params,
            "bbox": bbox,
            "datetime": f"{date.strftime('%Y-%m-%dT00:00:00Z')}/{date.strftime('%Y-%m-%dT23:59:59Z')}",
        }
        for bbox in bbox_combinations
        for date in date_range
    ]

    print(f"Created {len(search_params)} search parameter combinations")
    print(f"Date range: {args.start_date.date()} to {args.end_date.date()}")
    print(f"Number of dates: {len(date_range)}")
    print(f"Number of bbox combinations: {len(bbox_combinations)}")

    # Setup filesystem
    if str(args.output).startswith("s3://"):
        s3 = fs.S3FileSystem()
        output_fs = s3
        output_path = str(args.output).replace("s3://", "")
    else:
        output_fs = fs.LocalFileSystem()
        output_path = str(args.output)

    # Create intermediate directory
    intermediate_dir = Path("/tmp") / args.output.stem
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    # Create semaphore for controlling concurrency
    semaphore = asyncio.Semaphore(args.max_workers)

    all_files = []

    async with httpx.AsyncClient(timeout=300) as client:
        tasks = [
            process_search_batch(
                client, args.stac_api, params, intermediate_dir, semaphore
            )
            for params in search_params
        ]

        # Use tqdm to show progress
        for task in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Processing searches",
            unit="search",
        ):
            files = await task
            all_files.extend(files)

    print(f"Wrote results to {len(all_files)} parquet files")

    # Combine all parquet files (same as before)
    if all_files:
        print(f"Combining {len(all_files)} files into {args.output}")

        dataset = ds.dataset(all_files, format="parquet")
        table = dataset.to_table()

        pq.write_table(table, output_path, filesystem=output_fs)

        for temp_file in all_files:
            Path(temp_file).unlink()

        intermediate_dir.rmdir()

        print(f"Successfully combined all files into {args.output}")
    else:
        print("No results found to combine")


if __name__ == "__main__":
    asyncio.run(main())
