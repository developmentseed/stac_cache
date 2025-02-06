# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyarrow",
#     "pystac",
#     "pystac-client",
#     "stac_geoparquet",
#     "stacrs",
#     "retry",
#     "s3fs",
#     "tqdm",
# ]
# ///

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pystac
import pystac_client
import stac_geoparquet
from retry import retry
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


@retry(tries=3, delay=30, backoff=2)
def search_pystac_client(
    client: pystac_client.Client, *args, **kwargs
) -> pystac.ItemCollection:
    """Execute a STAC search with any search parameters."""
    search = client.search(*args, **kwargs)

    items = search.item_collection()
    if limit := kwargs.get("limit"):
        if len(items) == limit:
            raise ValueError(
                f"this query returned exactly the limit of {limit} items! "
                f"that means these results are probably incomplete {kwargs}"
            )

    return items


async def main():
    args = parse_args()

    if args.end_date < args.start_date:
        raise ValueError("End date must be after start date")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # generate set of bboxes
    bbox_combinations = chunk_bbox(
        args.bbox,
        x_chunk_size=args.x_chunk_size,
        y_chunk_size=args.y_chunk_size,
    )

    # generate date range
    date_range = [
        args.start_date + timedelta(days=x)
        for x in range((args.end_date - args.start_date).days + 1)
    ]

    # create base search parameters
    base_params = {
        "collections": args.collections,
        "limit": args.limit,
    }

    # Add optional parameters
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

    all_files = []
    max_workers = min(len(search_params), args.max_workers)

    executor = ThreadPoolExecutor(max_workers=max_workers)

    try:
        progress_bar = tqdm(
            total=len(search_params), desc="Processing searches", unit="search"
        )
        client = pystac_client.Client.open(args.stac_api)

        future_to_params = {
            executor.submit(search_pystac_client, client, **params): params
            for params in search_params
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_params):
            params = future_to_params[future]
            try:
                items = future.result()
                if not items:
                    continue
                chunk_file = (
                    intermediate_dir
                    / f"{params['datetime'][:10]}_{'_'.join(str(coord) for coord in params['bbox'])}.parquet"
                )
                rbr = stac_geoparquet.arrow.parse_stac_items_to_arrow(items)
                stac_geoparquet.arrow.to_parquet(rbr, chunk_file)
                all_files.append(chunk_file)

            except Exception as exc:
                print(f"Search with params {params} generated an exception: {exc}")
            finally:
                progress_bar.update(1)

        progress_bar.close()
    finally:
        executor.shutdown()

    print(f"Wrote results to {len(all_files)} parquet files")

    # Combine all parquet files
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
