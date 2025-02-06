import argparse
import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable

import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pystac_client
import stac_geoparquet
from tqdm import tqdm


def generate_params_hash(args: argparse.Namespace) -> str:
    """Generate a hash from the request parameters."""
    params = {
        "collections": sorted(args.collections),  # Sort for consistency
        "bbox": args.bbox,
        "stac_api": args.stac_api,
        "filter": args.filter,
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
    }

    params_str = json.dumps(params, sort_keys=True)

    return hashlib.sha256(params_str.encode()).hexdigest()[:12]


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
        help="Output directory for geoparquet archive",
    )
    parser.add_argument(
        "--x-chunk-size",
        type=float,
        default=60.0,
        help="Size of x-axis chunks in degrees (default: 60.0)",
    )
    parser.add_argument(
        "--y-chunk-size",
        type=float,
        default=10.0,
        help="Size of y-axis chunks in degrees (default: 10.0)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum number of concurrent workers (default: 2)",
    )
    return parser.parse_args()


def write_to_parquet(features: Iterable[Dict[str, Any]]) -> str:
    chunk_file = NamedTemporaryFile(suffix=".parquet").name
    rbr = stac_geoparquet.arrow.parse_stac_items_to_arrow(features)
    stac_geoparquet.arrow.to_parquet(rbr, chunk_file)

    return chunk_file


async def main():
    args = parse_args()

    if args.end_date < args.start_date:
        raise ValueError("End date must be after start date")

    bbox_combinations = chunk_bbox(
        args.bbox,
        x_chunk_size=args.x_chunk_size,
        y_chunk_size=args.y_chunk_size,
    )

    date_range = [
        args.start_date + timedelta(days=x)
        for x in range((args.end_date - args.start_date).days + 1)
    ]

    base_params = {
        "collections": args.collections,
        "limit": args.limit,
    }

    if args.filter:
        base_params["filter"] = args.filter

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

    params_hash = generate_params_hash(args)
    output_path = args.output / f"stac_cache_{params_hash}.parquet"

    # Setup filesystem
    if str(args.output).startswith("s3://"):
        s3 = fs.S3FileSystem()
        output_fs = s3
        output_path_str = str(output_path).replace("s3://", "")
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        output_fs = fs.LocalFileSystem()
        output_path_str = str(output_path)

    max_workers = min(len(search_params), args.max_workers)
    executor = ThreadPoolExecutor(max_workers=max_workers)

    features = []
    ids = []
    files = []

    try:
        client = pystac_client.Client.open(args.stac_api)

        # Submit all searches as futures
        futures = [
            executor.submit(
                lambda p: client.search(method="GET", **p).item_collection_as_dict(),
                params,
            )
            for params in search_params
        ]

        # Process futures as they complete
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                search_features = future.result()
                for feature in search_features.get("features", []):
                    if feature["id"] not in ids:
                        features.append(feature)
                        ids.append(feature["id"])

                if len(features) > 1e4:
                    chunk_file = write_to_parquet(features)
                    print(f"chunk written to {chunk_file}")
                    files.append(chunk_file)
                    features = []

            except Exception as exc:
                print(f"Search generated an exception: {exc}")

        # write any remaining features to disk
        if features:
            chunk_file = write_to_parquet(features)
            print(f"chunk written to {chunk_file}")
            files.append(chunk_file)

    finally:
        executor.shutdown()

    print(f"Wrote {len(ids)} features to {len(files)} intermediate parquet files")

    if files:
        print(f"Combining {len(files)} files into {output_path}")

        dataset = ds.dataset(files, format="parquet")
        table = dataset.to_table()
        pq.write_table(table, output_path_str, filesystem=output_fs)

        for temp_file in files:
            Path(temp_file).unlink()

        print(f"Successfully combined all files into {output_path}")
    else:
        print("No results found to combine")


if __name__ == "__main__":
    asyncio.run(main())
