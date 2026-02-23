"""One-time setup: download NLCD 2021 Tree Canopy Cover for CONUS and extract PNW region.

Usage:
    python3 scripts/download_nlcd.py

Downloads:
    https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/nlcd_tcc_CONUS_2021_v2021-4.zip
    (~3.7 GB zip, ~3.5 GB extracted TIF)

Output:
    data/raw/nlcd_canopy_pnw.tif  — PNW window clipped from the CONUS TIF, reprojected to WGS84

The CONUS TIF is Albers (EPSG:5070). We clip to the PNW bounding box and reproject
to WGS84 so habitat_refinement.py can do simple lat/lon pixel lookups without pyproj.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import requests

DOWNLOAD_URL = (
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/"
    "nlcd_tcc_CONUS_2021_v2021-4.zip"
)
ZIP_PATH = Path("data/raw/nlcd_tcc_CONUS_2021_v2021-4.zip")
CONUS_TIF = Path("data/raw/nlcd_tcc_conus_2021_v2021-4.tif")
OUTPUT_PATH = Path("data/raw/nlcd_canopy_pnw.tif")

# PNW extent in WGS84
LON_MIN, LON_MAX = -125.0, -116.0
LAT_MIN, LAT_MAX = 42.0, 49.0


def download_zip() -> None:
    ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading NLCD 2021 TCC CONUS zip (~3.7 GB)...", flush=True)
    print(f"  → {ZIP_PATH}", flush=True)

    with requests.get(DOWNLOAD_URL, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with ZIP_PATH.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"  {downloaded / 1e9:.2f} GB / {total / 1e9:.2f} GB ({pct:.0f}%)",
                          flush=True)
                else:
                    print(f"  {downloaded / 1e9:.2f} GB downloaded", flush=True)

    print(f"Download complete: {ZIP_PATH.stat().st_size / 1e9:.2f} GB", flush=True)


def extract_zip() -> None:
    print(f"Extracting zip...", flush=True)
    with zipfile.ZipFile(ZIP_PATH) as zf:
        members = zf.namelist()
        tif_members = [m for m in members if m.lower().endswith(".tif")]
        print(f"  TIF files in zip: {tif_members}", flush=True)
        for m in tif_members:
            zf.extract(m, ZIP_PATH.parent)
            extracted = ZIP_PATH.parent / m
            print(f"  Extracted: {extracted} ({extracted.stat().st_size / 1e9:.2f} GB)", flush=True)


def clip_to_pnw() -> None:
    """Clip CONUS TIF to PNW window and reproject to WGS84 using rasterio/GDAL."""
    try:
        import rasterio
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        from rasterio.crs import CRS
    except ImportError:
        raise RuntimeError("rasterio is required: pip install rasterio")

    wgs84 = CRS.from_epsg(4326)

    print(f"Clipping and reprojecting CONUS TIF to PNW WGS84...", flush=True)
    with rasterio.open(CONUS_TIF) as src:
        print(f"  Source CRS: {src.crs}  size: {src.width}×{src.height}", flush=True)

        # Calculate transform for the PNW window in WGS84
        transform, width, height = calculate_default_transform(
            src.crs, wgs84, src.width, src.height, *src.bounds,
            dst_width=int((LON_MAX - LON_MIN) / 0.0002698),   # ~30m in degrees
            dst_height=int((LAT_MAX - LAT_MIN) / 0.0002698),
        )

        # Clip: only reproject the PNW bounding box
        from rasterio.transform import from_bounds
        dst_width = round((LON_MAX - LON_MIN) / 0.0002698)
        dst_height = round((LAT_MAX - LAT_MIN) / 0.0002698)
        dst_transform = from_bounds(LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, dst_width, dst_height)

        kwargs = src.meta.copy()
        kwargs.update({
            "crs": wgs84,
            "transform": dst_transform,
            "width": dst_width,
            "height": dst_height,
        })

        print(f"  Output: {dst_width}×{dst_height} pixels at ~30m in WGS84", flush=True)
        with rasterio.open(OUTPUT_PATH, "w", **kwargs) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=dst_transform,
                dst_crs=wgs84,
                resampling=Resampling.nearest,
            )

    size_mb = OUTPUT_PATH.stat().st_size / 1e6
    print(f"  Written: {OUTPUT_PATH} ({size_mb:.0f} MB)", flush=True)

    # Verify
    with rasterio.open(OUTPUT_PATH) as ds:
        print(f"Verification: CRS={ds.crs}  size={ds.width}×{ds.height}  "
              f"bounds={[round(x,2) for x in ds.bounds]}", flush=True)


def main() -> None:
    if not ZIP_PATH.exists():
        download_zip()
    else:
        print(f"Zip already exists ({ZIP_PATH.stat().st_size / 1e9:.2f} GB), skipping download.",
              flush=True)

    if not CONUS_TIF.exists():
        extract_zip()
    else:
        print(f"CONUS TIF already extracted, skipping.", flush=True)

    if OUTPUT_PATH.exists():
        print(f"PNW TIF already exists at {OUTPUT_PATH}, skipping clip.", flush=True)
    else:
        clip_to_pnw()

    print("\nDone. Run the refinement pipeline to use real canopy values:", flush=True)
    print("  python3 -m app.pipelines.habitat_refinement", flush=True)


if __name__ == "__main__":
    main()
