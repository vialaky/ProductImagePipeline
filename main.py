import requests
from fake_useragent import UserAgent
from pathlib import Path
import json
from datetime import datetime
from loguru import logger
import sys
import zipfile
import tarfile

# =========================
# Configuration dictionary
# =========================
config = {
    "project_name": "ProductImagePipeline",
    "output_dir": "output",
    "logs_dir": "logs",
    "image_profile": {
        "mode": "instagram_portrait",
        "size": (1080, 1350),
        "background": "white",
        "fit": "contain",
        "quality": 85,
        "progressive": True
    },
    "skus": [
        {
            "sku": "AAA-0001",
            "source_url": "https://github.com/Horea94/Fruit-Images-Dataset/archive/refs/heads/master.zip",
            "category": "Fruits"
        }
    ]
}

# =========================
# Logging setup
# =========================
# Human-readable time format: YYYY-MM-DD HH:mm:ss
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")
logger.add("pipeline.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO", rotation="1 MB")

# =========================
# Report function
# =========================
def update_report(sku, filename, size, archive_type, download_status=None, extract_status=None):
    """
    Update report.json with both download and extract status in a single entry per SKU.
    If the entry exists, update it; otherwise, create a new one.
    """
    report_path = Path("data") / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Read existing report
    if report_path.exists():
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    # Find existing entry for this SKU
    entry = next((item for item in data if item["sku"] == sku), None)

    if entry:
        # Update existing entry
        if download_status:
            entry["download_status"] = download_status
        if extract_status:
            entry["extract_status"] = extract_status
    else:
        # Create new entry
        entry = {
            "time": now,
            "sku": sku,
            "filename": filename,
            "size": size,
            "archive type": archive_type,
            "download_status": download_status,
            "extract_status": extract_status
        }
        data.append(entry)

    # Write back to JSON
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# =========================
# Archive type detection
# =========================
def detect_archive_type(filepath: Path) -> str:
    """
    Detect archive type based on file suffix.
    """
    suffixes = filepath.suffixes  # e.g., ['.tar', '.gz']
    if suffixes == ['.tar', '.gz']:
        return "targz"
    if filepath.suffix == '.tgz':
        return "tgz"
    if filepath.suffix == '.zip':
        return "zip"
    if filepath.suffix == '.tar':
        return "tar"
    return "unknown"

# =========================
# Download function
# =========================
def download_archive(url, sku):
    """
    Download archive for a given SKU.
    """
    ua = UserAgent()
    random_ua = ua.random

    target_dir = Path("data") / sku / "download"
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    filepath = target_dir / filename
    archive_type = detect_archive_type(filepath)

    if filepath.exists() and filepath.stat().st_size > 0:
        logger.info(f"{sku}: already downloaded, skipping")
        update_report(sku, filename, filepath.stat().st_size, archive_type, download_status="success")
        return filepath, archive_type

    for attempt in range(1, 4):
        try:
            response = requests.get(url, headers={'User-Agent': random_ua}, stream=True, timeout=30)
            if response.status_code == 200:
                logger.info(f"{sku}: downloading {filename} (attempt {attempt})")
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1048576):
                        if chunk:
                            f.write(chunk)
                size = filepath.stat().st_size
                if size > 0:
                    logger.info(f"{sku}: download complete, size {size} bytes")
                    update_report(sku, filename, size, archive_type, download_status="success")
                    return filepath, archive_type
                else:
                    logger.error(f"{sku}: empty file (attempt {attempt})")
            else:
                logger.error(f"{sku}: HTTP {response.status_code} (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            logger.error(f"{sku}: network error {e} (attempt {attempt})")

    logger.error(f"{sku}: failed after 3 attempts")
    update_report(sku, filename, 0, archive_type, download_status="failed")
    return filepath, archive_type

# =========================
# Extract function
# =========================
def extract_archive(sku, filepath, archive_type):
    """
    Extract archive into raw directory.
    Supports ZIP and TAR/TAR.GZ/TGZ.
    """
    raw_dir = Path("data") / sku / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        if archive_type == "zip":
            logger.info(f"{sku}: extracting ZIP archive...")
            with zipfile.ZipFile(filepath, "r") as zf:
                zf.extractall(raw_dir)

            files_count = sum(1 for _ in raw_dir.rglob("*") if _.is_file())
            logger.info(f"{sku}: extracted {files_count} files")
            update_report(sku, filepath.name, files_count, archive_type, extract_status="extracted_zip")

        elif archive_type in ["tar", "targz", "tgz"]:
            logger.info(f"{sku}: extracting TAR archive...")
            with tarfile.open(filepath, "r:*") as tf:
                tf.extractall(raw_dir)

            files_count = sum(1 for _ in raw_dir.rglob("*") if _.is_file())
            logger.info(f"{sku}: extracted {files_count} files")
            update_report(sku, filepath.name, files_count, archive_type, extract_status="extracted_tar")

        else:
            logger.error(f"{sku}: unsupported archive type {archive_type}")
            update_report(sku, filepath.name, 0, archive_type, extract_status="failed")

    except Exception as e:
        logger.error(f"{sku}: extraction failed → {e}")
        update_report(sku, filepath.name, 0, archive_type, extract_status="failed")

# =========================
# Main entry point
# =========================
if __name__ == "__main__":
    # Add a blank line and marker to separate runs in the log
    logger.info("")
    logger.info("=== New run started ===")

    # Reset report at the start of each run
    report_path = Path("data") / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump([], f)

    # Process SKUs (limited to first one for testing)
    for item in config['skus'][:1]:
        download_url = item['source_url']
        sku_name = item["sku"]
        filepath, archive_type = download_archive(download_url, sku_name)
        extract_archive(sku_name, filepath, archive_type)
    logger.info("=== Run finished ===")
    logger.remove()  # release handlers
