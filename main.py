import requests
from fake_useragent import UserAgent
from pathlib import Path
import json
from datetime import datetime
from loguru import logger
import sys
import zipfile
import tarfile
from PIL import Image, ImageOps

# =========================
# Configuration dictionary
# =========================
config = {
    "project_name": "ProductImagePipeline",
    "output_dir": "output",
    "logs_dir": "logs",
    "image_profile": {
        "mode": "instagram_square",
        "size": (1080, 1080),       # square format for Instagram
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
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")
logger.add("pipeline.log", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO", rotation="1 MB")

# =========================
# Report function
# =========================
def update_report(sku, filename, size, archive_type,
                  download_status=None, extract_status=None, process_status=None):
    """
    Update report.json with statuses (download, extract, process) in a single entry per SKU.
    """
    report_path = Path("data") / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if report_path.exists():
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    entry = next((item for item in data if item["sku"] == sku), None)

    if entry:
        if download_status is not None:
            entry["download_status"] = download_status
        if extract_status is not None:
            entry["extract_status"] = extract_status
        if process_status is not None:
            entry["process_status"] = process_status
    else:
        entry = {
            "time": now,
            "sku": sku,
            "filename": filename,
            "size": size,
            "archive type": archive_type,
            "download_status": download_status,
            "extract_status": extract_status,
            "process_status": process_status
        }
        data.append(entry)

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# =========================
# Archive type detection
# =========================
def detect_archive_type(filepath: Path) -> str:
    suffixes = filepath.suffixes
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
# Process one image
# =========================
def process_one_image(sku, profile):
    """
    Process one image from raw folder into a square 1:1 canvas.
    """
    raw_dir = Path("data") / sku / "raw"
    processed_dir = Path("data") / sku / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    image_files = []
    image_files.extend(raw_dir.rglob("*.jpg"))
    image_files.extend(raw_dir.rglob("*.jpeg"))
    image_files.extend(raw_dir.rglob("*.png"))

    if not image_files:
        logger.error(f"{sku}: no images found in {raw_dir}")
        update_report(sku, "", 0, "", process_status="failed")
        return

    img_path = image_files[0]
    logger.info(f"{sku}: processing image {img_path.name}")

    try:
        with Image.open(img_path) as img:
            img = ImageOps.contain(img, profile["size"])
            canvas = Image.new("RGB", profile["size"], profile["background"])
            offset = ((profile["size"][0] - img.width) // 2,
                      (profile["size"][1] - img.height) // 2)
            canvas.paste(img, offset)

            out_path = processed_dir / f"{img_path.stem}_processed.jpg"
            canvas.save(out_path, "JPEG",
                        quality=profile["quality"],
                        progressive=profile["progressive"])
            logger.info(f"{sku}: saved processed image → {out_path.name}")
            update_report(sku, img_path.name, 0, "", process_status="processed")

    except Exception as e:
        logger.error(f"{sku}: failed to process {img_path.name} → {e}")
        update_report(sku, img_path.name, 0, "", process_status="failed")

# =========================
# Main entry point
# =========================
if __name__ == "__main__":
    logger.info("")
    logger.info("=== New run started ===")

    # Reset report at the start of each run
    report_path = Path("data") / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump([], f)

    # Process SKUs (limit to first for testing)
    for item in config['skus'][:1]:
        download_url = item['source_url']
        sku_name = item["sku"]
        filepath, archive_type = download_archive(download_url, sku_name)
        extract_archive(sku_name, filepath, archive_type)
        process_one_image(sku_name, config["image_profile"])

    logger.info("=== Run finished ===")
    logger.remove()