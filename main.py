import sys
import json
import shutil
import zipfile
import tarfile
from datetime import datetime
from pathlib import Path
import requests
from fake_useragent import UserAgent
from loguru import logger
from PIL import Image, ImageOps, UnidentifiedImageError
import pickle
import numpy as np
import warnings


# =========================
# Suppress deprecation warnings
# =========================
warnings.filterwarnings("ignore", category=DeprecationWarning)

# =========================
# Configuration dictionary
# =========================
config = {
    "project_name": "ProductImagePipeline",
    "output_dir": "output",
    "logs_dir": "logs",
    "image_profile": {
        "mode": "instagram_square",
        "size": (1080, 1080),
        "background": "white",
        "fit": "contain",
        "quality": 85,
        "progressive": True
    },
    "skus": [
        {"sku": "AAA-0001", "source_url": "https://github.com/Horea94/Fruit-Images-Dataset/archive/refs/heads/master.zip", "category": "Fruits"},
        {"sku": "AAA-0002", "source_url": "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz", "category": "CIFAR-10 Objects"},
        {"sku": "AAA-0003", "source_url": "https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz", "category": "CIFAR-100 Objects"},
        {"sku": "AAA-0004", "source_url": "http://www.vision.caltech.edu/Image_Datasets/Caltech101/101_ObjectCategories.tar.gz", "category": "Caltech101 Objects"},
        {"sku": "AAA-0005", "source_url": "http://www.vision.caltech.edu/Image_Datasets/Caltech256/256_ObjectCategories.tar", "category": "Caltech256 Objects"},
        {"sku": "AAA-0006", "source_url": "http://vision.stanford.edu/aditya86/ImageNetDogs/images.tar", "category": "Stanford Dogs"},
        {"sku": "AAA-0007", "source_url": "http://ai.stanford.edu/~jkrause/car196/car_ims.tgz", "category": "Stanford Cars"},
        {"sku": "AAA-0008", "source_url": "http://ftp.cs.stanford.edu/cs/cvgl/Stanford_Online_Products.zip", "category": "Online Products"},
        {"sku": "AAA-0009", "source_url": "http://cs231n.stanford.edu/tiny-imagenet-200.zip", "category": "Tiny ImageNet"},
        {"sku": "AAA-0010", "source_url": "https://www.robots.ox.ac.uk/~vgg/data/flowers/102/102flowers.tgz", "category": "Oxford Flowers"}
    ]
}

# =========================
# Logging setup
# =========================
logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")
logger.add("pipeline.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="100 KB",
           retention="100 days")

# =========================
# Report function
# =========================
def update_report(sku, filename=None, size=None, archive_type=None,
                  download_status=None, extract_status=None, process_status=None, processed_count=None):
    """Update or create an entry in report.json for the given SKU with current pipeline status."""
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

    entry = next((item for item in data if item.get("sku") == sku), None)
    if entry:
        if filename is not None: entry["filename"] = filename
        if size is not None: entry["size"] = size
        if archive_type is not None: entry["archive type"] = archive_type
        if download_status is not None: entry["download_status"] = download_status
        if extract_status is not None: entry["extract_status"] = extract_status
        if process_status is not None: entry["process_status"] = process_status
        if processed_count is not None: entry["processed_count"] = processed_count
    else:
        entry = {
            "time": now, "sku": sku, "filename": filename, "size": size,
            "archive type": archive_type, "download_status": download_status,
            "extract_status": extract_status, "process_status": process_status,
            "processed_count": processed_count
        }
        data.append(entry)

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# =========================
# Archive type detection
# =========================
def detect_archive_type(filepath: Path) -> str:
    """Detect archive type (zip, tar, tgz, targz) based on file suffix."""
    suffixes = filepath.suffixes
    if suffixes == ['.tar', '.gz']: return "targz"
    if filepath.suffix == '.tgz': return "tgz"
    if filepath.suffix == '.zip': return "zip"
    if filepath.suffix == '.tar': return "tar"
    return "unknown"

# =========================
# Download function
# =========================
def download_archive(url, sku):
    """Download an archive from the given URL for a SKU, retrying up to 3 times."""
    ua = UserAgent()
    random_ua = ua.random
    target_dir = Path("data") / sku / "download"
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    filepath = target_dir / filename
    archive_type = detect_archive_type(filepath)

    if filepath.exists() and filepath.stat().st_size > 0:
        logger.info(f"{sku}: already downloaded, skipping")
        update_report(sku, filename=filename, size=filepath.stat().st_size,
                      archive_type=archive_type, download_status="success")
        return filepath, archive_type

    for attempt in range(1, 4):
        try:
            response = requests.get(url, headers={'User-Agent': random_ua}, stream=True, timeout=60)
            if response.status_code == 200:
                logger.info(f"{sku}: downloading {filename} (attempt {attempt})")
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1048576):
                        if chunk: f.write(chunk)
                size = filepath.stat().st_size
                if size > 0:
                    logger.info(f"{sku}: download complete, size {size} bytes")
                    update_report(sku, filename=filename, size=size,
                                  archive_type=archive_type, download_status="success")
                    return filepath, archive_type
            else:
                logger.error(f"{sku}: HTTP {response.status_code} (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            logger.error(f"{sku}: network error {e} (attempt {attempt})")

    logger.error(f"{sku}: failed after 3 attempts")
    update_report(sku, filename=filename, size=0, archive_type=archive_type, download_status="failed")
    return filepath, archive_type

# =========================
# CIFAR conversion helper
# =========================
def convert_cifar_batches(raw_dir: Path, sku: str) -> int:
    """Convert CIFAR-10/100 Python batch files into PNG images inside raw_dir."""

    cifar10_dir = raw_dir / "cifar-10-batches-py"
    cifar100_dir = raw_dir / "cifar-100-python"

    batch_dir = None
    mode = None
    if cifar10_dir.exists():
        batch_dir = cifar10_dir
        mode = "cifar10"
    elif cifar100_dir.exists():
        batch_dir = cifar100_dir
        mode = "cifar100"
    else:
        return 0

    batch_files = []
    if mode == "cifar10":
        for name in ["data_batch_1", "data_batch_2", "data_batch_3", "data_batch_4", "data_batch_5", "test_batch"]:
            bf = batch_dir / name
            if bf.exists() and bf.is_file():
                batch_files.append(bf)
    else:
        for name in ["train", "test"]:
            bf = batch_dir / name
            if bf.exists() and bf.is_file():
                batch_files.append(bf)

    count = 0
    for bf in batch_files:
        try:
            with open(bf, "rb") as f:
                batch = pickle.load(f, encoding="bytes")

            # Normalize keys to str
            batch = { (k.decode("utf-8") if isinstance(k, bytes) else k): v for k, v in batch.items() }

            raw_data = batch.get("data")
            filenames = batch.get("filenames") or batch.get("file_names")

            if raw_data is None:
                logger.error(f"{sku}: CIFAR batch missing 'data' → {bf.name}")
                continue

            # Recreate array cleanly to avoid deprecated align behavior
            data = np.array(raw_data, dtype=np.uint8, copy=True)

            n = len(data)
            # If filenames missing or length mismatch—generate synthetic names
            if filenames is None or len(filenames) != n:
                filenames = [f"{mode}_{bf.name}_{i:06d}.png" for i in range(n)]

            for i in range(n):
                img_array = data[i]
                if img_array.size != 32 * 32 * 3:
                    # Some CIFAR-100 'train'/'test' store data as (N, 3072)
                    # Ensure flat length is correct
                    flat = np.asarray(img_array, dtype=np.uint8).ravel()
                    if flat.size != 32 * 32 * 3:
                        continue
                    img_array = flat

                # CHW → HWC
                img = img_array.reshape(3, 32, 32).transpose(1, 2, 0)
                img = Image.fromarray(img)

                fname = filenames[i].decode("utf-8") if isinstance(filenames[i], bytes) else filenames[i]
                out_path = raw_dir / fname
                img.save(out_path)
                count += 1

        except Exception as e:
            logger.error(f"{sku}: failed to convert CIFAR batch {bf.name} → {e}")

    logger.info(f"{sku}: converted {count} CIFAR images")
    return count

# =========================
# Extract function
# =========================
def extract_archive(sku, filepath, archive_type):
    """Extract a downloaded archive into raw_dir and trigger CIFAR conversion if needed."""
    raw_dir = Path("data") / sku / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        if archive_type == "zip":
            logger.info(f"{sku}: extracting ZIP archive...")
            with zipfile.ZipFile(filepath, "r") as zf:
                zf.extractall(raw_dir)
            files_count = sum(1 for _ in raw_dir.rglob("*") if _.is_file())
            update_report(sku, filename=filepath.name, size=files_count, archive_type=archive_type, extract_status="extracted_zip")

        elif archive_type in ["tar", "targz", "tgz"]:
            logger.info(f"{sku}: extracting TAR archive...")
            with tarfile.open(filepath, "r:*") as tf:
                tf.extractall(raw_dir, filter="data")  # Python 3.14+ safe
            files_count = sum(1 for _ in raw_dir.rglob("*") if _.is_file())
            update_report(sku, filename=filepath.name, size=files_count, archive_type=archive_type, extract_status="extracted_tar")

            # If CIFAR—convert binary batches to images
            if "cifar" in filepath.name.lower():
                convert_cifar_batches(raw_dir, sku)

        else:
            logger.error(f"{sku}: unsupported archive type {archive_type}")
            update_report(sku, filename=filepath.name, size=0, archive_type=archive_type, extract_status="failed")

    except Exception as e:
        logger.error(f"{sku}: extraction failed → {e}")
        update_report(sku, filename=filepath.name, size=0, archive_type=archive_type, extract_status="failed")

# =========================
# Process images
# =========================
def process_images(sku, profile, limit=10):
    """Process up to 'limit' images for a SKU into Instagram-square JPEGs with given profile."""
    raw_dir = Path("data") / sku / "raw"
    processed_dir = Path("data") / sku / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    image_files = []
    image_files.extend(raw_dir.rglob("*.jpg"))
    image_files.extend(raw_dir.rglob("*.jpeg"))
    image_files.extend(raw_dir.rglob("*.png"))

    if not image_files:
        logger.error(f"{sku}: no images found in {raw_dir}")
        update_report(sku, process_status="failed", processed_count=0)
        _safe_delete_raw(sku, raw_dir)
        return

    count = 0
    for img_path in image_files[:limit]:
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
                logger.info(f"{sku}: processed {img_path.name} → {out_path.name}")
                count += 1
        except UnidentifiedImageError:
            logger.error(f"{sku}: unidentified image format → {img_path.name}")
        except Exception as e:
            logger.error(f"{sku}: failed to process {img_path.name} → {e}")

    status = "processed" if count > 0 else "failed"
    update_report(sku, process_status=status, processed_count=count)
    _safe_delete_raw(sku, raw_dir)

def _safe_delete_raw(sku, raw_dir: Path):
    """Safely delete the raw_dir folder after processing, logging errors if deletion fails."""
    try:
        if raw_dir.exists():
            shutil.rmtree(raw_dir)
            logger.info(f"{sku}: raw folder deleted after processing")
    except Exception as e:
        logger.error(f"{sku}: failed to delete raw folder → {e}")

# =========================
# Main entry point
# =========================
if __name__ == "__main__":
    """Entry point: run the pipeline for all configured SKUs."""
    logger.info("")
    logger.info("=== New run started ===")

    report_path = Path("data") / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if not report_path.exists():
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump([], f)

    for item in config["skus"]:
        sku_name = item["sku"]
        download_url = item["source_url"]

        logger.info(f"{sku_name}: start pipeline")
        filepath, archive_type = download_archive(download_url, sku_name)
        extract_archive(sku_name, filepath, archive_type)
        process_images(sku_name, config["image_profile"], limit=10)
        logger.info(f"{sku_name}: pipeline finished")

    logger.info("=== Run finished ===")
    logger.remove()
