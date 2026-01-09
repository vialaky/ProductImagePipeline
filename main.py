import requests
from fake_useragent import UserAgent
from pathlib import Path
import json
from datetime import datetime

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
        },
        {
            "sku": "AAA-0002",
            "source_url": "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz",
            "category": "CIFAR-10 Objects"
        },
        {
            "sku": "AAA-0003",
            "source_url": "https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz",
            "category": "CIFAR-100 Objects"
        },
        {
            "sku": "AAA-0004",
            "source_url": "http://www.vision.caltech.edu/Image_Datasets/Caltech101/101_ObjectCategories.tar.gz",
            "category": "Caltech101 Objects"
        },
        {
            "sku": "AAA-0005",
            "source_url": "http://www.vision.caltech.edu/Image_Datasets/Caltech256/256_ObjectCategories.tar",
            "category": "Caltech256 Objects"
        },
        {
            "sku": "AAA-0006",
            "source_url": "http://vision.stanford.edu/aditya86/ImageNetDogs/images.tar",
            "category": "Stanford Dogs"
        },
        {
            "sku": "AAA-0007",
            "source_url": "http://ai.stanford.edu/~jkrause/car196/car_ims.tgz",
            "category": "Stanford Cars"
        },
        {
            "sku": "AAA-0008",
            "source_url": "http://ftp.cs.stanford.edu/cs/cvgl/Stanford_Online_Products.zip",
            "category": "Online Products"
        },
        {
            "sku": "AAA-0009",
            "source_url": "http://cs231n.stanford.edu/tiny-imagenet-200.zip",
            "category": "Tiny ImageNet"
        },
        {
            "sku": "AAA-0010",
            "source_url": "https://www.robots.ox.ac.uk/~vgg/data/flowers/102/102flowers.tgz",
            "category": "Oxford Flowers"
        }
    ]
}

# =========================
# Report function
# =========================
def update_report(sku, status, filename, size, archive_type):
    """
    Append a new record to report.json with download results.
    Creates the file if it does not exist.
    """
    report_path = Path("data") / "ProductImagePipeline Data" / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    # Read existing report if available
    if report_path.exists():
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    # Add new entry
    data.append({
        "time": now,
        "sku": sku,
        "download status": status,
        "filename": filename,
        "size": size,
        "archive type": archive_type
    })

    # Write back to JSON file
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# =========================
# Archive type detection
# =========================
def detect_archive_type(filepath: Path) -> str:
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
    - Creates target folder
    - Skips if already downloaded (idempotency)
    - Retries up to 3 times with timeout
    - Logs INFO/ERROR messages
    - Updates report.json with results
    """
    ua = UserAgent()
    random_ua = ua.random

    target_dir = Path("data") / "ProductImagePipeline Data" / sku / "download"
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    filepath = target_dir / filename

    # Detect archive type
    archive_type = detect_archive_type(filepath)

    # Idempotency check
    if filepath.exists() and filepath.stat().st_size > 0:
        print(f"INFO: {sku} already downloaded, skipping...")
        update_report(sku, "success", filename, filepath.stat().st_size, archive_type)
        return

    # Retry loop
    for attempt in range(1, 4):
        try:
            response = requests.get(url, headers={'User-Agent': random_ua}, stream=True, timeout=30)
            if response.status_code == 200:
                print(f"INFO: Start downloading {sku} → {filename} (attempt {attempt})...")
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1048576):
                        if chunk:
                            f.write(chunk)
                size = filepath.stat().st_size
                if size > 0:
                    print(f"INFO: Download {sku} complete, file size {size} bytes")
                    update_report(sku, "success", filename, size, archive_type)
                    return
                else:
                    print(f"ERROR {sku}: file is empty (attempt {attempt})")
                    update_report(sku, "failed", filename, 0, archive_type)
            else:
                print(f"ERROR {sku}: HTTP {response.status_code} (attempt {attempt})")
                update_report(sku, "failed", filename, 0, archive_type)
        except requests.exceptions.RequestException as e:
            print(f"ERROR {sku}: network error {e} (attempt {attempt})")
            update_report(sku, "failed", filename, 0, archive_type)

    print(f"ERROR {sku}: Failed to download file after 3 attempts")
    update_report(sku, "failed", filename, 0, archive_type)

# =========================
# Main entry point
# =========================
if __name__ == "__main__":
    # Reset report at the start of each run
    report_path = Path("data") / "ProductImagePipeline Data" / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump([], f)

    # Download archives with images
    for sku in config['skus'][:3]:   # take first 3 for test
        download_url = sku['source_url']
        sku_name = sku["sku"]
        download_archive(download_url, sku_name)