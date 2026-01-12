# ProductImagePipeline

ProductImagePipeline is a Python pipeline for downloading, extracting, converting, and processing images from popular open datasets.  
It automates the workflow of fetching archives, handling CIFAR batch conversions, resizing images into Instagram‑ready squares, and generating a structured JSON report.

---

## ✨ Features
- Automated **download** of multiple datasets (Fruits, CIFAR‑10/100, Caltech, Stanford Dogs/Cars, Tiny ImageNet, Oxford Flowers, etc.).
- Safe **archive extraction** (`zip`, `tar`, `tgz`, `targz`) with Python 3.14+.
- CIFAR batch conversion into PNG images with NumPy 2.4 compatibility.
- Image **processing** into 1080×1080 JPEGs (Instagram square profile).
- Centralized **logging** with [loguru](https://github.com/Delgan/loguru) (rotation + retention 100 days).
- Structured **report.json** with pipeline status for each SKU.

---

## 📦 Requirements
Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
