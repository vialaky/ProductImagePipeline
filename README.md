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
```

Key packages:
- requests
- fake-useragent
- loguru
- Pillow
- numpy

---

## 🚀 Usage
Clone the repository and run the pipeline:
```bash
git clone https://github.com/your-username/ProductImagePipeline.git
cd ProductImagePipeline
python main.py
```

---

## 📂 Project Structure
ProductImagePipeline/
├── main.py
├── requirements.txt
├── README.md
├── data/
├── logs/
└── output/

---

## 📝 Example Report Entry
```json
{
  "time": "2026-01-12 10:47:49",
  "sku": "AAA-0001",
  "filename": "master.zip",
  "size": 90503,
  "archive type": "zip",
  "download_status": "success",
  "extract_status": "extracted_zip",
  "process_status": "processed",
  "processed_count": 10
}
```

---

## ⚖️ License
This project is licensed under the MIT License.
See the LICENSE file for details.

--- 

## 🤝 Contributing
Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

---
