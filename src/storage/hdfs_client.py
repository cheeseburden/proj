"""
Resilient Hadoop HDFS Client using WebHDFS REST API.
Handles directory creation, appending JSONL article records, listing files,
and buffering to local disk when HDFS is starting or in safe mode.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config import HDFS_BASE_DIR, HDFS_URL, HDFS_USER, LOCAL_BUFFER_DIR

logger = logging.getLogger("HDFSClient")
logging.basicConfig(level=logging.INFO)

class HDFSClient:
    def __init__(self, base_url: str = HDFS_URL, user: str = HDFS_USER):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.endpoint = f"{self.base_url}/webhdfs/v1"
        self.local_buffer = Path(LOCAL_BUFFER_DIR)
        self.local_buffer.mkdir(parents=True, exist_ok=True)
        self.is_connected = False
        self.check_connection()

    def check_connection(self) -> bool:
        """Verifies if WebHDFS service is reachable."""
        try:
            url = f"{self.endpoint}/?op=LISTSTATUS&user.name={self.user}"
            resp = requests.get(url, timeout=3)
            self.is_connected = (resp.status_code == 200)
            return self.is_connected
        except Exception:
            self.is_connected = False
            return False

    def mkdir(self, hdfs_path: str) -> bool:
        """Creates directory on HDFS if not exists."""
        clean_path = "/" + hdfs_path.strip("/")
        url = f"{self.endpoint}{clean_path}?op=MKDIRS&user.name={self.user}"
        try:
            resp = requests.put(url, timeout=5)
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"HDFS mkdir error: {e}")
            return False

    def write_record(self, record: Dict[str, Any], partition_date: str = None) -> bool:
        """
        Appends an article record into HDFS partitioned by date:
        /data/news/YYYY-MM-DD/articles.jsonl
        Falls back seamlessly to local buffer if HDFS is unavailable.
        """
        if partition_date is None:
            partition_date = datetime.utcnow().strftime("%Y-%m-%d")

        hdfs_dir = f"{HDFS_BASE_DIR}/{partition_date}"
        hdfs_file = f"{hdfs_dir}/articles.jsonl"
        line = json.dumps(record, ensure_ascii=False) + "\n"
        data_bytes = line.encode("utf-8")

        # 1. Attempt writing to HDFS if available
        if self.is_connected or self.check_connection():
            try:
                # Ensure directory exists
                self.mkdir(hdfs_dir)

                # Check if file exists
                status_url = f"{self.endpoint}{hdfs_file}?op=GETFILESTATUS&user.name={self.user}"
                status_resp = requests.get(status_url, timeout=4)

                if status_resp.status_code == 404:
                    # Create new file
                    create_url = f"{self.endpoint}{hdfs_file}?op=CREATE&overwrite=false&user.name={self.user}"
                    step1 = requests.put(create_url, timeout=5, allow_redirects=False)
                    if step1.status_code in [307, 201]:
                        upload_location = step1.headers.get("Location")
                        if upload_location:
                            put_resp = requests.put(upload_location, data=data_bytes, headers={"Content-Type": "application/octet-stream"}, timeout=10)
                            if put_resp.status_code in [200, 201]:
                                logger.info(f"Created file & saved record in HDFS: {hdfs_file}")
                                self._flush_buffer_if_any()
                                return True
                elif status_resp.status_code == 200:
                    # Append to existing file
                    append_url = f"{self.endpoint}{hdfs_file}?op=APPEND&user.name={self.user}"
                    step1 = requests.post(append_url, timeout=5, allow_redirects=False)
                    if step1.status_code == 307:
                        upload_location = step1.headers.get("Location")
                        if upload_location:
                            post_resp = requests.post(upload_location, data=data_bytes, headers={"Content-Type": "application/octet-stream"}, timeout=10)
                            if post_resp.status_code in [200, 201]:
                                logger.info(f"Appended record to HDFS file: {hdfs_file}")
                                return True
            except Exception as e:
                logger.warning(f"Failed writing directly to HDFS ({e}). Buffering locally...")

        # 2. Local buffer fallback (always ensures no data loss)
        local_date_dir = self.local_buffer / partition_date
        local_date_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_date_dir / "articles.jsonl"
        with open(local_file, "a", encoding="utf-8") as f:
            f.write(line)
        logger.info(f"Buffered record locally to: {local_file}")
        return True

    def _flush_buffer_if_any(self):
        """Attempts to sync local buffer to HDFS once connection is restored."""
        try:
            for date_folder in self.local_buffer.iterdir():
                if date_folder.is_dir():
                    buf_file = date_folder / "articles.jsonl"
                    if buf_file.exists() and buf_file.stat().st_size > 0:
                        with open(buf_file, "rb") as f:
                            content = f.read()
                        hdfs_file = f"{HDFS_BASE_DIR}/{date_folder.name}/articles.jsonl"
                        self.mkdir(f"{HDFS_BASE_DIR}/{date_folder.name}")
                        create_url = f"{self.endpoint}{hdfs_file}?op=CREATE&overwrite=true&user.name={self.user}"
                        step1 = requests.put(create_url, timeout=5, allow_redirects=False)
                        if step1.status_code in [307, 201]:
                            loc = step1.headers.get("Location")
                            if loc:
                                r = requests.put(loc, data=content, headers={"Content-Type": "application/octet-stream"}, timeout=10)
                                if r.status_code in [200, 201]:
                                    buf_file.unlink()
        except Exception:
            pass

    def list_files(self, path: str = HDFS_BASE_DIR) -> List[Dict[str, Any]]:
        """Lists directory structure and files in HDFS."""
        files = []
        if self.is_connected or self.check_connection():
            try:
                url = f"{self.endpoint}{path}?op=LISTSTATUS&user.name={self.user}"
                resp = requests.get(url, timeout=4)
                if resp.status_code == 200:
                    statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
                    for s in statuses:
                        item_path = f"{path.rstrip('/')}/{s['pathSuffix']}"
                        is_dir = (s["type"] == "DIRECTORY")
                        files.append({
                            "path": item_path,
                            "name": s["pathSuffix"],
                            "is_dir": is_dir,
                            "length": s.get("length", 0),
                            "replication": s.get("replication", 1),
                            "blocksize": s.get("blockSize", 0),
                            "modification_time": s.get("modificationTime", 0),
                            "storage_type": "HDFS",
                        })
                        if is_dir:
                            # Recurse one level
                            files.extend(self.list_files(item_path))
            except Exception as e:
                logger.warning(f"Error listing HDFS files: {e}")

        # Also list files from local buffer
        if self.local_buffer.exists():
            for p in self.local_buffer.rglob("*.jsonl"):
                rel_path = p.relative_to(self.local_buffer)
                files.append({
                    "path": f"{HDFS_BASE_DIR}/{rel_path.as_posix()}",
                    "name": p.name,
                    "is_dir": False,
                    "length": p.stat().st_size,
                    "replication": 1,
                    "blocksize": 134217728,
                    "modification_time": int(p.stat().st_mtime * 1000),
                    "storage_type": "LOCAL_BUFFER",
                })
        return files

    def read_sample_content(self, hdfs_path: str, max_bytes: int = 4096) -> str:
        """Reads snippet of a file from HDFS or local buffer."""
        # Check local buffer first
        clean = hdfs_path.replace(HDFS_BASE_DIR, "").strip("/\\")
        local_cand = self.local_buffer / clean
        if local_cand.exists():
            with open(local_cand, "r", encoding="utf-8", errors="ignore") as f:
                return f.read(max_bytes)

        if self.is_connected or self.check_connection():
            try:
                url = f"{self.endpoint}{hdfs_path}?op=OPEN&length={max_bytes}&user.name={self.user}"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    return resp.text
            except Exception:
                pass
        return ""

def get_hdfs_client() -> HDFSClient:
    return HDFSClient()
