import os
import zipfile

import requests
from tqdm import tqdm


def fetch_new_data(params: dict):
    url = params["cricsheet_url"]
    target_directory = params["raw_data_directory"]

    zip_file_path = "all.zip"
    new_files_count = 0

    # Download the zip file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_file_path, 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192), unit="KB"):
                f.write(chunk)

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for member in tqdm(zip_ref.infolist(), desc="Extracting"):
            member_path = os.path.join(target_directory, member.filename)
            if os.path.isfile(member_path) and os.path.getsize(member_path) == member.file_size:
                continue
            zip_ref.extract(member, target_directory)
            new_files_count += 1

    os.remove(zip_file_path)
