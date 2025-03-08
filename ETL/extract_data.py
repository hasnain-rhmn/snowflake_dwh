import os
import requests
from tqdm import tqdm
import gzip
import shutil
import pandas as pd

def download_airbnb_data(cities, datasets, output_dir="airbnb_data"):
    base_url = "https://data.insideairbnb.com/{country}/{date}/data/{dataset}.csv.gz"

    for dataset in datasets:
        dataset_dir = os.path.join(output_dir, dataset)
        os.makedirs(dataset_dir, exist_ok=True)

        for city in cities:
            country = city['country']
            date = city['date']
            
            url = base_url.format(
                country=country,
                date=date,
                dataset=dataset
            )

            clean_country = country.replace('/', '_')

            file_name = f"{clean_country}_{dataset}_{date}.csv"
            gz_path = os.path.join(dataset_dir, file_name + ".gz")
            csv_path = os.path.join(dataset_dir, file_name)
            
            try:
                print(f"Downloading {dataset} data for {country} ({date})...")
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(gz_path, 'wb') as file, tqdm(
                    desc=gz_path,
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)
                        bar.update(len(chunk))
                
                # Unzip the file
                with gzip.open(gz_path, 'rb') as f_in:
                    with open(csv_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                os.remove(gz_path)
                print(f"Saved: {csv_path}\n")
                
            except requests.exceptions.HTTPError as e:
                print(f"Failed to download {dataset} data for {country} ({date}): {e}\n")


cities = [
    {"country": "greece/attica/athens", "date": "2024-12-25"} 
    ]


datasets = ["listings", "calendar", "reviews"]

download_airbnb_data(cities, datasets)
