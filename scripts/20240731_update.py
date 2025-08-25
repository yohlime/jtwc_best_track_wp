from io import BytesIO
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile
import httpx

import pandas as pd
from tqdm import tqdm

from helper.io import parse_input

OUT_FILE = Path("output/JTWC_raw.parquet")
BAK_FILE = Path("output/JTWC_raw.bak.parquet")


def main():
    zip_urls = [
        {
            "year": 2016,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2016/2016s-bwp/bwp2016.zip",
        },
        {
            "year": 2017,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2017/2017s-bwp/bwp2017.zip",
        },
        {
            "year": 2018,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2018/2018s-bwp/bwp2018.zip",
        },
        {
            "year": 2019,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2019/2019s-bwp/bwp2019.zip",
        },
        {
            "year": 2020,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2020/2020s-bwp/bwp2020.zip",
        },
        {
            "year": 2021,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2021/2021s-bwp/bwp2021.zip",
        },
        {
            "year": 2022,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2022/2022s-bwp/bwp2022.zip",
        },
        {
            "year": 2023,
            "url": "https://www.metoc.navy.mil/jtwc/products/best-tracks/2023/2023s-bwp/bwp2023.zip",
        },
    ]
    new_df = []
    for zip_url in tqdm(zip_urls):
        print(f"Processing {zip_url}")
        with httpx.stream("GET", zip_url["url"]) as res:
            zip_bytes = BytesIO()
            for chunk in res.iter_bytes():
                zip_bytes.write(chunk)
            zip_bytes.seek(0)
            with ZipFile(zip_bytes) as z:
                for fname in z.namelist():
                    if not fname.lower().endswith((".csv", ".dat")):
                        continue
                    with z.open(fname) as f:
                        new_df.append(parse_input(f))

    new_df = pd.concat(new_df, sort=False, ignore_index=True)
    new_df = new_df.sort_values(["YYYY", "MM", "DD", "HH", "CY"])

    if OUT_FILE.exists():
        in_df = pd.read_parquet(OUT_FILE)
        copyfile(OUT_FILE, BAK_FILE)  # Backup the file
    else:
        in_df = pd.DataFrame()

    in_df0 = in_df.loc[~in_df["YYYY"].isin([z["year"] for z in zip_urls])]
    out_df = pd.concat([in_df0, new_df], sort=False, ignore_index=True)
    out_df.to_parquet(OUT_FILE, index=False)  # Save


if __name__ == "__main__":
    main()
