import re
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile

import pandas as pd
from tqdm import tqdm

from helper.io import parse_input

OUT_FILE = Path("output/JTWC_raw.parquet")
BAK_FILE = Path("output/JTWC_raw.bak.parquet")
IN_DIR = Path("input/update")


def main():
    zip_files = list(IN_DIR.glob("*.zip"))
    zip_files = pd.DataFrame(
        zip_files,
        index=[int(re.findall("[0-9]{4}", z.as_posix())[0]) for z in zip_files],
        columns=["name"],
    ).sort_index()

    new_df = []
    for zfile in tqdm(zip_files["name"]):
        with ZipFile(zfile) as z:
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

    out_df = pd.concat([in_df, new_df], sort=False, ignore_index=True)
    out_df.to_parquet(OUT_FILE, index=False)  # Save


if __name__ == "__main__":
    main()
