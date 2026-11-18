import pandas as pd
import os
import glob
from dte_utils import get_cal_dte
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_ROWS = 500_000
DEDUP_COLS = ["timestamp", "tradingsymbol", "expiry"]


INPUT_FILE = "data/expiry_files"
OUTPUT_FOLDER = "output"
PARQUET_FOLDER = "synth_futures/underlying_type = index"


def formatting_expiry_files(folder_path, sensex_data):
    sensex_data["datetime"] = pd.to_datetime(sensex_data["datetime"])
    sensex_data = sensex_data[["datetime", "close"]]
    sensex_data = sensex_data.rename(columns={"close": "spot"})
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            file_df = pd.read_csv(file_path)
            file_df["datetime"] = pd.to_datetime(file_df["datetime"])
            file_df["time"] = file_df["datetime"].dt.strftime("%H:%M:%S")
            file_df["date"] = file_df["datetime"].dt.strftime("%Y-%m-%d")
            file_df = file_df[file_df["time"] >= "09:15:00"]
            file_df = file_df[
                ((file_df["date"] < "2026-08-02") & (file_df["time"] < "15:30:00"))
                | ((file_df["date"] >= "2026-08-02") & (file_df["time"] < "15:40:00"))
            ]
            file_df["month"] = file_df["datetime"].dt.month
            file_df["year"] = file_df["datetime"].dt.year
            file_df = pd.merge(file_df, sensex_data, on="datetime", how="left")
            file_df["underlying"] = "SENSEX"
            file_df["instrument_type"] = file_df["tradingsymbol"].str[-2:]
            file_df["datetime"] = pd.to_datetime(file_df["datetime"])
            file_df["expiry"] = pd.to_datetime(file_df["expiry"])
            # file_df["dte"] = file_df.apply(
            #     lambda row: get_cal_dte(row["datetime"], row["expiry"]), axis=1
            # )
            file_df["strike"] = file_df["tradingsymbol"].str.extract(
                r"(\d{5})(?:CE|PE)$"
            )

            file_df = file_df.rename(
                columns={
                    "datetime": "timestamp",
                }
            )
            file_df.to_csv(os.path.join(OUTPUT_FOLDER, filename), index=False)


# def folder_assignment(source_folder_path):
#     for filename in os.listdir(source_folder_path):
#         if filename.endswith(".csv"):
#             source_file_path = os.path.join(source_folder_path, filename)
#             file_df = pd.read_csv(source_file_path)
#             file_df["year"] = pd.to_numeric(file_df["year"]).astype(int)
#             file_df["month"] = pd.to_numeric(file_df["month"]).astype(int)

#             for (year, month), group_df in file_df.groupby(["year", "month"]):
#                 target_folder = os.path.join(
#                     PARQUET_FOLDER, str(int(year)), f"{int(month):02d}"
#                 )
#                 os.makedirs(target_folder, exist_ok=True)
#                 target_file = os.path.join(target_folder, "data.parquet")
#                 if os.path.exists(target_file):
#                     existing_df = pd.read_parquet(target_file)
#                     group_df = pd.concat([existing_df, group_df], ignore_index=True)
#                     dedup_cols = ["timestamp", "tradingsymbol"]
#                     group_df = group_df.drop_duplicates(subset=dedup_cols, keep="last")
#                 group_df.to_parquet(target_file, index=False)


def folder_assignment(source_folder_path):
    staging_root = os.path.join(PARQUET_FOLDER, "_staging")
    touched = set()
    part_no = 0

    for filename in sorted(os.listdir(source_folder_path)):
        if not filename.endswith(".csv"):
            continue
        source_file_path = os.path.join(source_folder_path, filename)

        for chunk in pd.read_csv(source_file_path, chunksize=CHUNK_ROWS):
            chunk["year"] = pd.to_numeric(chunk["year"]).astype(int)
            chunk["month"] = pd.to_numeric(chunk["month"]).astype(int)
            for (year, month), g in chunk.groupby(["year", "month"]):
                d = os.path.join(staging_root, str(year), f"{month:02d}")
                os.makedirs(d, exist_ok=True)
                part_no += 1
                g.to_parquet(
                    os.path.join(d, f"part_{part_no:08d}.parquet"), index=False
                )
                touched.add((year, month))
    for year, month in sorted(touched):
        stage_dir = os.path.join(staging_root, str(year), f"{month:02d}")
        target_folder = os.path.join(PARQUET_FOLDER, str(year), f"{month:02d}")
        os.makedirs(target_folder, exist_ok=True)
        target_file = os.path.join(target_folder, "data.parquet")
        files = []
        if os.path.exists(target_file):
            files.append(target_file)
        files += sorted(glob.glob(os.path.join(stage_dir, "part_*.parquet")))
        df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
        df = df.drop_duplicates(subset=DEDUP_COLS, keep="last")
        tmp_file = target_file + ".tmp"
        df.to_parquet(tmp_file, index=False)
        os.replace(tmp_file, target_file)

        for f in files[
            1 if os.path.exists(target_file) and files[0] == target_file else 0 :
        ]:
            os.remove(f)
        del df
    for root, dirs, _ in os.walk(staging_root, topdown=False):
        for d in dirs:
            try:
                os.rmdir(os.path.join(root, d))
            except OSError:
                pass
    try:
        os.rmdir(staging_root)
    except OSError:
        pass


if __name__ == "__main__":
    sensex_data = pd.read_csv("SENSEX.csv")
    formatting_expiry_files(INPUT_FILE, sensex_data)
    folder_assignment(OUTPUT_FOLDER)
