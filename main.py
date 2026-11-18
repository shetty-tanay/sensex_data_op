import os
import glob
import pickle
import shutil
import concurrent.futures
from tokenize import group
import pandas as pd
from datetime import datetime, timedelta
import time

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv(".env")

MANIFEST_PATH = "data/manifest.csv"
PROCESSED_PATH = "data/master_processed_file_ids.txt"  # legacy / consolidated log
PROCESSED_PARTS_DIR = "data/processed_parts"  # one log per chunk (no write races)
ERROR_LOG_PATH = "data/error_files_log.txt"
TEMP_DIR = "data/temp"
PARTS_DIR = "data/expiry_parts"  # <expiry>/<chunk_idx>.csv
EXPIRY_DIR = "data/expiry_files"

monthly_expiries = {
    (2025, "JAN"): "2025-01-30",
    (2025, "FEB"): "2025-02-27",
    (2025, "MAR"): "2025-03-27",
    (2025, "APR"): "2025-04-24",
    (2025, "MAY"): "2025-05-29",
    (2025, "JUN"): "2025-06-26",
    (2025, "JUL"): "2025-07-31",
    (2025, "AUG"): "2025-08-28",
    (2025, "SEP"): "2025-09-25",
    (2025, "OCT"): "2025-10-30",
    (2025, "NOV"): "2025-11-27",
    (2025, "DEC"): "2025-12-25",
    (2026, "JAN"): "2026-01-29",
    (2026, "FEB"): "2026-02-26",
    (2026, "MAR"): "2026-03-26",
    (2026, "APR"): "2026-04-30",
    (2026, "MAY"): "2026-05-28",
    (2026, "JUN"): "2026-06-25",
    (2026, "JUL"): "2026-07-30",
    (2026, "AUG"): "2026-08-27",
    (2026, "SEP"): "2026-09-24",
    (2026, "OCT"): "2026-10-29",
    (2026, "NOV"): "2026-11-26",
    (2026, "DEC"): "2026-12-31",
    (2027, "JAN"): "2027-01-28",
    (2027, "FEB"): "2027-02-25",
    (2027, "MAR"): "2027-03-25",
    (2027, "APR"): "2027-04-29",
    (2027, "MAY"): "2027-05-27",
    (2027, "JUN"): "2027-06-24",
    (2027, "JUL"): "2027-07-29",
    (2027, "AUG"): "2027-08-26",
    (2027, "SEP"): "2027-09-30",
    (2027, "OCT"): "2027-10-28",
    (2027, "NOV"): "2027-11-25",
    (2027, "DEC"): "2027-12-30",
    (2028, "JAN"): "2028-01-27",
    (2028, "FEB"): "2028-02-24",
    (2028, "MAR"): "2028-03-30",
    (2028, "APR"): "2028-04-27",
    (2028, "MAY"): "2028-05-25",
    (2028, "JUN"): "2028-06-29",
    (2028, "JUL"): "2028-07-27",
    (2028, "AUG"): "2028-08-31",
    (2028, "SEP"): "2028-09-28",
    (2028, "OCT"): "2028-10-26",
    (2028, "NOV"): "2028-11-30",
    (2028, "DEC"): "2028-12-28",
}


def log_issue(msg):
    """Print to console AND append to the error log. Main process only (single writer)."""
    print(msg)
    os.makedirs("data", exist_ok=True)
    with open(ERROR_LOG_PATH, "a") as err:
        err.write(f"[{datetime.now()}] {msg}\n")


# ==========================================
# 1. AUTH & DRIVE HELPERS
# ==========================================
def authenticate_drive(write_token=True):
    """write_token=False is used inside workers so that several processes
    never write token.pickle at the same time."""
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", ["https://www.googleapis.com/auth/drive.readonly"]
            )
            creds = flow.run_local_server(port=0)
        if write_token:
            with open("token.pickle", "wb") as token:
                pickle.dump(creds, token)
    return build("drive", "v3", credentials=creds)


def list_children(service, parent_id, extra_query=""):
    """All direct children of a folder, following pagination."""
    query = f"'{parent_id}' in parents and trashed=false {extra_query}"
    items, page_token = [], None
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def get_folder_by_name(service, parent_id, folder_name):
    query = (
        f"'{parent_id}' in parents and name='{folder_name}' "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    response = service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    return files[0] if files else None


# ==========================================
# 2. DATA PROCESSING CORE (unchanged)
# ==========================================
def apply_transformations(df):
    df = df.loc[df["tradingsymbol"].ne("SENSEX")].copy()
    df["expiry"] = get_expiry(df["tradingsymbol"], monthly_expiries)
    df["internal_timestamp"] = pd.to_datetime(df["internal_timestamp"], errors="coerce")
    df["datetime"] = df["internal_timestamp"].dt.tz_localize(None).dt.floor("min")
    df["date"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d")
    df["offer_qty"] = df["offer_qty"].fillna(0)
    df["bid_qty"] = df["bid_qty"].fillna(0)
    # df = df.dropna(subset=["offer_price", "bid_price"])
    return df


def qty_at_price_extreme(group, price_col, qty_col, method):
    idx = getattr(group[price_col], method)()
    return group.loc[idx, qty_col]


def get_expiry(symbols, monthly_expiries):
    MONTH_CODES = {
        **{str(i): i for i in range(1, 10)},
        "O": 10,
        "N": 11,
        "D": 12,
    }

    SYMBOL_PATTERN = (
        r"^SENSEX(?P<year>\d{2})"
        r"(?:(?P<monthly_month>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
        r"|(?P<weekly_month>[1-9OND])(?P<day>\d{2}))"
    )

    parts = symbols.str.extract(SYMBOL_PATTERN)
    skip_mask = symbols.eq("SENSEX")
    invalid_mask = parts.isna().all(axis=1) & ~skip_mask
    if invalid_mask.any():
        invalid = symbols[parts.isna().all(axis=1)].unique()
        print(f"Invalid trading symbols: {invalid[:10]}")

    expiry = pd.Series(index=symbols.index, dtype="object")
    monthly_mask = parts["monthly_month"].notna()

    expiry.loc[monthly_mask] = [
        monthly_expiries[(2000 + int(year), month)]
        for year, month in zip(
            parts.loc[monthly_mask, "year"], parts.loc[monthly_mask, "monthly_month"]
        )
    ]

    weekly_mask = parts["weekly_month"].notna()
    weekly_dates = [
        f"20{year}-{MONTH_CODES[month]:02d}-{day}"
        for year, month, day in zip(
            parts.loc[weekly_mask, "year"],
            parts.loc[weekly_mask, "weekly_month"],
            parts.loc[weekly_mask, "day"],
        )
    ]

    expiry.loc[weekly_mask] = weekly_dates
    expiry = pd.to_datetime(expiry, errors="coerce")
    if expiry.isna().any():
        raise ValueError("Invalid or missing expiry date found.")

    return expiry.dt.strftime("%Y-%m-%d")


# ==========================================
# 3. PHASE 1: BUILD THE MANIFEST CSV
# ==========================================
def build_manifest(start_date_str=None, end_date_str=None):
    """Crawl root -> day folders -> BOT_FOLDER_NAME -> ticklogs file and write one
    row per file to MANIFEST_PATH. Sorted by date so chunks are date-contiguous."""
    service = authenticate_drive()
    root_id = os.getenv("MANUAL_LOG_UPLOADS_ID")
    bot_name = os.getenv("BOT_FOLDER_NAME")

    start = (
        datetime.strptime(start_date_str, "%Y-%m-%d").date() if start_date_str else None
    )
    end = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else None

    day_folders = list_children(
        service, root_id, "and mimeType='application/vnd.google-apps.folder'"
    )

    dated = []
    for f in day_folders:
        try:
            d = datetime.strptime(
                f["name"], "%d-%m-%Y"
            ).date()  # accepts un-padded 1-1-2026
        except ValueError:
            continue  # not a day folder
        if (start and d < start) or (end and d > end):
            continue
        dated.append((d, f))
    dated.sort(key=lambda x: x[0])

    # Report dates in the range that have no day folder in Drive at all.
    # Weekends are skipped (no trading); exchange holidays will still show up here.
    if dated:
        lo = start or dated[0][0]
        hi = end or dated[-1][0]
        found = {d for d, _ in dated}
        cur = lo
        while cur <= hi:
            if cur not in found and cur.weekday() < 5:
                log_issue(f"[{cur}] day folder missing in Drive")
            cur += timedelta(days=1)

    rows = []
    for d, day_folder in dated:
        bot_folder = get_folder_by_name(service, day_folder["id"], bot_name)
        if not bot_folder:
            log_issue(f"[{d}] bot folder '{bot_name}' missing, skipped")
            continue

        # n files per date is normal; sort by name so the order within a date is stable.
        files = sorted(
            (
                f
                for f in list_children(service, bot_folder["id"])
                if f["name"].startswith("ticklogs")
            ),
            key=lambda f: f["name"],
        )
        if not files:
            log_issue(f"[{d}] no ticklogs file, skipped")
            continue

        for f in files:
            rows.append(
                {
                    "date": d.isoformat(),
                    "bot_folder": bot_name,
                    "file_id": f["id"],
                    "file_name": f["name"],
                    "drive_path": f"{day_folder['name']}/{bot_name}/{f['name']}",
                }
            )

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(
        rows, columns=["date", "bot_folder", "file_id", "file_name", "drive_path"]
    ).to_csv(MANIFEST_PATH, index=False)
    print(f"Manifest written: {len(rows)} files -> {MANIFEST_PATH}")


# ==========================================
# 4. PHASE 2: WORKER (one chunk, sequential, appends after every file)
# ==========================================
def split_into_chunks(items, n_chunks):
    """Contiguous, near-equal chunks. 25 items / 5 -> 5 each; 26 / 5 -> 6,5,5,5,5."""
    n_chunks = max(1, min(n_chunks, len(items)))
    size, extra = divmod(len(items), n_chunks)
    chunks, i = [], 0
    for c in range(n_chunks):
        step = size + (1 if c < extra else 0)
        chunks.append(items[i : i + step])
        i += step
    return chunks


def mark_processed(chunk_idx, file_id):
    """Each chunk has its own log file, so workers never write the same file."""
    with open(f"{PROCESSED_PARTS_DIR}/{chunk_idx:03d}.txt", "a") as f:
        f.write(file_id + "\n")


def process_one_file(service, chunk_idx, file_id, file_name):
    """Download -> transform -> append to this chunk's expiry part files -> mark done."""
    raw_path = f"{TEMP_DIR}/raw_{file_id}.csv"
    try:
        request = service.files().get_media(fileId=file_id)
        for attempt in range(3):
            try:
                with open(raw_path, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                break
            except Exception as e:
                if attempt == 2:
                    raise e
                time.sleep(2)
        # with open(raw_path, "wb") as fh:
        #     downloader = MediaIoBaseDownload(fh, request)
        #     done = False
        #     while not done:
        #         _, done = downloader.next_chunk()

        dedup_cols = ["datetime", "tradingsymbol", "expiry","date"]
        chunk_list = []
        with pd.read_csv(raw_path, chunksize=100000) as reader:
            for chunk in reader:
                chunk = apply_transformations(chunk)
                chunk = chunk.sort_values("internal_timestamp")
                chunk = (
                    chunk.groupby(dedup_cols, sort=False)
                    .agg(
                        open_bid_price=("bid_price", "first"),
                        open_bid_qty=("bid_qty", "first"),
                        high_bid_price=("bid_price", "max"),
                        high_bid_qty=(
                            "bid_price",
                            lambda x: chunk.loc[x.idxmax(), "bid_qty"],
                        ),
                        low_bid_price=("bid_price", "min"),
                        # low_bid_qty=("bid_qty", "min"),
                        low_bid_qty=(
                            "bid_price",
                            lambda x: chunk.loc[x.idxmin(), "bid_qty"],
                        ),
                        close_bid_price=("bid_price", "last"),
                        close_bid_qty=("bid_qty", "last"),
                        open_offer_price=("offer_price", "first"),
                        open_offer_qty=("offer_qty", "first"),
                        high_offer_price=("offer_price", "max"),
                        high_offer_qty=(
                            "offer_price",
                            lambda x: chunk.loc[x.idxmax(), "offer_qty"],
                        ),
                        low_offer_price=("offer_price", "min"),
                        low_offer_qty=(
                            "offer_price",
                            lambda x: chunk.loc[x.idxmin(), "offer_qty"],
                        ),
                        # low_offer_qty=("offer_qty", "min"),
                        close_offer_price=("offer_price", "last"),
                        close_offer_qty=("offer_qty", "last"),
                        open_last_price=("last_price", "first"),
                        high_last_price=("last_price", "max"),
                        low_last_price=("last_price", "min"),
                        close_last_price=("last_price", "last"),
                        close_oi=("oi", "last"),
                        open_oi=("oi", "first"),
                         
                        cumm_volume_trades=("volume_traded", "last"),

                        close_bid_price_last_available= ("bid_price", lambda x: x.iloc[-1]),
                        close_bid_qty_last_available=("bid_qty", lambda x: x.iloc[-1]),

                        open_bid_price_first_available=("bid_price", lambda x: x.iloc[0]),
                        open_bid_qty_first_available=("bid_qty", lambda x: x.iloc[0]),

                        close_offer_price_last_available=("offer_price", lambda x: x.iloc[-1]),
                        close_offer_qty_last_available=("offer_qty", lambda x: x.iloc[-1]),

                        open_offer_price_first_available=("offer_price", lambda x: x.iloc[0]),
                        open_offer_qty_first_available=("offer_qty", lambda x: x.iloc[0]),

                        tick_count=("internal_timestamp", "size"),
                        open_timestamp=("internal_timestamp", "first"),
                        close_timestamp=("internal_timestamp", "last"),
                    )
                    .reset_index()
                )
                chunk_list.append(chunk)
        if chunk_list:
            master_df = pd.concat(chunk_list, ignore_index=True)
            master_df = master_df.sort_values("open_timestamp")
            master_df = (
                master_df.groupby(dedup_cols, sort=False)
                .agg(
                    open_bid_price=("open_bid_price", "first"),
                    open_bid_qty=("open_bid_qty", "first"),
                    high_bid_price=("high_bid_price", "max"),
                    # high_bid_qty=("high_bid_qty", "max"),
                    high_bid_qty=(
                        "high_bid_price",
                        lambda x: master_df.loc[x.idxmax(), "high_bid_qty"],
                    ),
                    low_bid_price=("low_bid_price", "min"),
                    # low_bid_qty=("low_bid_qty", "min"),
                    low_bid_qty=(
                        "low_bid_price",
                        lambda x: master_df.loc[x.idxmin(), "low_bid_qty"],
                    ),
                    close_bid_price=("close_bid_price", "last"),
                    close_bid_qty=("close_bid_qty", "last"),
                    open_offer_price=("open_offer_price", "first"),
                    open_offer_qty=("open_offer_qty", "first"),
                    high_offer_price=("high_offer_price", "max"),
                    # high_offer_qty=("high_offer_qty", "max"),
                    high_offer_qty=(
                        "high_offer_price",
                        lambda x: master_df.loc[x.idxmax(), "high_offer_qty"],
                    ),
                    low_offer_price=("low_offer_price", "min"),
                    low_offer_qty=(
                        "low_offer_price",
                        lambda x: master_df.loc[x.idxmin(), "low_offer_qty"],
                    ),
                    # low_offer_qty=("low_offer_qty", "min"),
                    close_offer_price=("close_offer_price", "last"),
                    close_offer_qty=("close_offer_qty", "last"),
                    open_last_price=("open_last_price", "first"),
                    high_last_price=("high_last_price", "max"),
                    low_last_price=("low_last_price", "min"),
                    close_last_price=("close_last_price", "last"),
                    close_oi=("close_oi", "last"),
                    open_oi=("open_oi", "first"),

                    cumm_volume_trades=("cumm_volume_trades", "last"),
                    
                    close_bid_price_last_available= ("close_bid_price_last_available", lambda x: x.iloc[-1]),
                    close_bid_qty_last_available=("close_bid_qty_last_available", lambda x: x.iloc[-1]),
                    open_bid_price_first_available=("open_bid_price_first_available", lambda x: x.iloc[0]),
                    open_bid_qty_first_available=("open_bid_qty_first_available", lambda x: x.iloc[0]),
                    close_offer_price_last_available=("close_offer_price_last_available", lambda x: x.iloc[-1]),
                    close_offer_qty_last_available=("close_offer_qty_last_available", lambda x: x.iloc[-1]),
                    open_offer_price_first_available=("open_offer_price_first_available", lambda x: x.iloc[0]),
                    open_offer_qty_first_available=("open_offer_qty_first_available", lambda x: x.iloc[0]),
                    tick_count = ("tick_count", "sum"),
                    open_timestamp = ("open_timestamp","first"),
                    close_timestamp = ("close_timestamp","last")
                )
                .reset_index()
            )
            master_df = master_df.sort_values("open_timestamp")
            master_df["volume_traded"] = (master_df.groupby(["tradingsymbol", "date"],sort=False)
                                        ["cumm_volume_trades"].diff().fillna(master_df["cumm_volume_trades"]))

            for expiry_val, group_df in master_df.groupby("expiry"):
                print(
                    f"[chunk {chunk_idx}] {file_name} -> {expiry_val} ({len(group_df)} rows)"
                )
                safe_expiry = str(expiry_val).replace("/", "-").replace("\\", "-")
                part_dir = f"{PARTS_DIR}/{safe_expiry}"
                os.makedirs(part_dir, exist_ok=True)
                part_path = f"{part_dir}/{chunk_idx:03d}.csv"

                write_header = (
                    not os.path.exists(part_path) or os.path.getsize(part_path) == 0
                )
                group_df.to_csv(part_path, mode="a", header=write_header, index=False)

        mark_processed(chunk_idx, file_id)  # only after the append succeeded
        return {"status": "success", "file_id": file_id, "file_name": file_name}
    except Exception as e:
        return {
            "status": "error",
            "file_id": file_id,
            "file_name": file_name,
            "error": str(e),
        }
    finally:
        if os.path.exists(raw_path):
            os.remove(raw_path)


def worker_process_chunk(args):
    """args = (chunk_idx, [(file_id, file_name), ...]). Files run one after another."""
    chunk_idx, chunk = args
    service = authenticate_drive(write_token=False)  # one Drive client per worker
    results = []
    for file_id, file_name in chunk:
        print(f"[chunk {chunk_idx} | pid {os.getpid()}] {file_name}")
        results.append(process_one_file(service, chunk_idx, file_id, file_name))
    return results


# ==========================================
# 5. STITCH: part files -> final expiry files (chunk order = date order)
# ==========================================
def stitch_parts():
    """Concatenate <expiry>/000.csv, 001.csv, ... into data/expiry_files/<expiry>.csv.
    Also safe to run on leftovers from an interrupted run."""
    if not os.path.isdir(PARTS_DIR):
        return
    for expiry in sorted(os.listdir(PARTS_DIR)):
        part_dir = os.path.join(PARTS_DIR, expiry)
        if not os.path.isdir(part_dir):
            continue
        master_path = f"{EXPIRY_DIR}/{expiry}.csv"
        master_exists = os.path.exists(master_path) and os.path.getsize(master_path) > 0

        with open(master_path, "ab") as master_file:
            for part_name in sorted(os.listdir(part_dir)):
                part_path = os.path.join(part_dir, part_name)
                if os.path.getsize(part_path) == 0:
                    continue
                with open(part_path, "rb") as part_file:
                    if master_exists:
                        part_file.readline()  # skip header
                    shutil.copyfileobj(part_file, master_file)
                master_exists = True
        shutil.rmtree(part_dir)


def load_processed_ids():
    ids = set()
    if os.path.exists(PROCESSED_PATH):
        with open(PROCESSED_PATH) as f:
            ids |= {line.strip() for line in f if line.strip()}
    for path in glob.glob(f"{PROCESSED_PARTS_DIR}/*.txt"):
        with open(path) as f:
            ids |= {line.strip() for line in f if line.strip()}
    return ids


def consolidate_processed_logs():
    """Fold the per-chunk logs into the single master log, then remove them."""
    ids = set()
    paths = glob.glob(f"{PROCESSED_PARTS_DIR}/*.txt")
    for path in paths:
        with open(path) as f:
            ids |= {line.strip() for line in f if line.strip()}
    if ids:
        with open(PROCESSED_PATH, "a") as f:
            for fid in sorted(ids):
                f.write(fid + "\n")
    for path in paths:
        os.remove(path)


# ==========================================
# 6. MAIN
# ==========================================
def run_pipeline(max_workers=5, rebuild_manifest=True, start_date=None, end_date=None):
    for d in (TEMP_DIR, EXPIRY_DIR, PARTS_DIR, PROCESSED_PARTS_DIR):
        os.makedirs(d, exist_ok=True)

    # Refresh the token once here, so workers only ever read it.
    authenticate_drive()

    # Fold in anything left behind by an interrupted run before starting a new one.
    stitch_parts()
    consolidate_processed_logs()

    if rebuild_manifest or not os.path.exists(MANIFEST_PATH):
        build_manifest(start_date, end_date)

    manifest = pd.read_csv(MANIFEST_PATH)
    processed_ids = load_processed_ids()
    manifest = manifest[~manifest["file_id"].isin(processed_ids)]
    tasks = list(zip(manifest["file_id"], manifest["file_name"]))
    if not tasks:
        print("Nothing new to process.")
        return

    chunks = split_into_chunks(tasks, max_workers)
    print(
        f"{len(tasks)} files -> {len(chunks)} chunks of sizes {[len(c) for c in chunks]}"
    )

    with concurrent.futures.ProcessPoolExecutor(max_workers=len(chunks)) as executor:
        chunk_results = list(
            executor.map(worker_process_chunk, list(enumerate(chunks)))
        )

    # Log failures from the main process (single writer).
    for res in (r for cr in chunk_results for r in cr):
        if res["status"] == "error":
            print(f"❌ Failed: {res['file_name']} -> {res['error']}")
            with open(ERROR_LOG_PATH, "a") as err:
                err.write(
                    f"[{datetime.now()}] {res['file_name']} - ERROR: {res['error']}\n"
                )

    stitch_parts()
    consolidate_processed_logs()
    print("Done.")


if __name__ == "__main__":
    run_pipeline(
        max_workers=5,
        rebuild_manifest=False,
        start_date="2026-11-05",
        end_date="2026-11-05",
    )
