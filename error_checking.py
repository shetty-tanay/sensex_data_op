import re
import pandas as pd

INPUT_FILE = "data/error_files_log.txt"
OUTPUT_FILE = "skipped_dates.csv"
data_file = "data/manifest.csv"

holidays_df = pd.read_csv("holiday_list.csv")
data_df = pd.read_csv(data_file)

skipped_dates = []

with open(INPUT_FILE, "r", encoding="utf-8") as file:
    for line in file:
        match = re.search(r"\[(\d{4}-\d{2}-\d{2})\] bot folder .* skipped", line)

        if match:
            skipped_dates.append(match.group(1))

df = pd.DataFrame(skipped_dates, columns=["date"])
holidays_df["date"] = pd.to_datetime(holidays_df["Date"]).dt.strftime("%Y-%m-%d")
holidays_df = holidays_df[["date", "Holidays"]]
df = df[~df["date"].isin(holidays_df["date"])]
df["weekend"] = pd.to_datetime(df["date"]).dt.dayofweek >= 5
df = df[~df["weekend"]]
df["filename"] = df["date"].apply(
    lambda x: f"ticklogs-{pd.to_datetime(x).strftime('%d_%m_%Y')}.csv"
)
data_df = data_df["file_name"]
df = df[~df["filename"].isin(data_df)]
df.to_csv(OUTPUT_FILE, index=False)
print(f"Total skipped dates: {len(df)}")
print(df)
