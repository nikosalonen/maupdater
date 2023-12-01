import pandas as pd
import datetime
import sys
import os

# get values from .env file
from dotenv import load_dotenv

load_dotenv()

MAU_CLIENTS = os.getenv("MAU_CLIENTS").split(",")
MAU_EMPTY_ROWS_AFTER = os.getenv("MAU_EMPTY_ROWS_AFTER").split(",")
MAU_RENAMED_CLIENTS = dict(
    pair.split(",") for pair in os.getenv("MAU_RENAMED_CLIENTS").split(";")
)


# if any of the env variables are unset (as in empty string), exit
if (
    len(MAU_CLIENTS) == 1
    or len(MAU_EMPTY_ROWS_AFTER) == 1
    or len(MAU_RENAMED_CLIENTS) == 1
):
    print("Missing env variables")
    sys.exit(1)

# get filename from parameter
original_filename = sys.argv[1]


# read csv file
df = pd.read_csv(original_filename, encoding="utf-8")

renamed_clients = MAU_RENAMED_CLIENTS

# handle renamed clients
CLIENT = "detail.data.client_name"

df[CLIENT] = df[CLIENT].replace(renamed_clients)

# add missing clients
all_clients = MAU_CLIENTS


for client in all_clients:
    if client not in df[CLIENT].values:
        df = df._append({CLIENT: client, "count": 0}, ignore_index=True)

# convert count to int
df["count"] = df["count"].astype(int)

# Reorder data based on Excel file order
df = df.set_index(CLIENT).reindex(all_clients).reset_index()

# notify if there are any missing clients
if df["count"].isnull().values.any():
    print("Missing clients")

# notify if there are clients that are not in the list
if df[CLIENT].isin(all_clients).all() == False:
    print("There are clients that are not in the list")
    # print clients that are not in the list
    print(df[~df[CLIENT].isin(all_clients)][CLIENT].values)

# add empty row after each business area
clients_to_add_empty_row_after = MAU_EMPTY_ROWS_AFTER

for client in reversed(clients_to_add_empty_row_after):
    index = df[df[CLIENT] == client].index[0] + 1
    new_row = pd.DataFrame({CLIENT: "", "count": pd.NA}, index=[index])

    df = pd.concat([df.iloc[:index], new_row, df.iloc[index:]]).reset_index(drop=True)
    if client == "Vuokraovi":
        # add second empty row after 'Vuokraovi'
        df = pd.concat([df.iloc[:index], new_row, df.iloc[index:]]).reset_index(
            drop=True
        )


# write to csv

# filename is "reordered_" + original + current date + ".csv"
# Current date in YYYYMMDD format
current_date = datetime.datetime.now().strftime("%Y%m%d")

# New filename
new_filename = f"reordered_{original_filename[:-4]}_{current_date}.csv"

# write path is /output create it if it doesn't exist


if not os.path.exists("output"):
    os.makedirs("output")

df.to_csv(
    f"output/{new_filename}", index=False, float_format="%.0f", encoding="utf-8-sig"
)
