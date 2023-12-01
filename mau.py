import sys
import os
import random
import string
import datetime


from collections import Counter

# get values from .env file
from dotenv import load_dotenv

load_dotenv()

try:
    import pandas as pd
except ImportError:
    print("Please install pandas library: pip install pandas")
    sys.exit(1)

MAU_CLIENTS = os.getenv("MAU_CLIENTS").split(",")
MAU_EMPTY_ROWS_AFTER = os.getenv("MAU_EMPTY_ROWS_AFTER").split(",")
MAU_RENAMED_CLIENTS = dict(
    pair.split(",") for pair in os.getenv("MAU_RENAMED_CLIENTS").split(";")
)


def reorder_mau_csv(original_filename):
    """
    Reorders a CSV file based on the specified configuration and saves it to a new file.

    Args:
      original_filename (str): The path of the original CSV file.

    Returns:
      None
    """

    # check that all env variables are set
    if len(MAU_CLIENTS) == 1:
        raise ValueError("MAU_CLIENTS is not set")
    if len(MAU_EMPTY_ROWS_AFTER) == 1:
        raise ValueError("MAU_EMPTY_ROWS_AFTER is not set")
    if len(MAU_RENAMED_CLIENTS) == 1:
        raise ValueError("MAU_RENAMED_CLIENTS is not set")

    # read csv file
    df = pd.read_csv(original_filename, encoding="utf-8")

    renamed_clients = MAU_RENAMED_CLIENTS

    # handle renamed clients
    CLIENT = "detail.data.client_name"

    df[CLIENT] = df[CLIENT].replace(renamed_clients)

    # add missing clients
    all_clients = MAU_CLIENTS

    # check that there are no duplicate values in CLIENT column
    if not df[CLIENT].is_unique:
        raise ValueError("Duplicate values found in 'CLIENT' column", df[CLIENT])

    # check that there are no duplicate values in all_clients
    for client in all_clients:
        if client not in df[CLIENT].values:
            print(f"Source data doesn't have: {client}")
            df = df._append({CLIENT: client, "count": 0}, ignore_index=True)

    # convert count to int
    df["count"] = df["count"].astype(int)

    # Reorder data based on Excel file order
    df = df.set_index(CLIENT).reindex(all_clients).reset_index()

    # notify if there are any missing clients
    if df["count"].isnull().values.any():
        print("Missing clients")
        print(df[df["count"].isnull()][CLIENT].values)

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

        df = pd.concat([df.iloc[:index], new_row, df.iloc[index:]]).reset_index(
            drop=True
        )
        if client == "Vuokraovi":
            # add second empty row after 'Vuokraovi'
            df = pd.concat([df.iloc[:index], new_row, df.iloc[index:]]).reset_index(
                drop=True
            )

    # write to csv

    # filename is "reordered_" + original + current date + ".csv"
    # Current date in YYYYMMDD format
    current_date = datetime.datetime.now().strftime("%Y%m%d")

    # create a random word
    def randomword(length):
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    # create a short hash
    short_hash = str(hash(original_filename + current_date + randomword(5)))[-5:]

    # New filename
    new_filename = f"reordered_{original_filename[:-4]}_{current_date}_{short_hash}.csv"

    # write path is /output create it if it doesn't exist
    if not os.path.exists("output"):
        os.makedirs("output")

    df.to_csv(
        f"output/{new_filename}", index=False, float_format="%.0f", encoding="utf-8-sig"
    )


# get filename from command line argument
argv = sys.argv

reorder_mau_csv(argv[1])
