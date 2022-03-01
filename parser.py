import os
from tqdm import tqdm
from utils import time_this


def get_paths() -> list[tuple[int, str]]:
    """
    Find all .plt files in data set.
    Returns a list of tuples, with username as integer and full path for each
    file.
    """
    filepaths = []
    for root, _, files in os.walk("data", topdown=True):
        for filename in files:
            if filename.endswith(".plt"):
                path = os.path.join(root, filename)
                username = path.split("\\")[1]
                filepaths.append((int(username), path))
    return filepaths


@time_this
def read_data(max_folders=None):
    """
    Read up to MAX_FOLDERS .plt files from disk and parse them into a standard
    format.
    """
    folders = get_paths()
    if max_folders is not None:
        folders = folders[:max_folders]

    data = []
    progress = tqdm(total=len(folders))
    for username, path in folders:
        progress.set_description(f"Processing {path}")
        file = open(path, "r")
        rows = file.readlines()[6:]  # Skip header
        rows = [x.strip().split(",") for x in rows]
        for row in rows:
            latitude = float(row[0])
            longitude = float(row[1])
            altitude = float(row[3])
            date = row[5]
            time = row[6]
            data.append([username, latitude, longitude, altitude, date, time])
        file.close()
        progress.update(1)
    return data


@time_this
def pg_parse(data: list):
    """
    Parse a list of preprocessed rows and convert them into a format suited to
    the Postgres version of the schema.
    """
    pg_data = []
    for user, lat, lon, alt, date, time in data:
        # Note order swapping: (lat, lon) => (lon, lat)
        pg_data.append([user, lon, lat, alt, date, time])
    return pg_data
