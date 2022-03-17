# IT3010

IT3010 V22 - Group 2

## Contributors

- Lars-Olav Vågene
- Ingvild Løver Thon
- Eirik Schøien
- Christian Axell
- Lukas Tveiten

## Setup

Prerequisites:

- Python
- Docker

Steps:

1. Make a copy of the `.env-template` file and rename it `.env`
2. Run `docker-compose up`
3. Connect to Adminer in a browser at `localhost:8080` (server="psql")
4. Download the dataset from https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/ and place the user folders (000-181) in the folder `./data`

## Experiment

To run the experiments with the CLI:

```bash
# Drop and create SQLite tables for storing experimental results
py cli.py prepare

# Run experiment with desired iterations and total size
py cli.py run -i 3 -n 5000
```

The results are stored in a SQLite database, which can be easily accessed
with Python or a GUI tool like DB Browser.
