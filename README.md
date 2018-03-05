# ETL_Google
Experiments with google products

## Part one - BigQuery

- Python Wrapper for Google API library

## Part Two - CloudSQL (google MySQL)

- Up & running. Todo: Test the table creation & data insertion

## Part Three - Linting & testing

- pycodestyle (/)
- pyflakes (/)
- pytest (/) (sort of)
- CI with travis (/)

# Requirements beyond pip installs.

- private/ folder
- log/ folder
- a google key for cloud and stuff
- private/config.ini file looking a bit like this:
```
[bigquery]
dump_file   = dump/output.json

[cloudsql]
db_name     = noaa_agg
username    = root
password    = thisismyrootpassword
proxy_port  = 3333

[logging]
logfile     = log/ELT.log
loglevel    = 10
```
