# openflights-etl
The scripts in this repo read in the data provided by [OpenFlights](https://openflights.org/data.html), normalize it and write it to a Postgres DB. It was created as somewhat interesting sample data for learning some SQL.

## Create the schema
Assuming that you have some sort of Postgres server running (this was tested using Postgres 11)

1. Create a database
```
postgres=# CREATE DATABASE openflights;
```

2. run `schema/create.sql`
```
psql <schema/create.sql
```

## Load data
1. extract the required .dat files from `data/data.tar.gz` or download them yourself from `data/sources.txt`
2. place all *.dat files in a folder alongside with `data/planes.csv`
3. install the python requirements 
```
python3 -m pip install -r etl/requirements.txt
```
4. run `etl/main.py`, look at the options using `--help`

The initial loading process takes a couple of minutes, depending on your hardware of course.

Alternatively you can also load the SQL dump provided in `dump/dump.sql.gz`, which only takes a couple of seconds.