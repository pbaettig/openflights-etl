import argparse
import re
from collections import defaultdict, namedtuple
from contextlib import contextmanager
from dataclasses import dataclass
from operator import itemgetter
from os.path import isdir, join
from pprint import pprint

import psycopg2


def parse_args():
    def dir_path(d):
        if isdir(d):
            return d
        else:
            raise argparse.ArgumentTypeError(f'{d} is not a valid directory')

    parser = argparse.ArgumentParser()
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', default='5432')
    parser.add_argument('--db-name', default='openflights')
    parser.add_argument('--db-user', default='postgres')
    parser.add_argument('--db-password', default='1234')
    parser.add_argument('--data-dir', default='./data/', type=dir_path)
    return parser.parse_args()


@contextmanager
def connect_db():
    try:
        conn = psycopg2.connect(
            database=args.db_name,
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password
        )
        conn.autocommit = True
        cur = conn.cursor()
        yield cur
    finally:
        cur.close()
        conn.close()

def sanitize_and_split(l):
    # Remove \N, replace with empty string ""
    # This is necessary because the line split is on '",'
    # "Jan Mayen",\N,"JN" -> "Jan Mayen","","JN"
    l = l.replace(r'\N', '""')

    for s in l.strip().split('",'):
        # strip out unwanted characters
        ss = strip(s)

        # yield None if no string is empty
        yield ss if ss else None


def load_countries():
    # base INSERT query without vales
    q = "INSERT INTO countries VALUES "
    with open(join(args.data_dir, 'countries.dat')) as f:
        # build a list of tuples (one per line) from input file, use
        # line number as id
        rows = [(i,)+tuple(sanitize_and_split(l)) for i,l in enumerate(f,start=1)]
        with connect_db() as cur:
            # join all tuples from file into a single value string
            vs = ','.join(cur.mogrify('(%s,%s,%s,%s)', r).decode() for r in rows)
            
            # concat and execute, all values are inserted to DB in one query
            cur.execute(q + vs)

def translate_country(c):
    # Some country names don't correspond with
    # entries in the countries table. This dict is used to
    # manually translate these
    ct = {
        'Burma': 'Myanmar',
        'Kyrgyzstan': 'Kyrgyz Republic',
        'Micronesia': 'Micronesia, Fed. Sts.',
        'Congo (Kinshasa)': 'DR Congo',
        'Congo (Brazzaville)': 'Congo Republic',
        'Saint Vincent and the Grenadines': 'St. Vincent and the Grenadines',
        'Faroe Islands': 'Faeroe Islands',
        'Saint Kitts and Nevis': 'St. Kitts and Nevis',
        'Brunei': 'Brunei Darussalam',
        'Cape Verde': 'Cabo Verde',
        'Svalbard': 'Svalbard and Jan Mayen Islands',
        'East Timor': 'Timor-Leste',
        'Saint Lucia': 'St. Lucia',
        'Saint Pierre and Miquelon': 'St. Pierre and Miquelon'
    }

    return c if not c in ct else ct[c]

def read_airports():
    with open(join(args.data_dir, 'airports.dat')) as f:
        for l in f:
            v = [strip(e) if e != r'\N' else None for e in l.rstrip().split(',')]

 
def strip(s):
    s = s.replace("'", ' ')
    return s.replace('"','')

def load_cities():
    City = namedtuple('City', 
    [   
        'city_name',
        'country_name',
        'utc_offset',
        'dst',
        'timezone'
    ])
    
    # Cities are extracted from the airport list (for normalization)
    cities = defaultdict(list)
    with open(join(args.data_dir, 'airports.dat')) as f:
        for l in f:
            v = [strip(e) if e != r'\N' else None for e in l.rstrip().split(',')]
            city_name = v[2]
            country_name = v[3]
            utc_offset = v[9]
            dst = v[10]
            timezone = v[11]

            
            cities[f'{country_name}/{city_name}'].append(City(city_name, country_name, utc_offset, dst, timezone))


    # cities_multiple_airports = sorted(
    #     [(k, len(v)) for k,v in cities.items() if len(v) > 1],
    #     key=itemgetter(1),
    #     reverse=True)
    
    # keep a list of cities where we're not able to find
    # a matching country in the countries table
    no_country_id = []
    for _,v in cities.items():
        # get the first entry even if there are more
        # and hope that this one has good data...
        city = v[0]

        country = translate_country(city.country_name)

        # try to determine the country_id based off the country_name
        with connect_db() as cur:
            cur.execute('SELECT country_id FROM countries WHERE country_name = %s;', (country,))
            ci = cur.fetchone()
            if not ci:
                print(f'no ID found for "{city.country_name}"')
                no_country_id.append(city.country_name)
                continue

            # We have all the info, let's insert the city into the DB
            q = 'INSERT INTO cities (city_name,country_id,timezone_offset_utc,daylight_saving,timezone) VALUES (%s,%s,%s,%s,%s);'
            cur.execute(q, (
                city.city_name,
                ci,
                city.utc_offset,
                city.dst,
                city.timezone
            ))

    no_country_id.sort()
    print(no_country_id, len(no_country_id))

def load_airports():
    # airport_id      integer GENERATED BY DEFAULT AS IDENTITY,
    # airport_name    varchar(80) NOT NULL,
    # city_id         integer,
    # airport_iata    char(2),
    # airport_icao    char(3),
    # geo_location    point,
    # altitude_ft     integer,
    # type            varchar(40),
    # source          varchar(40),   
    Airport = namedtuple()



if __name__ == "__main__":
    args = parse_args()
    
    # load_countries()
    load_cities()
