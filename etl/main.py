#!/usr/bin/env python3

import argparse
import logging
from functools import partial
from operator import attrgetter
from os.path import isdir, join
from traceback import print_exc

from db import OpenflightsDB
from extract import (read_airlines, read_airports, read_countries, read_planes,
                     read_planes_csv, read_routes)
from load import (load_airlines, load_airports, load_cities, load_countries,
                  load_planes, load_routes)

logging.basicConfig(level=logging.INFO)


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


def combine_planes(p1, p2):
    planes_all = set(p1) | set(p2)
    planes_all_iata_icao = zip([f'{p.plane_iata}{p.plane_icao}' for p in planes_all], planes_all)
    inserted = set()
    for iata_icao, p in planes_all_iata_icao:
        if iata_icao in inserted:
            continue

        inserted.add(iata_icao)
        yield(p)


def display_table_stats(t, s):
    print(f'# Stats for {t}')
    print(f'rows inserted: {s.insert_ok}')

    total_failed = sum(s.insert_errors.values())
    print(f'rows failed: {total_failed}')
    for ex, num in s.insert_errors.items():
        print(f'  - {ex}: {num}')
    print()


def main(args):
    airlines = partial(read_airlines, join(args.data_dir, 'airlines.dat'))
    airports = partial(read_airports, join(args.data_dir, 'airports.dat'))
    countries = partial(read_countries, join(args.data_dir, 'countries.dat'))
    planes = partial(read_planes, join(args.data_dir, 'planes.dat'))
    planes_csv = partial(read_planes_csv, join(args.data_dir, 'planes.csv'))
    routes = partial(read_routes, join(args.data_dir, 'routes.dat'))


    load_countries(countries(), database)
    display_table_stats('countries', database.table_stats['countries'])


    load_cities(airports(), database)
    display_table_stats('cities', database.table_stats['cities'])

    load_airports(airports(), database)
    display_table_stats('airports', database.table_stats['airports'])

    load_airlines(airlines(), database)
    display_table_stats('airlines', database.table_stats['airlines'])

    load_planes(combine_planes(planes(), planes_csv()), database)
    display_table_stats('planes', database.table_stats['planes'])

    logging.basicConfig(level=logging.DEBUG)

    load_routes(routes(), database)
    display_table_stats('routes', database.table_stats['routes'])
    display_table_stats('route_plane', database.table_stats['route_plane'])
  

if __name__ == "__main__":
    args = parse_args()
    database = OpenflightsDB(
        args.db_name,
        args.db_host,
        args.db_port,
        args.db_user,
        args.db_password
    )
    
    try:
        main(args)
    except KeyboardInterrupt:
        logging.debug('Ctrl-C. Bye.')
    except:
        print_exc()
    finally:
        database.close()
