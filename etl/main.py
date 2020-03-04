#!/usr/bin/env python3

import argparse
from functools import partial
from os.path import join, isdir

from db import OpenflightsDB
from extract import read_airlines, read_airports, read_countries, read_planes,read_planes_csv, read_routes
from load import load_airlines, load_airports, load_cities, load_countries, load_planes, load_routes

from traceback import print_exc
import logging

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
    parser.add_argument('--data-dir', default='../data/', type=dir_path)
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


def format_db_stats(t, s):
    return f'Stats for table "{t}": {s.insert_ok} row(s) inserted, {s.insert_duplicate} duplicate(s), {s.insert_error} failed'

from operator import attrgetter
def main(args):
    airlines = partial(read_airlines, join(args.data_dir, 'airlines.dat'))
    airports = partial(read_airports, join(args.data_dir, 'airports.dat'))
    countries = partial(read_countries, join(args.data_dir, 'countries.dat'))
    planes = partial(read_planes, join(args.data_dir, 'planes.dat'))
    planes_csv = partial(read_planes_csv, join(args.data_dir, 'planes.csv'))
    routes = partial(read_routes, join(args.data_dir, 'routes.dat'))


    load_countries(countries(), database)
    logging.info(format_db_stats('countries', database._table_stats['countries']))

    load_cities(airports(), database)
    logging.info(format_db_stats('cities', database._table_stats['cities']))

    load_airports(airports(), database)
    logging.info(format_db_stats('airports', database._table_stats['airports']))

    load_airlines(airlines(), database)
    logging.info(format_db_stats('airlines', database._table_stats['airlines']))

    load_planes(combine_planes(planes(), planes_csv()), database)
    logging.info(format_db_stats('planes', database._table_stats['planes']))

    logging.basicConfig(level=logging.DEBUG)

    load_routes(routes(), database)
    logging.info(format_db_stats('routes', database._table_stats['routes']))
    logging.info(format_db_stats('route_plane', database._table_stats['route_plane']))



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

    