import argparse
from functools import partial
from os.path import join, isdir

from db import OpenflightsDB
from extract import read_airlines, read_airports, read_countries, read_planes, read_routes
from load import load_airlines, load_airports, load_cities, load_countries, load_planes, load_routes

from traceback import print_exc

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


def main(args):
    airlines = partial(read_airlines, join(args.data_dir, 'airlines.dat'))
    airports = partial(read_airports, join(args.data_dir, 'airports.dat'))
    countries = partial(read_countries, join(args.data_dir, 'countries.dat'))
    planes = partial(read_planes, join(args.data_dir, 'planes.dat'))
    routes = partial(read_routes, join(args.data_dir, 'routes.dat'))

    load_countries(countries(), database)
    load_cities(airports(), database)
    load_airports(airports(), database)
    load_airlines(airlines(), database)
    load_planes(planes(), database)
    load_routes(routes(), database)


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
        print('Ctrl-C. Bye.')
    except:
        print_exc()
    finally:
        database.close()

    