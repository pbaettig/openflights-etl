from model import CityDat, AirlineDat, AirportDat, CountryDat
from extract import read_airlines, read_airports, read_countries
from collections import defaultdict
from dataclasses import astuple, replace
from transform import translate_country



def load_countries(countries, db):
    for c in countries:
        db.insert_country(c)


def load_airlines(airlines, db):
    for al in airlines:
        db.insert_airline(al)


def load_cities(airports, db):
    # Cities are extracted from the airport list (for normalization)
    cities = defaultdict(list)
    for ap in airports:
        if ap.airport_name == 'Powidz Military Air Base':
            ap = replace(ap, city='Powidz')

        if not ap.city:
            print(f'load_cities: airport "{ap.airport_name}" does not have a city. Skipping.')
            continue

        cities[f'{ap.country}/{ap.city}'].append(
            CityDat(
                None, 
                None,
                ap.country,
                ap.city,
                ap.timezone_offset,
                ap.daylight_saving,
                ap.timezone
            ))

    # keep a list of cities where we're not able to find
    # a matching country in the countries table
    no_country_id = []
    for _,v in cities.items():
        # get the first entry even if there are more
        # and hope that this one has good data...
        city = v[0]
        db.insert_city(city)

       

    no_country_id.sort()


def load_airports(airports, db):
    # airport_id      integer GENERATED BY DEFAULT AS IDENTITY,
    # airport_name    varchar(80) NOT NULL,
    # city_id         integer,
    # airport_iata    char(2),
    # airport_icao    char(3),
    # geo_location    point,
    # altitude_ft     integer,
    # type            varchar(40),
    # source          varchar(40),
    for ap in airports:
        db.insert_airport(ap)


def load_planes(planes, db):
    for p in planes:
        db.insert_plane(p)


def load_routes(routes, db):
    for r in routes:
        db.insert_route(r)