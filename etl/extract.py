import csv
import re

from lineparser import LineParser
from model import AirlineDat, AirportDat, CountryDat, PlaneDat, RouteDat
from transform import convert_if_exists, translate_country


def read_countries(fname):
    lp = LineParser([
        'q',    # name
        'q',    # iso
        'q',    # dafif_code
    ])
    with open(fname) as f:
        for l in f:
            v = lp.parse(l)

            yield CountryDat(*v)


def read_airports(fname):
    lp = LineParser([
        'nq',   # id
        'q',    # name
        'q',    # city
        'q',    # country
        'q',    # iata
        'q',    # icao
        'nq',   # lat
        'nq',   # lon
        'nq',   # altitude
        'nq',   # tz offset
        'q',    # DST
        'q',    # tz name
        'q',    # type
        'q'     # source
    ])
    with open(fname) as f:
        for l in f:
            # replace double double-quotes with a single '
            # it's like that in the source file but it would
            # be a bit of a hassle to adapt LineParser to handle
            # this case correctly. So I won't
            leading_quotes_converted = re.sub(r'""([^,])', r"'\1", l)
            l = re.sub(r'([^,])""', r"\1'", leading_quotes_converted)

            v = lp.parse(l)
            ai = convert_if_exists(v[0], int)
            an = v[1]
            cy = v[2]
            ct = v[3]
            iata = v[4]
            icao = v[5]
            lat = convert_if_exists(v[6], float)
            lon = convert_if_exists(v[7], float)
            alt = convert_if_exists(v[8], int)
            tzo = convert_if_exists(v[9], float)
            dst = v[10]
            tzn = v[11]
            tp = v[12]
            src = v[13]

            yield AirportDat(ai, an, cy, ct, iata, icao, lat, lon, alt, tzo, dst, tzn, tp, src)


def read_airlines(fname):
    def _sanitize(s):
        # remove some uglyness from source data, replacing with None
        nones = ('N/A', '-', '', r"\\'", r"\\'\\")
        if s in nones:
            return None
        return s


    lp = LineParser([
        'nq',   # id
        'q',    # name
        'q',    # alias
        'q',    # iata
        'q',    # icao
        'q',    # callsign
        'q',    # country
        'q',    # active
    ])
    
    with open(fname) as f:
        for l in f:
            v = [_sanitize(v) for v in lp.parse(l)]
            if v[0] == '-1':
                # skip the "Unknown" airline
                continue

            v[0] = int(v[0])
            v[6] = translate_country(v[6])

            yield AirlineDat(*v)

def read_planes_csv(fname):
    def _skip(r):
        if r[0] == '' or r[0] == 'n/a':
            return True
        if r[2] == '' or r[2] == 'n/a':
            return True

        return False

    def _sanitize(s):
        if s == '' or s == 'n/a':
            return None
        return s


    with open(fname) as f:
        reader = csv.reader(f)
        # discard header
        _ = next(reader)
        for r in reader:
            if _skip(r):
                continue
            rs = [_sanitize(s) for s in r]
            yield PlaneDat(rs[2],rs[0], rs[1]) 


def read_planes(fname):
    lp = LineParser([
        'q',    # name
        'q',    # iata
        'q',    # icao
    ])
    
    with open(fname) as f:
        for l in f:
            v = lp.parse(l)
            yield PlaneDat(*v)


    # add some prominent missing planes here that came up during
    # loading of route_plane
    missing_planes_iata = {
        'L4T': 'Let L 410 Turbojet',
        '313': 'Airbus A310-300',
        '32S': 'Airbus A318/319/320/321'
    }
    for iata,name in missing_planes_iata.items():
        yield PlaneDat(name, iata, None)


def read_routes(fname):
    def _airline_iata_icao(s):
        return (s,None) if len(s) == 2 else (None, s)

    def _airport_iata_icao(s):
        return (s,None) if len(s) == 3 else (None, s)



    lp = LineParser([
        'nq',    # airline icao
        'nq',    # airline id (openflights)
        'nq',    # source airport, iata (3) or icao (4)
        'nq',    # source airport id (openflights)
        'nq',    # dest. airport, iata (3) or icao (4)
        'nq',    # dest. airport id (openflights)
        'nq',   # codeshare
        'nq',   # stops
        'nq'    # equipment
    ])

    with open(fname) as f:
        for l in f:
            v = lp.parse(l)
            al_iata, al_icao = _airline_iata_icao(v[0])
            al_id = v[1]
            src_ap_iata, src_ap_icao = _airport_iata_icao(v[2])
            src_ap_id = v[3]
            dest_ap_iata, dest_ap_icao = _airport_iata_icao(v[4])
            dest_ap_id = v[5]
            cs = v[6]
            st = convert_if_exists(v[7], int)
            eq = v[8].split(' ') if v[8] else []

            yield RouteDat(
                al_iata,
                al_icao,
                al_id,
                src_ap_iata,
                src_ap_icao,
                src_ap_id,
                dest_ap_iata,
                dest_ap_icao,
                dest_ap_id,
                cs,
                st,
                eq
            )
