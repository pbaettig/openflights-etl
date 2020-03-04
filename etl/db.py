import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, astuple, dataclass
from pprint import pprint
from traceback import print_exc

import psycopg2

from model import PlaneDat
from transform import translate_country, translate_plane


@dataclass
class TableStats:
    insert_ok : int = 0
    insert_error : int = 0
    insert_duplicate : int = 0

    def add_ok(self, i=1):
        self.insert_ok += i

    def add_error(self, i=1):
        self.insert_error += i

    def add_duplicate(self, i=1):
        self.insert_duplicate += i


class OpenflightsDB:
    def __init__(self, db_name, db_host, db_port, db_user, db_password):
        self._conn = psycopg2.connect(
            database=db_name,
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password
        )
        self._conn.autocommit = True
        
        self._tables_pk = {
            'airlines': 'airline_id',
            'airports': 'airport_id',
            'cities': 'city_id',
            'countries': 'country_id',
            'planes': 'plane_id',
            'routes': 'route_id',
            'route_plane': 'route_id,plane_id'
        }

        self._table_stats = defaultdict(TableStats)

        self._plane_iata_cache = {}
        self._country_name_cache = {}
        self._city_name_cache = {}


    def __enter__(self):
        self._cur = self._conn.cursor()
        return  self._cur


    def __exit__(self, *args):
        self._cur.close() 


    def _insert(self, table, values, update=False):
        pk = self._tables_pk.get(table, None)
        if not pk:
            raise ValueError(f'dont know PK for table {table}')

        values_q = ','.join('%s' for _ in values)
        q = f'INSERT INTO {table} VALUES ({values_q}) RETURNING {pk};'
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, values)
            except psycopg2.errors.UniqueViolation:
                self._table_stats[table].add_duplicate()
                if update:
                    raise NotImplementedError
            except Exception:
                self._table_stats[table].add_error()
            else:
                self._table_stats[table].add_ok()
                return cur.fetchone()[0]


    def _insert_kv(self, table, value_dict, update=False):
        pk = self._tables_pk.get(table, None)
        if not pk:
            raise ValueError(f'dont know PK for table {table}')
        cols_q = ','.join(value_dict.keys())
        values_q = ','.join('%s' for _ in value_dict.values())
        q = f'INSERT INTO {table} ({cols_q}) VALUES ({values_q}) RETURNING {pk};'
        
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, tuple(value_dict.values()))
            except psycopg2.errors.UniqueViolation:
                self._table_stats[table].add_duplicate()
                if update:
                    raise NotImplementedError
            except Exception:
                self._table_stats[table].add_error()
            else:
                self._table_stats[table].add_ok()
                return cur.fetchone()[0]


    def close(self):
        self._conn.close()


    def get_plane_by_iata(self, n):
        if n in self._plane_iata_cache:
            return self._plane_iata_cache[n]

        with self._conn.cursor() as cur:
            cur.execute('SELECT plane_id FROM planes WHERE plane_iata = %s;', (n,))
            r = cur.fetchone()
            v = r[0] if r else None
            if v:
                self._plane_iata_cache[n] = v
            return v


    def get_country_by_name(self, n):
        if n in self._country_name_cache:
            return self._country_name_cache[n]
    
        with self._conn.cursor() as cur:
            cur.execute('SELECT country_id FROM countries WHERE country_name = %s;', (n,))
            r = cur.fetchone() 
            v = r[0] if r else None
            if v:
                self._country_name_cache[n] = v
            return v


    def get_city_by_name(self, n):
        if n in self._city_name_cache:
            return self._city_name_cache[n]

        with self._conn.cursor() as cur:
            cur.execute('SELECT city_id FROM cities WHERE city_name = %s;', (n,))
            r = cur.fetchone() 
            v = r[0] if r else None
            if v:
                self._city_name_cache[n] = v
            return v


    def insert_country(self, country):
        v = asdict(country)
        return self._insert_kv('countries', v)


    def insert_city(self, city):
        v = asdict(city)
        cn = translate_country(v.pop('country_name'))
        del v['city_id']
       
        ci = self.get_country_by_name(cn)
        if not ci:
            logging.debug(f'insert_city: no country ID found for "{cn}". Skipping.')
            return

        v['country_id'] = ci
        return self._insert_kv('cities', v)
        

    def insert_airline(self, airline):
        v = asdict(airline)
        ci = self.get_country_by_name(v.pop('country'))
        if not ci:
            logging.debug(f'insert_airline: no country ID found for "{airline.country}". Skipping.')
            return

        return self._insert_kv('airlines', v)
               

    def insert_airport(self, airport):
        cn = airport.city
        ci = self.get_city_by_name(cn)
        if not ci:
            logging.debug(f'insert_airport: no city ID found for "{cn}". Skipping.')
            return

        apdb = (
            airport.airport_id,
            airport.airport_name,
            ci,
            airport.airport_iata,
            airport.airport_icao,
            airport.latitude, airport.longitude,
            airport.altitude_ft,
            airport.type,
            airport.source,
        )
        q = 'INSERT INTO airports VALUES (%s,%s,%s,%s,%s,point(%s,%s),%s,%s,%s) RETURNING airport_id;'
        
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, apdb)
            except psycopg2.errors.UniqueViolation:
                self._table_stats['airports'].add_duplicate()
                return
            except Exception:
                self._table_stats['airports'].add_error()
            else:
                self._table_stats['airports'].add_ok()
                return cur.fetchone()[0]


    def insert_plane(self, plane):
        v = asdict(plane)
        return self._insert_kv('planes', v)


    def insert_route(self, route):
        keep = [
            'airline_id',
            'src_airport_id',
            'dest_airport_id',
            'route_codeshare',
            'route_stops'
        ]
        v = {k:v for k,v in asdict(route).items() if k in keep}


        route_id = self._insert_kv('routes',v)
       

        for p in route.route_equipment_iata:
            p_id = self.get_plane_by_iata(translate_plane(p))
            if not p_id:
                # if we don't know the plane insert a DUMMY plane 
                # so that we can still insert the route
                p_id = self.insert_plane(PlaneDat('DUMMY', p, None))


            self._insert('route_plane',(route_id, p_id))
