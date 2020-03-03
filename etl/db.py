from contextlib import contextmanager
from dataclasses import asdict, astuple
from pprint import pprint
from traceback import print_exc

import psycopg2

from transform import translate_country, translate_plane


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
                if update:
                    raise NotImplementedError
            else:
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
                if update:
                    raise NotImplementedError
            else:
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
            self._plane_iata_cache[n] = v
            return v


    def get_country_by_name(self, n):
        if n in self._country_name_cache:
            return self._country_name_cache[n]
    
        with self._conn.cursor() as cur:
            cur.execute('SELECT country_id FROM countries WHERE country_name = %s;', (n,))
            r = cur.fetchone() 
            v = r[0] if r else None
            self._country_name_cache[n] = v
            return v


    def get_city_by_name(self, n):
        if n in self._city_name_cache:
            return self._city_name_cache[n]

        with self._conn.cursor() as cur:
            cur.execute('SELECT city_id FROM cities WHERE city_name = %s;', (n,))
            r = cur.fetchone() 
            v = r[0] if r else None
            self._city_name_cache[n] = v
            return v


    def insert_country(self, country):
        v = asdict(country)
        self._insert_kv('countries', v)


    def insert_city(self, city):
        v = asdict(city)
        cn = translate_country(v.pop('country_name'))
        del v['city_id']
       
        ci = self.get_country_by_name(cn)
        if not ci:
            print(f'insert_city: no city ID found for "{cn}". Skipping.')
            return

        v['country_id'] = ci
        self._insert_kv('cities', v)
        

    def insert_airline(self, airline):
        v = asdict(airline)
        ci = self.get_country_by_name(v.pop('country'))
        if not ci:
            print(f'insert_airline: no country ID found for "{airline.country}". Skipping.')
            return

        self._insert_kv('airlines', v)
               

    def insert_airport(self, airport):
        cn = airport.city
        ci = self.get_city_by_name(cn)
        if not ci:
            print(f'insert_airport: no city ID found for "{cn}". Skipping.')
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
        q = 'INSERT INTO airports VALUES (%s,%s,%s,%s,%s,point(%s,%s),%s,%s,%s)'
        
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, apdb)
            except psycopg2.errors.UniqueViolation:
                return


    def insert_plane(self, plane):
        v = asdict(plane)
        self._insert_kv('planes', v)


    def insert_route(self, route):
        keep = [
            'airline_id',
            'src_airport_id',
            'dest_airport_id',
            'route_codeshare',
            'route_stops'
        ]
        v = {k:v for k,v in asdict(route).items() if k in keep}

        try:
            route_id = self._insert_kv('routes',v)
        except psycopg2.errors.ForeignKeyViolation:
            print(f'insert_route: no src/dest airport found with ID {v["src_airport_id"]}/{v["dest_airport_id"]}. Skipping.')
            return

        
        try:
            for p in route.route_equipment_iata:
                p_id = self.get_plane_by_iata(translate_plane(p))
                if not p_id:
                    print(f'insert_route: no plane_id found for {p}')
                    return

                self._insert('route_plane',(route_id, p_id))
        except:
            print_exc()
