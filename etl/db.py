import psycopg2
from contextlib import contextmanager
from dataclasses import astuple, asdict
from transform import translate_country

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
        

    def __enter__(self):
        self._cur = self._conn.cursor()
        return  self._cur


    def __exit__(self, *args):
        self._cur.close() 


    def _insert(self, table, values, update=False):
        values_q = ','.join('%s' for _ in values)
        q = f'INSERT INTO {table} VALUES ({values_q});'
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, values)
            except psycopg2.errors.UniqueViolation:
                if update:
                    raise NotImplementedError


    def _insert_kv(self, table, value_dict, update=False):
        cols_q = ','.join(value_dict.keys())
        values_q = ','.join('%s' for _ in value_dict.values())
        q = f'INSERT INTO {table} ({cols_q}) VALUES ({values_q});'
        
        with self._conn.cursor() as cur:
            try:
                cur.execute(q, tuple(value_dict.values()))
            except psycopg2.errors.UniqueViolation:
                if update:
                    raise NotImplementedError


    def close(self):
        self._conn.close()


    def get_country_by_name(self, n):
        with self._conn.cursor() as cur:
            cur.execute('SELECT country_id FROM countries WHERE country_name = %s;', (n,))
            r = cur.fetchone() 
            return r[0] if r else None 


    def get_city_by_name(self, n):
        with self._conn.cursor() as cur:
            cur.execute('SELECT city_id FROM cities WHERE city_name = %s;', (n,))
            r = cur.fetchone() 
            return r[0] if r else None 


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
            self._insert_kv('routes',v)
        except psycopg2.errors.ForeignKeyViolation:
            print(f'insert_route: no src/dest airport found with ID {v["src_airport_id"]}/{v["dest_airport_id"]}. Skipping.')
