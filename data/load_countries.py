import psycopg2 as pg
import logging
import re

unq_re = re.compile(r'^"([^"]+)"$')

def unquote(s):
    m = re.match(r'^"([^"]+)"$', s)
    if not m:
        return s
    
    return m.groups()[0]

def make_null(s):
    if s == r'\N':
        return None
    
    return s

def sanitize(s):
    return s.replace("'", ' ')

if __name__ == "__main__":
    with open('countries.dat') as f:
        for line in f:
            values = [make_null(sanitize(unquote(s))) for s in line.split(',') ]
            if None in values:
                continue

            logging.debug("INSERT INTO countries (country_name,iso,dafif) VALUES ('{}','{}','{}');".format(*values))