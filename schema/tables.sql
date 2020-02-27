CREATE TABLE airlines (
    id          integer PRIMARY KEY,
    name        varchar(80) NOT NULL,
    alias       varchar(40),
    iata        char(2),
    icao        char(3),
    callsign    varchar(40),
--    country ...
    active boolean
);

CREATE TABLE airports (
    id          integer PRIMARY KEY,
    name        varchar(80) NOT NULL,
--  city       varchar(40),
--  country
    iata        char(2),
    icao        char(3),
    geo_location point,
    altitude_ft integer,
--  timezone_offset_utc float,
--  daylight_saving char(1),
--  timezone    varchar(40),
--  type    varchar(40),
-- source varchar(40),
);

CREATE TABLE countries (
    id         integer PRIMARY KEY,
    name       varchar(40) NOT NULL, 
    iso        char(2),
    dafif       char(2),
);


CREATE TABLE planes (
    id         integer PRIMARY KEY,
    name       varchar(40) NOT NULL, 
    iata        char(2),
    icao        char(3),
);

CREATE TABLE routes (
--  airline,
-- src_airport
-- dest_airport
    codeshare boolean,
    stops       integer,
-- equipment (list of plane types)
);