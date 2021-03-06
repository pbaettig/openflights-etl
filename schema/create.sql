-- CREATE DATABASE openflights;


CREATE TABLE countries (
    country_id      integer GENERATED BY DEFAULT AS IDENTITY,
    country_name    varchar(40) NOT NULL, 
    country_iso             char(2),
    country_dafif           char(2),
    CONSTRAINT unq_country_name UNIQUE(country_name),
    CONSTRAINT unq_country_iso UNIQUE(country_iso),
    CONSTRAINT unq_country_dafif UNIQUE(country_dafif),
    PRIMARY KEY (country_id)
);
CREATE TABLE planes (
    plane_id    integer GENERATED BY DEFAULT AS IDENTITY,
    plane_name        varchar(80) NOT NULL, 
    plane_iata        char(3),
    plane_icao        char(4),
    CONSTRAINT unq_plane_iata UNIQUE(plane_iata),
    CONSTRAINT unq_plane_icao UNIQUE(plane_icao),
    PRIMARY KEY (plane_id)
);

CREATE TABLE cities (
    city_id             integer GENERATED BY DEFAULT AS IDENTITY,
    city_name           varchar(40) NOT NULL, 
    country_id          integer,
    city_timezone_offset_utc float,
    city_daylight_saving     char(1),
    city_timezone            varchar(40),
    PRIMARY KEY (city_id),
    CONSTRAINT unq_city UNIQUE(city_name,country_id),
    FOREIGN KEY (country_id) REFERENCES countries (country_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE airlines (
    airline_id      integer GENERATED BY DEFAULT AS IDENTITY,
    airline_name    varchar(90) NOT NULL,
    airline_alias   varchar(40),
    airline_iata    char(2),
    airline_icao    char(3),
    airline_callsign        varchar(40),
    country_id      integer,
    airline_active          boolean,
    PRIMARY KEY (airline_id),
    CONSTRAINT unq_airline UNIQUE(airline_name,airline_alias),
    FOREIGN KEY (country_id) REFERENCES countries (country_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE airports (
    airport_id      integer GENERATED BY DEFAULT AS IDENTITY,
    airport_name    varchar(80) NOT NULL,
    city_id         integer,
    airport_iata    char(3),
    airport_icao    char(4),
    airport_geo_location    point,
    airport_altitude_ft     integer,
    type            varchar(40),
    source          varchar(40),
    PRIMARY KEY (airport_id),
    FOREIGN KEY (city_id) REFERENCES cities (city_id) ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE TABLE routes (
    route_id        integer GENERATED BY DEFAULT AS IDENTITY,
    airline_id      integer NOT NULL,
    src_airport_id  integer NOT NULL,
    dest_airport_id integer NOT NULL,
    route_codeshare       boolean,
    route_stops           integer,
    PRIMARY KEY (route_id),
    CONSTRAINT unq_route UNIQUE(airline_id,src_airport_id,dest_airport_id,route_codeshare,route_stops),
    FOREIGN KEY (src_airport_id) REFERENCES airports (airport_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (dest_airport_id) REFERENCES airports (airport_id) ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE TABLE route_plane (
    route_id    integer REFERENCES routes (route_id) ON UPDATE CASCADE ON DELETE CASCADE,
    plane_id    integer REFERENCES planes (plane_id) ON UPDATE CASCADE,
    CONSTRAINT route_plane_pkey PRIMARY KEY (route_id, plane_id)
);