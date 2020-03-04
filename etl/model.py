
from dataclasses import dataclass
from typing import Iterable

@dataclass(frozen=True)
class CityDat:
    city_id : int
    country_id : int
    country_name : str
    city_name : str
    city_timezone_offset_utc : float
    city_daylight_saving : str
    city_timezone : str

@dataclass(frozen=True)
class CountryDat:
    country_name : str
    country_iso : str
    country_dafif : str

@dataclass(frozen=True)
class AirportDat:
    airport_id : int
    airport_name : str
    city : str
    country : str
    airport_iata : str
    airport_icao : str
    latitude : float
    longitude : float
    altitude_ft : int
    timezone_offset : float
    daylight_saving : str
    timezone : str
    type : str
    source : str


@dataclass(frozen=True)
class AirlineDat:
    airline_id : int
    airline_name : str
    airline_alias : str
    airline_iata : str
    airline_icao : str
    airline_callsign : str
    country : str
    airline_active : str


@dataclass(frozen=True)
class PlaneDat:
    plane_name : str
    plane_iata : str
    plane_icao : str

    def __eq__(self, other):
        return self.plane_iata == other.plane_iata and self.plane_icao == other.plane_icao


@dataclass(frozen=True)
class RouteDat:
    airline_iata : str
    airline_icao : str

    airline_id : str
    
    src_airport_iata : str
    src_airport_icao : str
    src_airport_id : str

    dest_airport_iata : str
    dest_airport_icao : str
    dest_airport_id : str
    
    route_codeshare : str
    route_stops : int
    route_equipment_iata : Iterable[str]