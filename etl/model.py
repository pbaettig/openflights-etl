
from dataclasses import dataclass
from typing import Iterable

@dataclass
class CityDat:
    city_id : int
    country_id : int
    country_name : str
    city_name : str
    city_timezone_offset_utc : float
    city_daylight_saving : str
    city_timezone : str

@dataclass
class CountryDat:
    country_name : str
    country_iso : str
    country_dafif : str

@dataclass
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

    @staticmethod
    def from_line(l):
        m = AirportDat._line_re.match(l.strip())
        if not m:
            raise ValueError("line does not match expected format")

        v = m.groups()

        ai = int(v[0])
        an = v[1]
        cy = v[2]
        ct = v[3]
        iata = v[4]
        icao = v[5]
        lat = float(v[6])
        lon = float(v[7])
        alt = int(v[8])
        tzo = float(v[9])
        dst = v[10]
        tzn = v[11]
        tp = v[12]
        src = v[13]


        return AirportDat(ai, an, cy, ct, iata, icao, lat, lon, alt, tzo, dst, tzn, tp, src)


@dataclass
class AirlineDat:
    airline_id : int
    airline_name : str
    airline_alias : str
    airline_iata : str
    airline_icao : str
    airline_callsign : str
    country : str
    airline_active : str

@dataclass(frozen=True, eq=True)
class PlaneDat:
    plane_name : str
    plane_iata : str
    plane_icao : str

@dataclass
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