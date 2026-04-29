"""
Austin first-principles demand generation model.

Generates synthetic trip parquets from Census LODES/ACS, OSMnx POIs,
Capital Metro GTFS, OSRM travel times, and NHTS temporal profiles.
Output schema matches the sim engine: (request_time_seconds, origin_h3, destination_h3).
"""
