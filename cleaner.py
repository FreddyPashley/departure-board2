import json

with open("airports.json") as f:
    airports = json.load(f)
with open("flights.json") as f:
    slots = json.load(f)[2]["data"]

ports = []

for slot in slots:
    origin = slot["origin_icao"]
    dest = slot["destination_icao"]
    if origin is not None and origin not in ports:
        ports.append(origin)
    if dest is not None and dest not in ports:
        ports.append(dest)

ports = sorted(ports)
for port in ports:
    print(port, airports[port]["name"])