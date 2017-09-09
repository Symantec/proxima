# Proxima

```proxima -config /etc/proxima/proxima.yaml -ports 8086```

ports is a comma separated list of ports on which proxima listens.

# Proxima Config Files

## Example

```
databases:
- name: regular
  influxes:
  - hostAndPort: "localhost:8086"
    duration: 168h
    database: scotty
  - hostAndPort: "192.168.1.1:8086"
    duration: 8760h
    database: scotty
  scotties:
  - hostAndPort: "10.0.1.100:6980"
  - hostAndPort: "10.0.1.101:6980"
- name: just_scotty
  scotties:
  - hostAndPort: "10.0.1.100:6980"
  - hostAndPort: "10.0.1.101:6980"
```

## Explanation

"regular" and "just_scotty" are the database names that proxima recognize.
Remember that proxima speaks influxdb and influxdb requires a database name to
connect.

For the "regular" database, proxima tries to use the scotties at 10.0.1.100
and 10.0.1.101 for the most recent data. For data less than 1 week old, it
uses 192.168.1.1:8086 for data less than 1 year old, it uses localhost:8086.

