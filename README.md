# What's all this, then?

Skeeteater ingests mqtt messages and feeds them to a PostgreSQL
database.

It was inspired by [telegraf][], and follows the same model used by
telegraf's JSON data format of dividing message keys into "tags" and
"fields".  It creates tables as necessary on the fly from the
`mqtt_template` table, which looks something like:

    CREATE TABLE mqtt_template (
      measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      topic TEXT NOT NULL,
      tags JSONB,
      fields JSONB,
      UNIQUE (measured_at, topic)
    );

While this will work with a generic PostgreSQL installation, it was
designed for use with [TimescaleDB][], which offers optimizations for
storing and querying time series data.

[timescaledb]: https://www.timescale.com/
[telegraf]: https://www.influxdata.com/time-series-platform/telegraf/

## Configuration

You can use command line options or point `skeetereater` at a JSON
configuration file with the `--config` option.  Any command line
option can be placed in the config file. Replace `-` with `_`.  For
example:

    {
      "db_host": "localhost",
      "db_user": "postgres",
      "db_pass": "secret",
      "db_name": "sensors",

      "table_name_format": "sensor_{sensor_type}",

      "mqtt_host": "localhost",
      "topics": ["sensor/+/+"],
      "tag_keys": [
        "location",
        "sensor_id",
        "sensor_type"
      ]
    }

## Example queries

I'm collecting temperature and humidity data from a number of sensors
and using this tool to feed that data into a postgres database with
timescaledb.  I'm using [Grafana][] to visualize the data.  A
typically query in Grafana looks something like:

    SELECT
      time_bucket($__interval, measured_at) as time,
      tags->>'location' as metric,
      avg((fields->>'temperature')::float) as temperature
    FROM
      sensor_dht
    WHERE
      $__timeFilter(measured_at) and
      tags->>'location' in ($dht_locations) and
      tags->>'sensor_id' in ($dht_sensors)
    GROUP BY time, tags->>'location'

The Postgres documentation has more information about [querying JSON
data in Postgres][json].

[grafana]: https://grafana.com/
[json]: https://www.postgresql.org/docs/9.5/static/functions-json.html
