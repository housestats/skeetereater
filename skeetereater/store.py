import datetime
import json
import logging
import psycopg2
import time

LOG = logging.getLogger(__name__)


class Store():

    default_template_table = 'mqtt_template'
    default_table_name_format = 'mqtt_misc_data'

    sql_table_exists = (
        'SELECT EXISTS (SELECT 1 FROM information_schema.tables '
        'WHERE table_schema = %(schema)s AND table_name = %(tablename)s)'
    )

    sql_clone_table = (
        'CREATE TABLE {tablename} ('
        'LIKE {template} INCLUDING ALL)'
    )

    sql_insert_sample = (
        'INSERT INTO {tablename} (topic, tags, fields) '
        'VALUES (%s, %s, %s)'
    )

    sql_insert_sample_time = (
        'INSERT INTO {tablename} (measured_at, topic, tags, fields) '
        'VALUES (%s, %s, %s, %s)'
    )

    def __init__(self, dsn,
                 template_table=None,
                 table_name_format=None):

        if template_table is None:
            template_table = self.default_template_table

        if table_name_format is None:
            table_name_format = self.default_table_name_format

        self.template_table = template_table
        self.table_name_format = table_name_format
        self.dsn = dsn

        self._connect()

    def _connect(self):
        LOG.debug('connecting to database')
        while True:
            try:
                self.conn = psycopg2.connect(self.dsn)
                break
            except psycopg2.OperationalError as err:
                LOG.error('failed to connecto to server: %s', err)
                time.sleep(1)

    def table_exists(self, tablename, schema='public'):
        cur = self.conn.cursor()
        cur.execute(self.sql_table_exists, dict(
            schema=schema, tablename=tablename))
        res = cur.fetchone()
        return res[0]

    def _create_mqtt_table(self, tablename):
        cur = self.conn.cursor()

        with self.conn:
            LOG.warning('create table %s from %s',
                        tablename, self.template_table)
            cur.execute(self.sql_clone_table.format(
                tablename=tablename,
                template=self.template_table))
            cur.execute("select create_hypertable(%(tablename)s, 'measured_at')",
                        dict(tablename=tablename))

    def _store_messages(self, messages):
        LOG.info('storing %d messages', len(messages))
        tables = {}

        for message in messages:
            measured_at, topic, tags, fields = message

            tablename = self.table_name_format.format(topic=topic, **tags)
            if tablename not in tables:
                tables[tablename] = []

            tables[tablename].append((measured_at,
                                      topic,
                                      json.dumps(tags),
                                      json.dumps(fields)))

        with self.conn:
            cur = self.conn.cursor()
            for tablename, values in tables.items():
                if not self.table_exists(tablename):
                    self._create_mqtt_table(tablename)

                sql = self.sql_insert_sample_time.format(tablename=tablename)
                psycopg2.extras.execute_batch(cur, sql, values, page_size=10)

    def store_messages(self, messages):
        while True:
            try:
                self._store_messages(messages)
                break
            except psycopg2.IntegrityError as err:
                LOG.error('integrity error (probably duplicate topic '
                          '+ timestamp): %s', err)
                break
            except psycopg2.Error as err:
                LOG.error('exception %s code %s', type(err), err.pgcode)
                if self.conn.closed:
                    LOG.error('lost postgres connection, reconnecting')
                    self._connect()
                else:
                    LOG.error('failed to store messages: %s', err)
                    return False

        return True
