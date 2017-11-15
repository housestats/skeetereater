import argparse
import json
import logging
import os

from skeetereater.collect import Collect
from skeetereater.store import Store


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--config', '-f')

    g = p.add_argument_group('Database options')
    g.add_argument('--db-host',
                   default=os.environ.get('SKEETER_DB_HOST'))
    g.add_argument('--db-port',
                   type=int,
                   default=os.environ.get('SKEETER_DB_PORT'))
    g.add_argument('--db-user',
                   default=os.environ.get('SKEETER_DB_USER'))
    g.add_argument('--db-pass',
                   default=os.environ.get('SKEETER_DB_PASS'))
    g.add_argument('--db-name',
                   default=os.environ.get('SKEETER_DB_NAME'))
    g.add_argument('--template-table',
                   default=os.environ.get('SKEEPTER_TEMPLATE_TABLE',
                                          'mqtt_template')),
    g.add_argument('--table-name-format',
                   default=os.environ.get('SKEETER_TABLE_NAME_FORMAT'))

    g = p.add_argument_group('MQTT options')
    g.add_argument('--mqtt-host',
                   default=os.environ.get('SKEETER_MQTT_HOST'))
    g.add_argument('--mqtt-port',
                   type=int,
                   default=os.environ.get('SKEETER_MQTT_PORT'))
    g.add_argument('--mqtt-client-id',
                   default=os.environ.get('SKEETER_MQTT_CLIENT_ID'))
    g.add_argument('--flush-interval',
                   type=float,
                   default=os.environ.get('SKEETER_FLUSH_INTERVAL'))
    g.add_argument('--flush-size',
                   type=int,
                   default=os.environ.get('SKEETER_FLUSH_SIZE'))
    g.add_argument('--topic', '-t',
                   action='append')
    p.add_argument('--tag-key', '-k',
                   action='append')

    g = p.add_argument_group('Logging options')
    g.add_argument('--verbose', '-v',
                   action='store_const',
                   const='INFO',
                   dest='loglevel')
    g.add_argument('--debug', '-d',
                   action='store_const',
                   const='DEBUG',
                   dest='loglevel')

    p.set_defaults(loglevel='WARNING')

    return p.parse_args()


def make_pg_dsn(args):
    args_to_dsn = {
        'db_name': 'dbname',
        'db_host': 'host',
        'db_port': 'port',
        'db_user': 'user',
        'db_pass': 'password',
    }

    return ' '.join('{}={}'.format(v, getattr(args, k))
                    for k, v in args_to_dsn.items()
                    if getattr(args, k) is not None)


def main():
    args = parse_args()

    config = vars(args)
    if args.config:
        with open(args.config) as fd:
            config_from_file = json.load(fd)

        config.update({k: v for k, v in config_from_file.items()
                       if config[k] is None})

    logging.basicConfig(level=args.loglevel)

    store = Store(make_pg_dsn(args),
                  template_table=args.template_table,
                  table_name_format=args.table_name_format)

    collect = Collect(
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        clientid=args.mqtt_client_id,
        topics=args.topic,
        flushfunc=store.store_messages,
        tag_keys=args.tag_key,
    )

    collect.start()
