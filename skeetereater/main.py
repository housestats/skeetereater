import argparse
import json
import logging

from skeetereater.collect import Collect
from skeetereater.store import Store


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--config', '-f')

    g = p.add_argument_group('Database options')
    g.add_argument('--db-host')
    g.add_argument('--db-port',
                   type=int)
    g.add_argument('--db-user')
    g.add_argument('--db-pass')
    g.add_argument('--db-name')
    g.add_argument('--template-table',
                   default='mqtt_template')
    g.add_argument('--table-name-format')

    g = p.add_argument_group('MQTT options')
    g.add_argument('--mqtt-host')
    g.add_argument('--mqtt-port',
                   type=int)
    g.add_argument('--mqtt-client-id')
    g.add_argument('--flush-interval',
                   type=float)
    g.add_argument('--flush-size',
                   type=int)
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
