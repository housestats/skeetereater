import datetime
import json
import logging
import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
import threading

LOG = logging.getLogger(__name__)


class Periodic(threading.Thread):

    def __init__(self, interval, func, args=None, kwargs=None):
        super().__init__(daemon=True)

        self._interval = interval
        self._func = func
        self._args = args if args else []
        self._kwargs = kwargs if kwargs else {}

    def run(self):
        self._create_timer()

    def cancel(self):
        self._timer.cancel()

    def _create_timer(self):
        self._timer = threading.Timer(self._interval, self._alarm)
        self._timer.start()

    def _alarm(self):
        self._create_timer()
        self._func(*self._args, **self._kwargs)


class Collect():

    default_mqtt_host = 'localhost'
    default_mqtt_port = 1883
    default_flushinterval = 1
    default_flushsize = 100
    default_tag_keys = ['topic']

    def __init__(self,
                 mqtt_host=None,
                 mqtt_port=None,
                 clientid=None,
                 flushinterval=None,
                 flushsize=None,
                 topics=None,
                 tag_keys=None,
                 flushfunc=None):

        if mqtt_host is None:
            mqtt_host = self.default_mqtt_host

        if mqtt_port is None:
            mqtt_port = self.default_mqtt_port

        if flushinterval is None:
            flushinterval = self.default_flushinterval

        if flushsize is None:
            flushsize = self.default_flushsize

        if tag_keys is None:
            tag_keys = self.default_tag_keys

        self.topics = topics
        self.flushinterval = flushinterval
        self.flushsize = flushsize
        self.flushfunc = flushfunc
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.tag_keys = set(tag_keys)

        self.buffer = []
        self.buflen = 0
        self.flusher = None
        self.lock = threading.Lock()

        self.init_broker()

    def init_broker(self):
        self.broker = mqtt.Client()
        self.broker.on_connect = self.on_connect
        self.broker.on_message = self.on_message

    def start(self):
        LOG.debug('connecting to broker')
        self.broker.connect(self.mqtt_host, self.mqtt_port)
        self.broker.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        LOG.info('connected to mqtt broker')
        for t in self.topics:
            LOG.debug('subscribing to %s', t)
            self.broker.subscribe(t)

        LOG.debug('starting flush thread')
        self.flusher = Periodic(self.flushinterval, self.flush)
        self.flusher.start()

    def on_disconnect(self, client, userdata, rc):
        LOG.info('disconnected from mqtt broker')
        self.flusher.cancel()
        self.flush()

    def prepare_message(self, message):
        LOG.debug('preparing message with topic %s', message.topic)

        topic = message.topic
        data = json.loads(message.payload.decode('utf-8'))
        time_ = data.pop('__time__', datetime.datetime.utcnow().isoformat())

        tags = {k: v for k, v in data.items()
                if k in self.tag_keys}
        fields = {k: v for k, v in data.items()
                  if k not in self.tag_keys}

        return (time_, topic, tags, fields)

    def on_message(self, broker, userdata, msg):
        LOG.debug('received message on topic %s', msg.topic)
        with self.lock:
            self.buffer.append(self.prepare_message(msg))
            self.buflen += 1

        if self.buflen > self.flushsize:
            self.flush()

    def flush(self):
        with self.lock:
            count = len(self.buffer)
            LOG.debug('flushing %d messages', count)
            if count:
                if self.flushfunc(self.buffer):
                    self.buffer = []
                    self.buflen = 0
