#!/usr/bin/env python3

import paho.mqtt.client as mqtt

import sys
import json
import yaml
import bottle
import threading

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(name)s %(message)s')
logger = logging.getLogger('mitsubishi-exporter')

class MQTTclient(threading.Thread):
    def __init__(self):
        super().__init__()
        self._stopflag = threading.Event()
        self._pumps = {}

    def _mq_on_connect(self, client, userdata, flags, result_code):
        logger.info('MQTT connected with result %s' % result_code)
        client.subscribe('mitsubishi/#')

    def _mq_on_message(self, client, userdata, raw):
        topic = raw.topic
        payload = raw.payload.decode()
        parts = topic.split('/')
        data = json.loads(payload)

        if len(parts) == 3 and parts[2] == 'state':
            pump = parts[1]
            self._pumps[pump] = {'temperature_set': data['temperature'], 'temperature_room': data['roomTemperature']}


    def run(self):
        logger.info('Starting MQTT thread')
        self._mq = mqtt.Client()
        self._mq.on_connect = self._mq_on_connect
        self._mq.on_message = self._mq_on_message
        while 1:
            logger.info('Connecting to MQTT broker')
            try:
                self._mq.connect('127.0.0.1', 1883, 10)
                logger.info('Connected to MQTT broker')
                break
            except TimeoutError:
                logger.error('MQTT connection failed')
                time.sleep(10)
        self._mq.loop_forever()

    def stop(self):
        logger.info('Stopping MQTT thread')
        self._mq.disconnect()
        self.join()

    def get_pumps(self):
        return self._pumps

mq = MQTTclient()
@bottle.route('/metrics')
def bottle_metrics():
    output = []
    for pump, data in mq.get_pumps().items():
        for metric, value in data.items():
            output.append('mitsubishi_{}{{pump="{}"}} {}'.format(metric, pump, value))

    text = '\n'.join(output)
    return bottle.HTTPResponse(text, headers={'Content-Type': 'text/plain'})

def main():
    try:
        mq.start()
        bottle.run(host='0.0.0.0', port=9350)

    finally:
        mq.stop()




if __name__ == '__main__':
    main()
