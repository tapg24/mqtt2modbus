# from typing import Dict
import random
from time import sleep
import threading
import os
import signal
import sys
import configparser
import traceback
import json
import datetime
import struct

from paho.mqtt import client as mqtt_client

from pymodbus.server.asynchronous import StartTcpServer

from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSparseDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.payload import BinaryPayloadBuilder, Endian
from pymodbus.pdu import ModbusRequest, ModbusResponse, ModbusExceptions
from pymodbus.compat import int2byte, byte2int

from twisted.internet import task

# --------------------------------------------------------------------------- #
# configure the service logging
# --------------------------------------------------------------------------- #
import logging
FORMAT = ('%(asctime)-15s %(threadName)-15s'
          ' %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
# log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


map_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'map.json')
conf_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')


class MQTTChannel(object):

    def __init__(self, conf_path, map_path):
        self.conf_path = conf_path
        self.config = self.__parse_config__(self.conf_path)
        self.map_path = map_path
        self.map = self.__parse_map__(self.map_path)
        self.modbus_context = None
        self.client = None

    def __parse_config__(self, conf_path) -> dict:
        log.info('parse configuration')
        with open(conf_path) as config_file:
            config = json.load(config_file)
            return config

    def __parse_map__(self, map_path) -> dict:
        log.info('parse map')
        with open(map_path) as map_file:
            mapping = json.load(map_file)
            mapping['topic2address'] = {}
            mapping['address2param'] = {}
            for device_name, device_prop in mapping['devices'].items():
                for param_name, param_prop in device_prop['params'].items():
                    topic = '%s/%s' % (device_name, param_name)
                    mapping['topic2address'][topic] = param_prop['modbus_offset']
                    mapping['address2param'][param_prop['modbus_offset']] = param_prop
            return mapping

    def __validate_map__(self, mapping) -> bool:
        return True

    def __copy_payload_map__(self, src, dst) -> None:
        for src_device_name, src_device_prop in src['devices'].items():
            dst_device = dst['devices'].get(src_device_name)
            if dst_device:
                dst_params = dst_device.get('params')
                for src_param_name, src_param_prop in src_device_prop['params'].items():
                    dst_param_prop = dst_params.get(src_param_name)
                    if dst_param_prop:
                        src_value = src_param_prop.get('value')
                        dst_param_prop['value'] = src_value
                        dst_param_prop['updated_at'] = src_param_prop.get('updated_at')
                        dst_param_prop['alive'] = src_param_prop.get('alive')
                        if dst_param_prop['modbus_offset'] != src_param_prop['modbus_offset']:
                            if src_value is not None:
                                value_float = float(src_value)
                                builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
                                builder.add_32bit_float(value_float)
                                payload = builder.to_registers()
                                self.modbus_context[0].setValues(fx=3, address=dst_param_prop['modbus_offset'], values=payload)

    def load_map(self):
        mapping = self.__parse_map__(self.map_path)
        if self.__validate_map__(mapping):
            self.__copy_payload_map__(self.map, mapping)
            self.map = mapping
        else:
            raise Exception('New map not loaded')

    def __subscribe__(self, topic):
        # run in thread
        def on_message(client, userdata, msg):
            log.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
            address = self.map['topic2address'].get(msg.topic)
            if address is not None:
                try:
                    value_float = float(msg.payload.decode().split(',')[0])
                    builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
                    builder.add_32bit_float(value_float)
                    payload = builder.to_registers()
                    self.modbus_context[0].setValues(fx=3, address=address, values=payload)

                    device_name, param_name = tuple(msg.topic.rsplit('/', 1))
                    device = self.map['devices'].get(device_name)
                    if device:
                        param = device['params'].get(param_name)
                        if param:
                            param['value'] = value_float
                            param['updated_at'] = datetime.datetime.now()
                            param['alive'] = True

                except Exception as ex:
                    log.exception(ex)

        assert self.client
        self.client.on_message = on_message
        return self.client.subscribe(topic)

    def __connect__(self, broker_config) -> mqtt_client:
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                log.info("Connected to MQTT Broker - %s:%s" % (broker_config['address'], broker_config['port']))
                (rc, mid) = self.__subscribe__(broker_config['topic'])
                if rc == mqtt_client.MQTT_ERR_SUCCESS:
                    log.info("Subscribed to topic %s" % broker_config['topic'])
                else:
                    log.info("Failed to subscribe to topic %s" % broker_config['topic'])
            else:
                log.info("Failed to connect, return code %s" % str(rc))

        assert self.modbus_context
        client_userdata = {'modbus_context': self.modbus_context}
        self.client = mqtt_client.Client(broker_config['client_id'], userdata=client_userdata)
        self.client.username_pw_set(broker_config['username'], broker_config['password'])
        self.client.on_connect = on_connect
        self.client.connect(broker_config['address'], broker_config['port'])

    def check_alive(self):
        log.info('start check alive all devices...')
        for device_name, device_prop in self.map['devices'].items():
            device_alive = True
            for param_prop in device_prop.values():
                updated_at = param_prop.get('updated_at')
                if updated_at:
                    minutes_diff = (datetime.datetime.now() - updated_at).total_seconds() / 60.0
                    if minutes_diff > 2:
                        param_prop['alive'] = False
                        device_alive = False
                else:
                    param_prop['alive'] = False
                    device_alive = False
            if device_alive:
                log.info('device %s is alive' % device_name)
            else:
                log.info('device %s is dead' % device_name)
        log.info('end check alive all devices...')

    def is_param_alive(self, address, count):
        comp_address = address
        if address % 2 != 0:
            comp_address = address - 1
        param = self.map['address2param'].get(comp_address)
        if param is not None:
            alive = param.get('alive', None)
            if alive is None:
                return False
        return True

    def start_with_context(self, modbus_context: ModbusSlaveContext):
        self.modbus_context = modbus_context
        self.__connect__(self.config['mqtt_broker'][0])
        self.client.loop_start()  # start thread for subscribe on_message

    def stop(self):
        assert self.client
        self.client.loop_stop()


class MQTTDataBlock(ModbusSparseDataBlock):
    """ A datablock that stores the new value in memory
    and performs a custom action after it has been stored.
    """

    def __init__(self, mqtt_channel):
        self.mqtt_channel = mqtt_channel

        values = {i: 0 for i in range(100)}
        super(MQTTDataBlock, self).__init__(values)

    def validate(self, address, count):
        # log.info("validate address {} count {}".format(address, count))
        if self.mqtt_channel.is_param_alive(address, count):
            return super(MQTTDataBlock, self).validate(address, count)
        else:
            return False

    def setValues(self, address, value):
        """ Sets the requested values of the datastore

        :param address: The starting address
        :param values: The new values to be set
        """
        super(MQTTDataBlock, self).setValues(address, value)

        # whatever you want to do with the written value is done here,
        # however make sure not to do too much work here or it will
        # block the server, espectially if the server is being written
        # to very quickly
        # log.info("wrote {} to {}".format(value, address))

    def getValues(self, address, count=1):
        # log.info("read {} count {}".format(address, count))
        return super(MQTTDataBlock, self).getValues(address, count)


def initialize_signal_handlers(sighup_handler):
    def handle_sighup(signum, frame):
        log.info("received SIGHUP")
        log.info('init new configuration')
        sighup_handler()

    # def handle_sigterm(signal, frame):
        # print("received SIGTERM", flush=True)

    # def handle_sigint(signal, frame):
        # print("received SIGINT", flush=True)

    log.info("initialize_signal_handlers")
    # signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGHUP, handle_sighup)
    # signal.signal(signal.SIGINT, handle_sigint)


# custom request/response for custom modbus exception code
# https://github.com/riptideio/pymodbus/blob/dev/examples/common/custom_synchronous_server.py

if __name__ == '__main__':
    try:
        mqtt_channel = MQTTChannel(conf_filename, map_filename)
        block = MQTTDataBlock(mqtt_channel)
        store = ModbusSlaveContext(hr=block)
        context = ModbusServerContext(slaves=store, single=True)
        mqtt_channel.start_with_context(context)

        initialize_signal_handlers(sighup_handler=mqtt_channel.load_map)

        l = task.LoopingCall(mqtt_channel.check_alive)
        loopDeferred = l.start(60)

        # loop
        StartTcpServer(context, address=("localhost", 5020))
    except KeyboardInterrupt:
        log.info('KeyboardInterrupt')
    except Exception as ex:
        traceback.print_exc()
    finally:
        mqtt_channel.stop()
