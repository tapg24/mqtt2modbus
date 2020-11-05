import random
from time import sleep
import threading

from paho.mqtt import client as mqtt_client

from pymodbus.server.asynchronous import StartTcpServer

from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.payload import BinaryPayloadBuilder, Endian

# --------------------------------------------------------------------------- #
# configure the service logging
# --------------------------------------------------------------------------- #
# import logging
# FORMAT = ('%(asctime)-15s %(threadName)-15s'
#           ' %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
# logging.basicConfig(format=FORMAT)
# log = logging.getLogger()
# log.setLevel(logging.DEBUG)


broker = '31.31.202.123'
port = 1883
# topic = "#"
topic = "DarMal/#"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = "agrosensor"
password = "asmqttpas2"


assoc_map = {}


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        address = assoc_map.get(msg.topic)
        if address is not None:
            try:
                value_float = float(msg.payload.decode().split(',')[0])
                builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
                builder.add_32bit_float(value_float)
                payload = builder.to_registers()
                context = userdata['modbus_context']
                print('set', value_float)
                context[0].setValues(fx=3, address=address, values=payload)
            except Exception as ex:
                print('error')

    client.on_message = on_message
    return client.subscribe(topic)


def connect_mqtt(modbus_context) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            subscribe(client)
            (rc, mid) = subscribe(client)
            if rc == mqtt_client.MQTT_ERR_SUCCESS:
                print("Subscribed to topic", topic)
            else:
                print("Failed to subscribe to topic", topic)
        else:
            print("Failed to connect, return code %d\n", rc)

    client_userdata = {'modbus_context': modbus_context}
    client = mqtt_client.Client(client_id, userdata=client_userdata)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def run_mqtt_client(modbus_context) -> mqtt_client:
    client = connect_mqtt(modbus_context)
    # subscribe(client)
    client.loop_start()  # start thread for subscribe on_message
    return client


def read_config():
    with open('mqtt_modbus_mapping') as fp:
        for line in fp.readlines():
            parts = line.split(';')
            if len(parts) == 2:
                assoc_map[parts[0]] = int(parts[1])


if __name__ == '__main__':
    try:
        # read config
        read_config()
        # prepare modbus store
        store = ModbusSlaveContext(co=ModbusSequentialDataBlock(0, [0]*100))
        context = ModbusServerContext(slaves=store, single=True)

        # run mqtt client
        client = run_mqtt_client(context)

        # loop
        StartTcpServer(context, address=("localhost", 5020))
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    except Exception as ex:
        print(ex)
    finally:
        client.loop_stop()
