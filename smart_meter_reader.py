
#! /usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module for reading Tasmota Energy Meters and send data as json to influxDB."""

import logging
from logging.handlers import RotatingFileHandler
import datetime
import requests
from influxdb import InfluxDBClient
import config

logging.basicConfig(handlers=[RotatingFileHandler('./smart_meter_reader/log/energy_meter.log',
                                                  maxBytes=100000, backupCount=10)],
                    level=logging.DEBUG,
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


def read_measurement_from_powermeter(ip_address, device_name):
    # http://192.168.2.109/cm?cmnd=Status%208
    r = requests.get(url=f"{ip_address}/{config.device_subpage}", timeout=3)
    data = r.json()

    # print(data['StatusSNS']['ENERGY']['Yesterday'])
    # print(data['StatusSNS']['ENERGY']['ApparentPower'])

    data = data['StatusSNS']['ENERGY']

    # Remove all unused fields and sum the power, if it is a three phase measurement.
    if "Power" in data:
        if isinstance(data["Power"], list):
            if device_name in config.three_phase_devices:
                for phase, power in enumerate(data["Power"]):
                    name = "Power_P" + str(phase+1)
                    data[name] = power
            data["Power"] = sum(map(int, data["Power"]))
    if "Factor" in data:
        data.pop("Factor", None)
    if "Yesterday" in data:
        data.pop("Yesterday", None)
    if "ReactivePower" in data:
        data.pop("ReactivePower", None)
    if "ApparentPower" in data:
        data.pop("ApparentPower", None)
    if "Frequency" in data:
        data.pop("Frequency", None)
    if "Voltage" in data:
        data.pop("Voltage", None)
    if "Current" in data:
        data.pop("Current", None)
    return data

def generate_json(data_as_json, device_name):

    _json_body = [
        {
            "measurement": "power_meter",
            "location": "Neuendorf",
            "tags": {
                "host": device_name,
            },
            "time": datetime.datetime.utcnow().isoformat(),
            "fields": data_as_json
        }
    ]
    return _json_body

def write_to_influx(json_body):
    # print(json_body)
    # InfluxDB Configuration
    client = InfluxDBClient(host=config.db_ip, port=config.db_port, database=config.db_name)
    # print(client.get_list_database())
    # client.drop_database(dbname=config.db_name)
    # client.create_database(dbname=config.db_name)
    return client.write_points(points=json_body,
                               time_precision='s',
                               database=config.db_name,
                               protocol='json')



try:
    for num, ip in enumerate(config.device_ip):
        data_as_json = read_measurement_from_powermeter(ip_address=ip, device_name=config.device_name[num])
        logging.info("Device Name: %s\t data: %s", config.device_name[num], data_as_json)
        json_body = generate_json(data_as_json=data_as_json, device_name=config.device_name[num])
        success = write_to_influx(json_body=json_body)
        logging.info('successfully written to db: %s', success)
except Exception as e:
    logging.exception("Exception occured: %s", e.with_traceback)
    
