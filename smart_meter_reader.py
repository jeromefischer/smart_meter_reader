#! /usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import config
import requests
from influxdb import InfluxDBClient



def read_measurement_from_powermeter(ip_address, subpage=config.device_subpage):
    # http://192.168.2.109/cm?cmnd=Status%208
    r = requests.get(f"{ip_address}/{subpage}")
    data = r.json()

    # print(data['StatusSNS']['ENERGY']['Yesterday'])
    # print(data['StatusSNS']['ENERGY']['ApparentPower'])

    data = data['StatusSNS']['ENERGY']
    # print(data)
    return data
  
def generate_json(data_as_json, device_name):

    json_body = [
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
    return json_body

def write_to_influx(json_body):
    print(json_body)

    # InfluxDB Configuration
    client = InfluxDBClient(host=config.db_ip, port=config.db_port, database=config.db_name)
    # print(client.get_list_database())
    # client.drop_database(dbname=config.db_name)
    # client.create_database(dbname=config.db_name)
    success = client.write_points(points=json_body, time_precision='s', database=config.db_name, protocol=u'json')

    return success

for num, ip in enumerate(config.device_ip): 
    data_as_json = read_measurement_from_powermeter(ip_address=ip)
    json_body = generate_json(data_as_json=data_as_json, device_name=config.device_name[num])
    success = write_to_influx(json_body=json_body)
    print('successfully written to db: {}'.format(success))

