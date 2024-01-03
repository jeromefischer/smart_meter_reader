# Database
db_ip   = '192.168.2.152'
db_port = '8086'
db_name = 'power_meter'

# Devices
device_ip = [
    'http://192.168.2.19',
    'http://192.168.2.59', 
    'http://192.168.2.63', 
    'http://192.168.2.109']
device_name = [
    'Heatpump',
    'Lüftung', 
    'Boiler', 
    'Heizband']
three_phase_devices = ['Heatpump',
                       'Boiler']
device_subpage = 'cm?cmnd=Status%208'
