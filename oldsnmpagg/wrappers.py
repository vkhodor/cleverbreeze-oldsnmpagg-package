import socket
import json
import csv
from snmpagg.utils import lower_first_char
import datetime
import dateutil.parser

REGISTER_TYPES = {
    'coil': 'Coil',
    'discrete': 'Discrete',
    'input': 'Input',
    'holding_reg': 'Holding Reg'
}

RO_REGISTERS = ['input', 'discrete']
RW_REGISTERS = ['coil', 'holding_reg']

CONNECT_TEST_DELAY = 1

class Controller:
    """ Обертка для данных из таблицы controllers"""
    root_oid = ''
    ip_address = ''
    oid = 0
    oid_name = ''
    tcp_port = 0
    description = ''
    connect = None

    def __init__(self,
                 root_oid='',
                 ip_address='',
                 oid=0,
                 oid_name='',
                 tcp_port=0,
                 description='',
                 ):
        self.root_oid = root_oid
        self.ip_address = ip_address
        self.oid = oid
        self.oid_name = lower_first_char(oid_name)
        self.tcp_port = tcp_port
        self.description = description

    def __str__(self):
        return '<Controller>:{0}:{1}:{2}:{3}:{4}:{5}'.format(
            self.root_oid,
            self.ip_address,
            self.oid,
            self.oid_name,
            self.tcp_port,
            self.description
        )

    def __gt__(self, other):
        lst_ip_addresses = sorted(set([self.ip_address, other.ip_address]))
        if len(lst_ip_addresses) == 2 and self.ip_address == lst_ip_addresses[1]:
            return True
        return False

    def __eq__(self, other):
        if self.ip_address == other.ip_address and self.tcp_port == other.tcp_port:
            return True
        return False

    def set_connect(self, func=None, logger=None):
        con = func(addr=self.ip_address, port=self.tcp_port, logger=logger)
        if not con.connect_state:
            self.connect = None
            return False
        self.connect = con

    def connection_is_ok(self):
        """
        Testing connection
        True - if connected
        Flase - if not
        """

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(CONNECT_TEST_DELAY)

        try:
            sock.connect((self.ip_address, self.tcp_port))
            sock.close()
            return True
        except:
            return False


class Sensor:
    """Обертка для данных из таблицы sensors"""
    controller_ip = ''
    oid = 0
    oid_name = ''
    modbus_id = 0
    data_type = 0
    description = ''
    register_type = 'input'
    register_type_name = REGISTER_TYPES['input']
    monitoring = False
    controller = None
    min_value = None
    max_value = None
    value = None

    def __init__(self,
                 controller_root_oid='',
                 controller_ip_address='',
                 controller_oid=0,
                 controller_oid_name='',
                 controller_tcp_port=0,
                 controller_description='',
                 controller_ip='',
                 oid=0,
                 oid_name='',
                 modbus_id=0,
                 data_type=0,
                 description='',
                 register_type='input',
                 monitoring=False,
                 min_value='',
                 max_value='',
                 value=''
                 ):
        self.controller_ip = controller_ip
        self.oid = oid
        self.oid_name = lower_first_char(oid_name)
        self.modbus_id = modbus_id
        self.data_type = data_type
        self.description = description

        self.controller = Controller(
            controller_root_oid, controller_ip_address,
            controller_oid, controller_oid_name,
            controller_tcp_port, controller_description
        )

        self.set_register_type(register_type)
        self.set_monitoring(monitoring)
        self.set_min_value(min_value)
        self.set_max_value(max_value)
        self.value = value

    def __str__(self):
        return '<Sensor>:{0}:{1}:{2}:{3}:{4}:{5}:({6}={7}):{8}'.format(
            self.controller_ip,
            self.oid,
            self.oid_name,
            self.modbus_id,
            self.data_type,
            self.description,
            self.register_type,
            self.register_type_name,
            self.monitoring
        )

    def __gt__(self, other):
        lst = sorted([
            '{0}_{1}'.format(self.controller_ip, self.oid),
            '{0}_{1}'.format(other.controller_ip, other.oid)
        ])
        if len(lst) == 2 and '{0}_{1}'.format(self.controller_ip, self.oid) == lst[1]:
            return True
        return False

    def get_snmp_data_type(self):
        dict_types = {
            'int': 'integer',
            'integer': 'integer',
            'real': 'string'
        }
        try:
            return dict_types[self.data_type]
        except:
            return self.data_type

    def set_register_type(self, register_type):
        self.register_type = register_type
        try:
            self.register_type_name = REGISTER_TYPES[register_type]
        except KeyError:
            self.register_type_name = 'UnknownType'

    def full_oid(self):
        return '{0}.{1}.{2}'.format(self.controller.root_oid, self.controller.oid, self.oid)

    def set_monitoring(self, monitoring):
        if self.register_type in ['coil', 'holding_reg']:
            monitoring = 0

        self.monitoring = bool(monitoring)

    def get_monitoring_checkbox_value(self):
        if self.monitoring:
            return 'checked'
        return ''

    def __set_value(self, value):
        if value is None:
            return ''

        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return ''

    def set_min_value(self, min_value):
        self.min_value = self.__set_value(min_value)

    def set_max_value(self, max_value):
        self.max_value = self.__set_value(max_value)

    def __get_value(self, value):
        try:
            return str(value)
        except ValueError:
            return ''

    def get_min_value(self):
        return self.__get_value(self.min_value)

    def get_max_value(self):
        return self.__get_value(self.max_value)

    @staticmethod
    def from_csv(csv_string):
        lst_fields = csv_string.split(';')
        if len(lst_fields) != 10:
            return None

        sensor = Sensor(
                    controller_ip=lst_fields[0],
                    oid=lst_fields[1],
                    oid_name=lst_fields[2],
                    modbus_id=lst_fields[3],
                    data_type=lst_fields[4],
                    register_type=lst_fields[5],
                    min_value=lst_fields[6],
                    max_value=lst_fields[7],
                    description=lst_fields[8],
                    monitoring=lst_fields[9]
                )
        return sensor

    def to_csv(self):
        return '{0};{1};{2};{3};{4};{5};{6};{7};{8};{9}'.format(
                    self.controller_ip,
                    self.oid,
                    self.oid_name,
                    self.modbus_id,
                    self.data_type,
                    self.register_type,
                    self.min_value,
                    self.max_value,
                    self.description,
                    self.monitoring
                )


class Data:
    """Обертка для данных полученных с сенсора (таблица data_history"""
    id = 0
    date_time = None  # object Data
    sensor = None     # object Sensor
    value = None

    def __init__(self, sensor, value, date_time, item_id=0):
        self.id = item_id
        self.sensor = sensor
        self.value = value

        if type(date_time) == str:
            self.date_time = dateutil.parser.parse(date_time)
        elif type(date_time) == float:
            self.date_time = datetime.datetime.fromtimestamp(date_time)
        else:
            self.date_time = date_time

    def __str__(self):
        return '{0}:{1}:{2}:{3}'.format(self.id, self.sensor, self.value, self.date_time)

    def get_date_time_iso8601(self):
        return self.date_time.isoformat()

    def date_time_as_unixtimestap(self):
        return self.date_time.timestamp()

    def as_dict(self):
        return {
            'id': self.id,
            'value': self.value,
            'date_time': self.get_date_time_iso8601(),
            'controller_ip': self.sensor.controller_ip,
            'modbus_id': self.sensor.modbus_id,
            'oid': self.sensor.oid
        }

    def as_json(self):
        return json.dumps(self.as_dict())

    def as_csv(self):
        return '{0}, {1}, {2}, {3}\n'.format(
            self.get_date_time_iso8601(),
            self.sensor.controller_ip,
            self.sensor.modbus_id,
            self.value
        )