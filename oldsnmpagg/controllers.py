# -*- coding: utf-8 -*-
# Project: snmpagg
# File: controllers.py
# Author: Victor V. Khodorchenko
# Mail: hvv@nsu.ru
# Year: 2017

import sqlite3
from oldsnmpagg.wrappers import Controller, Sensor, Data

DEFAULT_ROOT_OID = '.1.3.6.1.4.1.49118.121'  # 121 взят с потолка
SQL_WILDCARD = '%'

ADD_SENSOR_ERRORS = {
    1: 'OK',
    0: 'ошибка внесения записи(Exception)',
    -1: 'oid занят',
    -2: 'modbus_id занят',
    -3: 'oid_name занят'

}


class Controllers(object):
    """Класс для работы с БД, в которой храниться конфигурация контроллеров"""
    logger = None

    def __init__(self, db_file, logger = None, read_only=False):
        """Если файла нет или он пустой, то создаем базу заднных"""

        self.logger = logger
        sql_foreign_on = 'PRAGMA foreign_keys = ON'

        sql_create_root_oids = '''
        CREATE TABLE IF NOT EXISTS root_oids (
            oid TEXT UNIQUE PRIMARY KEY -- корневые OIDы
        )
        '''

        sql_create_controllers = '''
        CREATE TABLE IF NOT EXISTS controllers (
            root_oid TEXT,  -- корневой OID
            ip_address TEXT UNIQUE PRIMARY KEY ASC,  -- ip адрес контроллера
            oid INTEGER UNIQUE,  -- числовой OID без корневой части
            oid_name TEXT UNIQUE, -- символьный OID
            tcp_port INTEGER,  -- порт по которому работает Modbus
            description TEXT,  -- справочная информация
            FOREIGN KEY (root_oid) REFERENCES  root_oids(oid) ON UPDATE CASCADE 
        )
        '''

        sql_create_sensors = '''
        CREATE TABLE IF NOT EXISTS sensors (
            controller_ip TEXT,  -- ip адрес контроллера
            oid INTEGER,  -- числовой OID сенсора
            oid_name, -- символьный OID сенсора
            modbus_id INTEGER,  -- id сенсора на шине Modbus
            data_type TEXT, -- тип данных, которые возвращает сенсор (real, int)
            description TEXT,  -- справочная информация
            register_type TEXT DEFAULT \'input\', -- тип регистра input, coil, discrete, holding_reg
            monitoring INTEGER DEFAULT 0, -- boolean мониторить или нет.
            min_value TEXT DEFAULT \'\',
            max_value TEXT DEFAULT \'\',
            value TEXT DEFAULT \'\',
            FOREIGN KEY (controller_ip)  REFERENCES controllers(ip_address) ON DELETE CASCADE ON UPDATE CASCADE
        )
        '''

        self.__sqlite = sqlite3.connect(db_file)
        self.__cursor = self.__sqlite.cursor()
        self.__cursor.execute(sql_foreign_on)
        if read_only:
            self.disable_changes()
            return
        self.__cursor.execute(sql_create_root_oids)
        self.__cursor.execute(sql_create_controllers)
        self.__cursor.execute(sql_create_sensors)

        try:
            self.__cursor.execute('INSERT INTO root_oids (oid) VALUES ("{0}")'.format(DEFAULT_ROOT_OID))
        except:
            pass

    def __del__(self):
        self.__sqlite.commit()
        self.__sqlite.close()

    def disable_changes(self):
        self.__cursor.execute('PRAGMA query_only = ON')

    def enable_changes(self):
        self.__cursor.execute('PRAGMA query_only = OFF')

    def add_controller(self, controller):
        """
        Добавляет контроллер
        True - OK
        False - error
        """

        try:
            self.__cursor.execute(
                '''INSERT INTO controllers 
                        (root_oid, ip_address, oid, oid_name, tcp_port, description) 
                   VALUES
                        ("{0}", "{1}", {2}, "{3}", {4}, "{5}")
                        '''.format(
                                    controller.root_oid,
                                    controller.ip_address,
                                    controller.oid,
                                    controller.oid_name,
                                    controller.tcp_port,
                                    controller.description
                            )
            )
            self.__debug('Добавили контроллер с ip = {0}'.format(controller.ip_address))
            self.__sqlite.commit()
            return True

        except Exception as e:
            self.__debug('Ошибка добавления контроллера!')
            self.__debug(controller)
            self.__debug(str(e))
            return False

    def update_controller(self, controller):
        """
        Обновляет данные контроллера
        :param controller_ip:
        :param st_values:
        :return:
        """
        st_values = {
            'oid': controller.oid,
            'oid_name': controller.oid_name,
            'tcp_port': controller.tcp_port,
            'description': controller.description
        }

        ret = self.__update_row('controllers', 'ip_address = "{0}"'.format(controller.ip_address), st_values)
        return ret

    def __update_row(self, table_name, condition, st_values):
        """
        Обновляем данные в таблице по условию

        :param (str)table_name:
        :param: (str)condition
        :param st_values: {ip_address: '1.1.1.1', oid: 1...}
        :return:
        """

        result = self.__cursor.execute('SELECT * FROM {0} WHERE {1}'.format(table_name, condition))
        if len(result.fetchall()) == 0:
            self.__debug('Ошибка! Такой записи нет!')
            return False

        row_update = ''
        for key, v in st_values.items():
            if type(v) == int:
                row_update = row_update + '{0} = {1}, '.format(key, v)
            else:
                row_update = row_update + '{0} = "{1}", '.format(key, v)

        query_string = '''
        UPDATE {0}
          SET
                {1}
          WHERE
                {2}        
        '''.format(table_name, row_update[:-2], condition)

        self.__debug(query_string)
        self.__cursor.execute(query_string)
        self.__sqlite.commit()
        return True

    def add_sensor(self, sensor):
        """
        Добовляет сенсон в базу
        :param sensor:
        :return:
        1 - OK
        0 - ошибка внесения записи (Exception)
        -1 - oid занят
        -2 - modbus_id занят
        -3 - oid_name занят
        """
        controller_ip = sensor.controller_ip
        sensor_oid = sensor.oid
        oid_name = sensor.oid_name
        modbus_id = sensor.modbus_id
        data_type = sensor.data_type
        description = sensor.description
        register_type = sensor.register_type
        monitoring = int(sensor.monitoring)
        min_value = sensor.get_min_value()
        max_value = sensor.get_max_value()
        value = sensor.value

        try:
            result = self.__cursor.execute(
                'SELECT * FROM sensors WHERE controller_ip = "{0}" AND oid = {1}'.format(
                    controller_ip, sensor_oid
                )
            ).fetchall()
            if len(result) != 0:
                self.__error('oid = {0} у контроллера {1} занят'.format(sensor_oid, controller_ip))
                return -1

            result = self.__cursor.execute(
                'SELECT * FROM sensors WHERE controller_ip = "{0}" AND modbus_id = {1}'.format(
                    controller_ip, modbus_id
                )
            ).fetchall()
            if len(result) != 0:
                self.__debug('modbus_id = {0} у контроллера {1} занят'.format(modbus_id, controller_ip))
                return -2

            result = self.__cursor.execute(
                'SELECT * FROM sensors WHERE controller_ip = "{0}" AND oid_name = "{1}"'.format(
                    controller_ip, oid_name
                )
            ).fetchall()
            if len(result) != 0:
                self.__debug('oid_name = {0} у контроллера {1} занят'.format(oid_name, controller_ip))
                return -3

            self.__cursor.execute(
                '''INSERT INTO sensors 
                        (controller_ip, oid, oid_name, modbus_id, data_type, description, register_type, monitoring, min_value, max_value, value)
                VALUES
                        ("{0}", {1}, "{2}", {3}, "{4}", "{5}", "{6}", "{7}", "{8}", "{9}", "{10}")
                '''.format(
                    controller_ip, sensor_oid, oid_name, modbus_id, data_type, description,
                    register_type, monitoring, min_value, max_value, value
                )
            )
            self.__sqlite.commit()
            return 1

        except Exception as e:
            self.__debug('Ошибка внесения записи о сенсоре.')
            self.__debug(str(e))
            return 0

    def update_sensor_value(self, sensor, new_value):
        return  self.__update_row(
            'sensors',
            'controller_ip="{0}" AND modbus_id={1}'.format(sensor.controller_ip, sensor.modbus_id),
            {'value': new_value}
        )

    def del_controller(self, controller_ip):
        """
        Удаляеет контроллер.
        Если к контроллеру привязаны сенсоры ничего не удаляет и возвращает 1
        :param controller_ip:
        :return:
        1 - все ОК
        0 - ошибка
        -1 - к контроллеру привязаны сенсоры
        """

        try:
            sensors = self.__cursor.execute('SELECT * FROM sensors WHERE controller_ip = "{0}"'.format(controller_ip))

            if len(sensors.fetchall()) != 0:
                self.__debug('К контроллеру привязаны сенсоры!')
                return -1

            self.__cursor.execute('DELETE FROM controllers WHERE ip_address = "{0}"'.format(controller_ip))
            self.__sqlite.commit()
            self.__debug('Контроллер успешно удален {0}'.format(controller_ip))
            return 1

        except Exception as e:
            self.__debug('Ошибка удаления контроллера controller_ip = {0}'.format(controller_ip))
            self.__debug(str(e))
            return 0

    def del_sensor(self, controller_ip, modbus_id):
        """
        Удаляем сенсор
        :param controller_ip:
        :param modbus_id:
        :return:
        True - все ок
        False - ошибка
        """
        try:
            self.__cursor.execute(
                'DELETE FROM sensors WHERE controller_ip = "{0}" AND modbus_id = {1}'.format(
                    controller_ip,
                    modbus_id
                )
            )
            self.__sqlite.commit()
            self.__debug('Успешно удалили сенсор {0} {1}'.format(controller_ip, modbus_id))
            return True

        except Exception as e:
            self.__debug('Ошибка удаления сенсора {0} {1}'.format(controller_ip, modbus_id))
            self.__debug(str(e))
            return False

    def get_controllers(self):
        """
            Возвращает все контроллеры из базы
            :return: [Controller, ...]
        """
        result = self.__cursor.execute('SELECT * FROM controllers')
        return [Controller(*item) for item in list(result)]

    def get_sensors(self, controller_ip=None):
        """
        Возвращает все сенсоры контроллера с controller_ip
        :param controller_ip:
        :return:
        """
        query = '''
                    SELECT
                            *
                    FROM 
                            controllers                                  
                    INNER JOIN 
                            sensors
                    ON 
                            sensors.controller_ip = controllers.ip_address
        '''

        if controller_ip is not None:
            query = '''
                            SELECT
                            *
                            FROM 
                                  controllers                                  
                            INNER JOIN 
                                  sensors
                            ON 
                                  sensors.controller_ip = controllers.ip_address
                            WHERE
                                  sensors.controller_ip = "{0}"
                '''.format(controller_ip)

        return [Sensor(*item) for item in list(self.__cursor.execute(query))]

    def get_sensor(self, controller_ip, modbus_id):
        query = '''
        SELECT
            *
        FROM
            controllers
        INNER JOIN
            sensors
        ON
            sensors.controller_ip = controllers.ip_address
        WHERE
            controller_ip = ?
        AND
            modbus_id = ?
        '''

        self.logger.debug(query)
        result = [Sensor(*item) for item in list(self.__cursor.execute(query, (controller_ip, modbus_id)))]

        if len(result) > 0:
            return result[0]
        return None

    def get_all_sensors(self):
        """
        Возвращает полуную информацию о всех сенсорах (включая контроллеры и корневой oid
        :return:
        """
        query = '''
                    SELECT
                    *
                    FROM 
                          controllers 
                    INNER JOIN 
                          sensors
                    ON 
                          sensors.controller_ip = controllers.ip_address

        '''

        return [Sensor(*item) for item in list(self.__cursor.execute(query))]

    def get_root_oids(self):
        """
        Возвращает содержимое таблицы root_oids
        :return:
        """

        result = self.__cursor.execute('SELECT * FROM root_oids')
        return  list(result)

    def copy_controller(self,
                        controller_ip,
                        new_controller_ip,
                        new_controller_oid,
                        new_oid_name,
                        new_description
                        ):
        """
            Copy controller with sensors as new one
        :param controller_ip:
        :param new_controller_ip:
        :param new_controller_oid:
        :param new_oid_name:
        :param new_description:
        :return:
            True - OK
            False - Error
        """

        sql = '''
                SELECT * FROM controllers WHERE ip_address=?
        '''

        controller = self.__cursor.execute(sql, (controller_ip,)).fetchone()
        self.__debug(controller)

        root_oid, _, _, _, tcp_port, _ = controller
        controller_to_add = Controller(
            root_oid=root_oid,
            ip_address=new_controller_ip,
            oid=new_controller_oid,
            oid_name=new_oid_name,
            tcp_port=tcp_port,
            description=new_description

        )
        ret = self.add_controller(controller_to_add)

        if not ret:
            self.logger.error('add_controller error: {0}'.format(ret))
            return False

        err_sensors = []

        for sensor in self.get_sensors(controller_ip):
            sensor.controller_ip = new_controller_ip
            self.logger.debug(sensor)
            if not self.add_sensor(sensor):
                err_sensors.append(sensor)

        if len(err_sensors) > 0:
            self.__error('Some sensors are not copied:')
            for sensor in err_sensors:
                self.__error('    {0}'.format(sensor))
            return False

        return True

    def change_controllers_ip(self, from_ip, to_ip):
        """
        Change ip of controller
        :return: True if OK, else False
        """
        sql = '''
                UPDATE
                        controllers
                SET
                        ip_address = ?
                WHERE
                        ip_address = ?
        '''

        try:
            self.__debug(sql)
            self.__cursor.execute(sql, (to_ip, from_ip,))
            self.__sqlite.commit()
            self.__debug('Succesfull change controller\'s IP ({0} -> {1})'.format(from_ip, to_ip))
            return True
        except Exception as e:
            self.__debug('Exception when try change controller\'s IP ({0} -> {1})'.format(from_ip, to_ip))
            self.__debug(e)
            return False

    # __logging__
    def __debug(self, msg):
        if self.logger is None:
            return

        self.logger.debug(msg)

    def __error(self, msg):
        if self.logger is None:
            return

        self.logger.error(msg)

    def __warning(self, msg):
        if self.logger is None:
            return

        self.logger.warning(msg)

    def __info(self, msg):
        if self.logger is None:
            return

        self.logger.info(msg)