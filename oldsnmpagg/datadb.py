import sqlite3
import datetime
from .wrappers import Data
from .wrappers import Sensor


class DataDB(object):
    logger = None

    def __init__(self, db_file, logger = None, mode='rwc'):
        """Если файла нет или он пустой, то создаем базу заднных"""

        self.logger = logger
        self.mode = mode
        self.db_file = db_file

        sql_foreign_on = 'PRAGMA foreign_keys = ON'

        sql_create_data = '''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY,
            controller_ip TEXT,
            oid INTEGER,
            modbus_id INTEGER,
            value BLOB,
            date_time REAL -- Unix TimeStamp as float
        )
        '''
        self.__sqlite = sqlite3.connect('file:{0}?mode={1}'.format(db_file, mode), uri=True)
        self.__cursor = self.__sqlite.cursor()
        self.__cursor.execute(sql_foreign_on)
        if self.mode == 'rwc':
            self.__cursor.execute(sql_create_data)

    def __del__(self):
        try:
            self.__sqlite.commit()
            self.__sqlite.close()
        except Exception as e:
            pass

    def reinit(self):
        self.__init__(self.db_file, logger=self.logger, mode=self.mode)

    def commit(self):
        self.__sqlite.commit()

    def add_data(self, data: Data, autocommit=True):
        if data.id != 0:
            self.__error('Id is not Null: {0}'.format(data))
            return False

        query = '''
        INSERT INTO data
            (controller_ip, oid, modbus_id, value, date_time) 
        VALUES 
            (?, ?, ?, ?, ?)
        '''

        try:
            self.__debug(query)
            self.__cursor.execute(
                query,
                (
                    data.sensor.controller_ip,
                    data.sensor.oid,
                    data.sensor.modbus_id,
                    data.value,
                    data.date_time_as_unixtimestap()
                )
            )
            if autocommit:
                self.commit()
                self.__debug('Successful write data history: {0}'.format(data))
            return True
        except Exception as e:
            self.__error('Exception when try to save data history: {0}'.format(data))
            self.__debug(e)
            self.__sqlite.close()
            self.reinit()
            return False

    def get_data(self, sensor: Sensor, from_date: datetime.datetime, to_date: datetime.datetime, interval):
        try:
            unix_from_date = from_date.timestamp()
            unix_to_date = to_date.timestamp()
        except:
            return None

        query = '''
                SELECT
                    id, value, date_time
                FROM
                    data
                WHERE
                    controller_ip = ?
                AND
                    modbus_id = ?
                AND
                    date_time BETWEEN ? AND ?
                ORDER BY
                    id
                '''

        self.logger.debug(query)
        lst_result = list(self.__cursor.execute(query, (sensor.controller_ip, sensor.modbus_id, unix_from_date, unix_to_date)))
        lst_result = [row for i, row in enumerate(lst_result) if i % int(interval) == 0]

        lst_data = []
        for item_id, value, date_time in lst_result:
            lst_data.append(Data(sensor, value, date_time, item_id))

        return lst_data

    def get_all_data(self, sensor: Sensor, interval):
        query = '''
        SELECT
            id, value, date_time
        FROM
            data
        WHERE
            controller_ip = ?
        AND
            modbus_id = ?
        ORDER BY
            id
        '''

        self.logger.debug(query)
        lst_result = list(self.__cursor.execute(query, (sensor.controller_ip, sensor.modbus_id)))
        lst_result = [row for i, row in enumerate(lst_result) if i % int(interval) == 0]


        lst_data = []
        for item_id, value, date_time in lst_result:
            lst_data.append(Data(sensor, value, date_time, item_id))

        return lst_data

    def get_last_data(self, sensor: Sensor):
        query = '''
        SELECT 
            id, value, date_time
        FROM 
            data
        WHERE
            controller_ip = ?
        AND
            modbus_id = ?
        ORDER BY 
            id
        DESC
        LIMIT 1
        '''

        lst_result = list(self.__cursor.execute(query, (sensor.controller_ip, sensor.modbus_id)))
        if len(lst_result) < 1:
            return None

        item_id, value, date_time = lst_result[0]
        return Data(sensor, value, date_time, item_id)

    def delete_data_older_than(self, days: int):
        now = datetime.datetime.now()
        dt_days_before = now - datetime.timedelta(days=days)
        days_before = dt_days_before.timestamp()

        query = '''
        DELETE FROM data WHERE date_time <= ?
        '''

        self.__debug(query.replace('?', str(days_before)))
        try:
            self.__cursor.execute(query, (str(days_before),))
        except Exception as e:
            self.__error('ERROR: {0}: {1]'.format(type(e), e))

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
