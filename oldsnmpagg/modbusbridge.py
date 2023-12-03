from pymodbus.constants import Endian
from pymodbus.utilities import make_byte_string
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ModbusException, ModbusIOException
from pymodbus.bit_read_message import ReadDiscreteInputsResponse, ReadCoilsResponse
from pymodbus.register_read_message import ReadHoldingRegistersResponse, ReadInputRegistersResponse
from struct import unpack,pack

from snmpagg.wrappers import Sensor
MODBUS_TIMEOUT = 100
MAX_ATTEMPT = 1

REGISTERS = {
    'integer': 1,
    'int': 1,
    'real': 2
}


class ExtendedBinaryPayloadBuilder(BinaryPayloadBuilder):
    def add_16bit_float(self, value):
        ''' Adds a 32 bit float to the buffer

        :param value: The value to add to the buffer
        '''
        fstring = self._endian + 'e'
        self._payload.append(pack('e', value))


#class ExtendedBinaryPayloadDecoder(BinaryPayloadDecoder):
#    def decode_16bit_float(self):
#        """
#        Decodes a 16 bit float from the buffer
#        """
#        self._pointer += 2
#        fstring = self._endian + 'e'
#        handle = self._payload[self._pointer - 2:self._pointer]
#        handle = make_byte_string(handle)
#        return unpack(fstring, handle)[0]
#
#    def get_real(self,addres=1):
#        return self.decode_32bit_float()
#
#    def get_uint(self,addres=1):
#        return self.decode_16bit_uint()


class ModBusBridge(object):

    connect_state = False

    def __init__(self, addr, port, timeout=MODBUS_TIMEOUT, logger=None):
        self.logger = logger
        self.client = ModbusClient(addr, port=port, timeout=timeout)
        self.connect_state = self.client.connect()

    def __del__(self):
        self.client.close()

    @staticmethod
    def __get_register_number(data_type):
        try:
            return REGISTERS[data_type]
        except KeyError:
            return REGISTERS['integer']

    def __decode(self, result, register_num):
        if type(result) == ReadInputRegistersResponse:
            self.logger.debug('Decode ReadInputRegistersResponse')
            return self.__input_decode(result, register_num)
        elif type(result) == ReadDiscreteInputsResponse:
            self.logger.debug('Decode ReadDiscreteInputsResponse')
            return self.__discrete_decode(result)
        elif type(result) == ReadCoilsResponse:
            self.logger.debug('Decode ReadCoilsResponse')
            return self.__coil_decode(result)
        elif type(result) == ReadHoldingRegistersResponse:
            self.logger.debug('Decode ReadHoldingRegistersResponse')
            return self.__holding_decode(result, register_num)

    def __input_decode(self, result, register_num):
        decoder = BinaryPayloadDecoder.fromRegisters(result.registers,
                                                     byteorder=Endian.Big, wordorder=Endian.Little)

        if register_num == REGISTERS['integer']:
            return decoder.decode_16bit_uint()
        elif register_num == REGISTERS['real']:
            return decoder.decode_32bit_float()

        self.logger.error('Unknown register type!')
        return None

    def __holding_decode(self, result, register_num):
        return self.__input_decode(result, register_num)

    @staticmethod
    def __get_fist_bit(result):
        return int(result.bits[0])

    @staticmethod
    def __discrete_decode(result):
        return ModBusBridge.__get_fist_bit(result)

    @staticmethod
    def __coil_decode(result):
        return ModBusBridge.__get_fist_bit(result)

    def set_value(self, sensor: Sensor, value, attempt=MAX_ATTEMPT):
        while attempt > 0:
            register_num = self.__get_register_number(sensor.data_type)
            try:
                if sensor.register_type == 'coil':
                    self.logger.debug('    Write Coil Register: {0}({1})'.format(value, type(value)))
                    result = self.client.write_coil(sensor.modbus_id, value, unit=1)

                elif sensor.register_type == 'holding_reg':
                    builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Little)
                    if type(value) == int:
                        builder.add_16bit_uint(value)
                    else:
                        builder.add_32bit_float(value)

                    registers = builder.to_registers()
                    self.logger.debug('registers: {0}'.format(registers))

                    result = self.client.write_registers(sensor.modbus_id, registers, unit=1)

                else:
                    self.logger.debug('   Returned value: None')
                    return None

                if type(result) in [ModbusIOException, ModbusException]:
                    self.logger.error('{0} ID={1}, TYPE={2}'.format(result, sensor.modbus_id, sensor.data_type))
                    attempt -= 1
                    continue

                return self.__decode(result, register_num)

            except Exception as e:
                self.logger.error('Unknown error. ID={0} TYPE={1}'.format(sensor.modbus_id, sensor.data_type))
                self.logger.debug('{0} - {1}'.format(type(e), e))
                attempt -= 1

        self.logger.debug('{0} errors in MAX attempt times. Return None!')
        return None

    def get_value(self, sensor: Sensor, attempt=MAX_ATTEMPT):
        while attempt > 0:
            register_num = self.__get_register_number(sensor.data_type)
            self.logger.debug('register_num: {0}'.format(register_num))

            try:
                if sensor.register_type == 'input':
                    self.logger.debug('    Input Register')
                    result = self.client.read_input_registers(sensor.modbus_id, register_num, unit=1)
                elif sensor.register_type == 'discrete':
                    self.logger.debug('    Discrete Register')
                    result = self.client.read_discrete_inputs(sensor.modbus_id, 1, unit=1)
                elif sensor.register_type == 'coil':
                    self.logger.debug('    Coil Register')
                    result = self.client.read_coils(sensor.modbus_id, 1, unit=1)
                elif sensor.register_type == 'holding_reg':
                    self.logger.debug('    Holding Register')
                    result = self.client.read_holding_registers(sensor.modbus_id, register_num, unit=1)
                else:
                    self.logger.debug('   Returned value: None')
                    return None

                if type(result) in [ModbusIOException, ModbusException]:
                    self.logger.error('{0} ID={1}, TYPE={2}'.format(result, sensor.modbus_id, sensor.data_type))
                    attempt -= 1
                    continue

                decoded_result = self.__decode(result, register_num)
                self.logger.debug('   Returned value: {0}'.format(decoded_result))
                return decoded_result

            except Exception as e:
                self.logger.error('Unknown error. ID={0} TYPE={1}'.format(sensor.modbus_id, sensor.data_type))
                self.logger.debug('{0} - {1}'.format(type(e), e))
                attempt -= 1

        self.logger.debug('{0} errors in MAX attempt times. Return None!')
        return None

    def close(self):
        self.client.close()

    def __debug(self, msg):
        if self.logger is not None:
            self.logger.debug(msg)

    def __warn(self, msg):
        if self.logger is not None:
            self.logger.warn(msg)

    def __info(self, msg):
        if self.logger is not None:
            self.logger.info(msg)

    def __error(self, msg):
        if self.logger is not None:
            self.logger.error(msg)