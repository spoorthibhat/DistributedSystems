"""
This module supplies the functions needed by the subscriber(for PubSub assignment) in order
to serialize or deserialize the elements.

:Authors: Spoorthi Bhat
:Version: f19-02
"""

from array import array
from datetime import datetime, timedelta

MICROS_PER_SECOND = 1_000_000


def serialize_address(host, port: int):
    """
    Converts the address into 6 byte stream
    :param host: host address
    :param port: port address
    :return: 4 + 2 byte stream
    """
    outputba = bytearray()
    ipAddrComponents = host.split('.')
    for i in range(0,len(ipAddrComponents)):
        a = int(ipAddrComponents[i])
        bb = (a).to_bytes(1, byteorder='big')
        outputba.append(bb[0])
    bb = (port).to_bytes(2, byteorder='big')
    outputba.append(bb[0])
    outputba.append(bb[1])
    return bytes(outputba)


def deserialize_utcdatetime(b: bytes):
    """
    Desrializes the datetime by converting the 8-byte stream into appropriate datetime.
    :param b: 8-byte stream
    :return: datetime
    """
    microseconds = int.from_bytes(b, byteorder='big', signed=False)
    timestamp = datetime(1970, 1, 1) + timedelta(seconds=microseconds/MICROS_PER_SECOND)
    return timestamp


def decode_currency(b: bytes):
    """
    Decodes the currency from bytes to string.
    :param b: bytes
    :return: Currency
    """
    return b.decode('utf-8')


def deserialize_price(b: bytes):
    """
    Converts byte array to 64-bit floating point number
    :param b: Bytes in IEEE 754 binary64 little-endian format.
    :return: floating point number
    """
    p = array('d')
    p.frombytes(b)
    return p[0]


