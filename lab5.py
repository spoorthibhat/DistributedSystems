"""
CPSC 5520, Seattle University
This is the assignment submission for Lab5 - Bitcoin.
The program connects to a peer in the bitcoin network by sending version and verack messages.
:Authors: Spoorthi Bhat
:Version: f19-02
"""

import hashlib
import random
import socket
import struct
import time

VERSION = 70015  # As of https://bitcoin.org/en/developer-reference#protocol-versions most recent version is 70015
BLOCK_HEIGHT = 606609  # Latest from https://www.blockchain.com/explorer
MAGIC_VALUE = 'f9beb4d9'  # https://en.bitcoin.it/wiki/Protocol_documentation#Common_structures
BUF_SZ = 4096  # Buffer size
HDR_SZ = 24  # header size


def get_version_message(recv_addr):
    """
    Frames the complete version message including the header
    :param recv_addr: Receiving peer address
    :return: Version message
    """
    version = int32_t(VERSION)
    services = uint64_t(0)
    timestamp = struct.pack("q", int(time.time()))
    addr_recv_services = uint64_t(0)
    add_recv_ip = struct.pack(">16s", bytes(recv_addr[0], 'utf-8'))

    # The receiver's port (Bitcoin default is 8333)
    add_recv_port = struct.pack(">H", 8333)

    addr_trans_services = uint64_t(0)
    add_trans_ip = struct.pack(">16s", bytes("127.0.0.1", 'utf-8'))
    add_trans_port = struct.pack(">H", 8333)

    nonce = uint64_t(random.getrandbits(64))
    user_agent_bytes = struct.pack("B", 0)
    start_height = struct.pack('i', BLOCK_HEIGHT)
    relay = struct.pack('?', 0)
    payload = version + services + timestamp + addr_recv_services + add_recv_ip + add_recv_port + \
              addr_trans_services + add_trans_ip + add_trans_port + nonce + user_agent_bytes + start_height + relay

    header = build_header('version', payload)
    message = header + payload

    print_message(message, 'Sending')
    return message


def build_header(command, payload):
    """
    Builds the header message from the command name and the payload input provided
    :param command: Command name
    :param payload: Payload in bytes
    :return: Header message
    """
    magic = bytes.fromhex(MAGIC_VALUE)  # use main network
    padding_count = 12 - len(command.encode())
    command_name = command.encode() + padding_count * b"\00"
    length = uint32_t(len(payload))
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[0:4]

    return magic + command_name + length + checksum


def connect_to_peer():
    """
    Connects to an active peer in the bitcoin network
    Note: For now it connects to peer at address '1.255.226.167', if there
    is any problem with that connection during execution, please UNCOMMENT from line 79-88, UNCOMMENT line 90 and
    COMMENT line 89 and line 91.
    """
    peers = []
    '''
    with open('nodes_main.txt') as node_file:
        cnt = 1
        line = node_file.readline()
        peers.append(line.split(':'))
        while cnt < 512:
            line = node_file.readline()
            peers.append(line.split(':'))
            cnt += 1
            '''
    peers.append(('1.255.226.167', 8333))
    # peer_index = connect(peers, 0)
    peer_index = 0
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as version_soc:
            version_soc.connect((peers[peer_index][0], 8333))
            version_soc.sendall(get_version_message(peers[peer_index][0]))
            count = 0
            message = b''
            while count < 2:
                msg = version_soc.recv(2 ** 10)
                message += msg
                count += 1

        user_agent_size, uasz = unmarshal_compactsize(message[104:])
        i = 104 + len(user_agent_size) + uasz + 4 + 1
        print_message(message[0:i], 'Received')
        extra = message[i:]
        print_message(extra, 'Received')

        verack_header = build_header('verack', b'')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as verack_soc:
            verack_soc.connect((peers[peer_index][0], 8333))
            print_message(verack_header, 'Sending')
            verack_soc.sendall(verack_header)
            recv_msg = verack_soc.recv(2 ** 10)
            while recv_msg:
                print_header(recv_msg)

    except Exception as err:
        print(err)


def connect(peers, index):
    """
    Tries to connect with the peers, returns the peer index.
    Keeps trying to connect until a connection can be established. i.e., an active peer is found
    :param peers: Array of peer addresses
    :param index: Index in the peers array
    :return: The index of successfully connected peer
    """
    try:
        print('Trying to connect to {}'.format(peers[index]))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
            e = soc.connect((peers[index][0], int(peers[index][1])))
            return index
    except Exception as err:
        return connect(index + 1)


def compactsize_t(n):
    """
    Converts to compact size integer type
    :param n: Input number to be converted
    :return: Compact size integer type of n
    """
    if n < 252:
        return uint8_t(n)
    if n < 0xffff:
        return uint8_t(0xfd) + uint16_t(n)
    if n < 0xffffffff:
        return uint8_t(0xfe) + uint32_t(n)
    return uint8_t(0xff) + uint64_t(n)


def unmarshal_compactsize(b):
    """
    Unmarshalls compact size integer
    :param b: The byte array to be unmarshalled
    """
    key = b[0]
    if key == 0xff:
        return b[0:9], unmarshal_uint(b[1:9])
    if key == 0xfe:
        return b[0:5], unmarshal_uint(b[1:5])
    return b[0:1], unmarshal_uint(b[0:1])


def ipv6_from_ipv4(ipv4_str):
    """
    Converts to ipv6 from ipv4 representation
    :param ipv4_str: Ip in ipv4 format
    :return: ipv6 string
    """
    pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
    return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))


def ipv6_to_ipv4(ipv6):
    """
    Converts to ipv4 from ipv6 representation
    :param ipv6: ipv6 string
    :return: ipv4 string
    """
    return '.'.join([str(b) for b in ipv6[12:]])


def uint8_t(n):
    """Converts n to unsigned int8_t format"""
    return int(n).to_bytes(1, byteorder='little', signed=False)


def uint16_t(n):
    """Converts n to unsigned int16_t format"""
    return int(n).to_bytes(2, byteorder='little', signed=False)


def int32_t(n):
    """Converts n to int32_t format"""
    return int(n).to_bytes(4, byteorder='little', signed=True)


def uint32_t(n):
    """Converts n to unsigned int32_t format"""
    return int(n).to_bytes(4, byteorder='little', signed=False)


def int64_t(n):
    """Converts to int64_t format"""
    return int(n).to_bytes(8, byteorder='little', signed=True)


def uint64_t(n):
    """Converts to unsigned int64_t format"""
    return int(n).to_bytes(8, byteorder='little', signed=False)


def unmarshal_int(b):
    """Unmarshals byte array to int"""
    return int.from_bytes(b, byteorder='little', signed=True)


def unmarshal_uint(b):
    """Unmarshals to unsigned int"""
    return int.from_bytes(b, byteorder='little', signed=False)


def checksum(payload):
    """Calculates the checksum from the input payload byte string"""
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[0:4]


def print_message(msg, text=None):
    """
    Report the contents of the given bitcoin message
    :param msg: bitcoin message including header
    :return: message type
    """
    print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
    print('({}) {}'.format(len(msg), msg[:60].hex() + ('' if len(msg) < 60 else '...')))
    payload = msg[HDR_SZ:]
    command = print_header(msg[:HDR_SZ], checksum(payload))
    if command == 'version':
        print_version_msg(payload)
    # FIXME print out the payloads of other types of messages, too
    return command


def print_version_msg(b):
    """
    Report the contents of the given bitcoin version message (sans the header)
    :param payload: version message contents
    """
    # pull out fields
    version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
    rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
    nonce = b[72:80]
    user_agent_size, uasz = unmarshal_compactsize(b[80:])
    i = 80 + len(user_agent_size)
    user_agent = b[i:i + uasz]
    i += uasz
    start_height, relay = b[i:i + 4], b[i + 4:i + 5]
    extra = b[i + 5:]

    # print report
    prefix = '  '
    print(prefix + 'VERSION')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:32} my services'.format(prefix, my_services.hex()))
    time_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
    print('{}{:32} your services'.format(prefix, your_services.hex()))
    print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
    print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))
    print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
    print('{}{:32} my host {}'.format(prefix, my_host.hex(), ipv6_to_ipv4(my_host)))
    print('{}{:32} my port {}'.format(prefix, my_port.hex(), unmarshal_uint(my_port)))
    print('{}{:32} nonce'.format(prefix, nonce.hex()))
    print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
    print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
    print('{}{:32} start height {}'.format(prefix, start_height.hex(), unmarshal_uint(start_height)))
    print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
    if len(extra) > 0:
        print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))


def print_header(header, expected_cksum=None):
    """
    Report the contents of the given bitcoin message header
    :param header: bitcoin message header (bytes or bytearray)
    :param expected_cksum: the expected checksum for this version message, if known
    :return: message type
    """
    magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    psz = unmarshal_uint(payload_size)
    if expected_cksum is None:
        verified = ''
    elif expected_cksum == cksum:
        verified = '(verified)'
    else:
        verified = '(WRONG!! ' + expected_cksum.hex() + ')'
    prefix = '  '
    print(prefix + 'HEADER')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} magic'.format(prefix, magic.hex()))
    print('{}{:32} command: {}'.format(prefix, command_hex.hex(), command))
    print('{}{:32} payload size: {}'.format(prefix, payload_size.hex(), psz))
    print('{}{:32} checksum {}'.format(prefix, cksum.hex(), verified))
    return command


if __name__ == '__main__':

    connect_to_peer()
