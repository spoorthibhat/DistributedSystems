"""
CPSC 5520, Seattle University
This is the assignment submission for Lab4-DHT.
:Authors: Spoorthi Bhat
:Version: f19-02
"""

import pickle
import socket
import sys

BUF_SZ = 4096
HOST = 'localhost'


class ChordQuery:
    """
    Chord Query class to query any arbitrary node with a key previously populated to the chord system.
    """

    def get_data(self, port, key):
        """
        Gets the value which is the entire row value from the csv file.
        :param port: Port number of the arbitrary node which is queried for the key
        :param key: Key used in order to retrieve the value
        :return: The corresponding row from the csv file
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
                soc.connect((HOST, port))
                print('Querying for key: ', key)
                print('Please wait while the value is retrieved...')
                soc.sendall(pickle.dumps(('query_key', key)))
                data = soc.recv(BUF_SZ)
                unmarshalled_data = pickle.loads(data)
                print('Received value:')
                print(unmarshalled_data)
        except ConnectionError:
            print('Failed to connect to ', port)
        except Exception as err:
            print(err)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        # Example on how to run :
        # python chord_query.py <port number that can be taken from node's output console> breezyreid/2523928 1951
        print("Usage: python chord_query.py EXISTING_PORT KEY_COLUMN1 KEY_COLUMN2")
        exit(1)

    ChordQuery().get_data(int(sys.argv[1]), sys.argv[2] + sys.argv[3])



