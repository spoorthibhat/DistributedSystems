"""
CPSC 5520, Seattle University
This is the assignment submission for Lab4 - DHT.
Note: This program chord_populate.py is run only once. i.e., Right after first node has joined the network
:Authors: Spoorthi Bhat
:Version: f19-02
"""
import csv
import hashlib
import pickle
import socket
import sys

HOST = 'localhost'


class ChordPopulate(object):
    """Chord populate class that is used to populate the first node in the chord system with the data"""

    def __init__(self, filename):
        """
        Initiates the object by reading the file contents to a dictionary: key->row
        :param filename: Filename to read the contents from.
        """
        with open(filename) as data_file:
            read_file = csv.reader(data_file, delimiter=',')
            next(read_file)
            self.keys = {}
            for row in read_file:
                each_key = row[0] + row[3]
                key_identifier = int(hashlib.sha1(each_key.encode()).hexdigest(), 16)
                self.keys[key_identifier] = row

    def load(self, port):
        """Function to call rpc to the only node in the chord network and pass the file contents to it"""
        print('Populating keys to the node')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
                soc.connect((HOST, port))
                soc.sendall(pickle.dumps(('populate_keys', self.keys)))
        except Exception as err:
            print(err)


if __name__ == '__main__':
    """Example on how to run :
       python chord_populate.py <port of the only node that joined the network> Career_Stats_Passing.csv
       Port number can be fetched from the output console of the first node being run. It would show something like,
       Node 714630580069727847654076509279678416881005246837 listening on port 52270 for incoming connections...
    """
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py EXISTING_PORT filename")
        exit(1)

    chord_populate = ChordPopulate(sys.argv[2])
    chord_populate.load(int(sys.argv[1]))
