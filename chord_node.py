"""
CPSC 5520, Seattle University
This is the assignment submission for Lab4-DHT.
:Authors: Spoorthi Bhat
:Version: f19-02
"""

import hashlib
import pickle
import socket
import sys
import threading

M = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
HOST = 'localhost'


class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr

    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        """ Returns the length of ModRange"""
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        """Helps in iteration"""
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe

    >>> fe.node = 1
    >>> fe

    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """

    def __init__(self, n, k, node=None):
        """ Initiates the finger entry object"""
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


class ChordNode(object):
    """
    This class constructs the node object with the system generated port and joins the node into the chord network.
    If this is not the first node joining, it updates the finger tables of all the other nodes and takes the keys that
    this node is responsible for from its successor.
    Each finger table entry not only holds the successor node but also its corresponding port.
    """

    def __init__(self, n, port):
        """
        Initiates the node object with the port and node number
        :param n: Node number/ node identifier
        :param port: port on which the node listens
        """
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M + 1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.listener = None
        self.port = port
        self.start_a_server(port)

    @property
    def successor(self):
        """ Returns the successor node of current node"""
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        """ Successor setter function"""
        self.finger[1].node = id

    def find_successor(self, id):
        """
        Ask this node to find id's successor = successor(predecessor(id))
        :param id: Id
        :return: Node and node port of the successor of id
        """
        np, node_port = self.find_predecessor(id)
        return self.call_rpc((np, node_port), 'successor', None)

    def find_predecessor(self, id):
        """
        Ask this node to find the predecessor of id"
        :param id: Id
        :return: The node and port of the predecessor
        """
        np = self.node
        node_port = self.listener.getsockname()[1]
        while id not in ModRange(np + 1, self.call_rpc((np, node_port), 'successor', None)[0] + 1, NODES):
            np, node_port = self.call_rpc((np, node_port), 'closest_preceding_finger', [id])
        return np, node_port

    def closest_preceding_finger(self, id):
        """
        Ask this node to find the closest preceding finger of the given id from the finger table
        :param id: Id to find the preceding finger for
        :return: The node number and the port number of the preceding finger
        """
        for i in range(M, 0, -1):
            if self.finger[i].node[0] in ModRange(self.node + 1, id, NODES):
                return self.finger[i].node  # node + port
        return self.node, self.listener.getsockname()[1]

    def join(self, reference_node_port):
        """
        Join function used by the node while joining the chord system.
        :param reference_node_port: The port of the existing arbitrary node to be used while joining
        """
        print('Node {} joining the chord system'.format(self.node))
        if reference_node_port != 0:
            addr = 'localhost/' + str(reference_node_port)
            reference_node = int(hashlib.sha1(addr.encode()).hexdigest(), 16)
            self.init_finger_table((reference_node, reference_node_port))
            self.update_others()
            self.transfer_keys_from_successor() # Transfer the keys
        else:
            for i in range(1, M + 1):
                self.finger[i].node = (self.node, self.listener.getsockname()[1])  # identifier + port
            self.predecessor = (self.node, self.listener.getsockname()[1])

        self.print_finger_table()
        print('Node {} listening on port {} for incoming connections...'.format(self.node, self.port))
        self.listen_incoming_connections()  # listening for incoming connections

    def transfer_keys_from_successor(self):
        """ Function to transfer the relevant keys to the newly joined node from its successor"""
        print('Transferring keys from successor node ', self.successor)
        self.keys = self.call_rpc(self.successor, 'get_keys_from_successor', [self.node])
        print('Received {} number of keys from {}'.format(len(self.keys), self.successor))

    def init_finger_table(self, np):
        """
        Initiates the finger table of the newly joined node
        :param np: Reference node used for initiating its finger table
        """
        self.finger[1].node = self.call_rpc(np, 'find_successor', [self.finger[1].start])
        print('Found successor of {} to be {}'.format(self.finger[1].start, self.finger[1].node))
        self.predecessor = self.call_rpc(self.successor, 'get_predecessor', None)
        self.call_rpc(self.successor, 'set_predecessor', [(self.node, self.listener.getsockname()[1])])
        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.node, self.finger[i].node[0], NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(np, 'find_successor', [self.finger[i + 1].start])

    def update_others(self):
        """Updates the finger tables of other nodes"""
        print('Updating other nodes finger tables...')
        for i in range(1, M + 1):
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES)
            print('Calling RPC to update finger table of {} at {}th entry'.format(p, i))
            self.call_rpc(p, 'update_finger_table', [(self.node, self.listener.getsockname()[1]), i])

    def update_finger_table(self, s, i):
        """
        Updates the finger table's ith entry with s
        :param s: To be replaced entry
        :param i: entry index
        """
        if (self.finger[i].start != self.finger[i].node[0]
                and s[0] in ModRange(self.finger[i].start, self.finger[i].node[0], NODES)):
            self.finger[i].node = s
            print('Updated self finger table at {}th entry to {}'.format(i, s))

            p = self.predecessor
            if p != s:
                print('Calling RPC to update finger table of {} at {}th entry'.format(p, i))
                self.call_rpc(p, 'update_finger_table', [s, i])

    def call_rpc(self, node, message, arg):
        """
        Makes an rpc call to the node with the request containing the message and arguments
        :param node: (Node number, port)
        :param message: Message
        :param arg: Argument list needed in order to serve this rpc
        :return: The response received after the request was served
        """
        try:
            if node[0] != self.node:
                node_address = (HOST, node[1])
                client = self.start_connection_to_peer(node_address)
                request = pickle.dumps((message, arg))
                client.sendall(request)
                data = b''
                if message != 'get_keys_from_successor':
                    data = client.recv(BUF_SZ)
                else:
                    # the data is too big so need a recv all kind of option
                    while True:
                        try:
                            client.settimeout(1)
                            rpc = client.recv(BUF_SZ)
                            if not rpc:
                                break
                            data += rpc
                        except socket.timeout:
                            break
                return pickle.loads(data)
            else:
                return self.dispatch_rpc(message, arg)
        except Exception as err:
            print(err)

    def start_a_server(self, port):
        """Starts the server and initializes the listener socket"""
        listening_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listening_soc.bind(('localhost', port))
        listening_soc.listen(BACKLOG)
        self.listener = listening_soc

    def listen_incoming_connections(self):
        """Listener listens for any incoming connections"""
        while True:
            client, client_addr = self.listener.accept()
            print('Accepted connection from ', client_addr)
            threading.Thread(target=self.handle_rpc, args=(client,)).start()

    def handle_rpc(self, client):
        """Handles the rpc request from the client"""
        data = b""

        while True:
            # This loop was needed since the data in rpc call was too huge to be handled with just recv
            try:
                client.settimeout(0.5)
                packet = client.recv(BUF_SZ)
                if not packet:
                    break
                data += packet
            except socket.timeout:
                break

        request_message, arg = pickle.loads(data)
        result = self.dispatch_rpc(request_message, arg)
        client.sendall(pickle.dumps(result))

    def dispatch_rpc(self, request_message, arg):
        """
        Dispatches the rpc request depending on the request message passed
        :param request_message: Request message used to understand the kind of request
        :param arg: Argument list needed to serve the request
        :return: Returns the response
        """

        if request_message == 'successor':
            successor = self.successor
            return successor
        elif request_message == 'find_successor':
            found_successor = self.find_successor(arg[0])
            return found_successor
        elif request_message == 'get_predecessor':
            pred = self.predecessor
            return pred
        elif request_message == 'closest_preceding_finger':
            closest_preceding_finger = self.closest_preceding_finger(arg[0])
            return closest_preceding_finger
        elif request_message == 'update_finger_table':
            self.update_finger_table(arg[0], arg[1])
            return None
        elif request_message == 'populate_keys':
            self.keys = arg
            print('Populated data sent in RPC')
            return None
        elif request_message == 'get_keys_from_successor':
            return self.move_keys_to_node(arg[0])
        elif request_message == 'query_key':
            # Request to query the given key
            return self.get_value_of_given_key(arg)
        elif request_message == 'set_predecessor':
            self.predecessor = arg[0]
            print('Changing predecessor to ', self.predecessor)
            return None
        elif request_message == 'get_value_for_key':
            return self.keys[arg[0]]
        else:
            raise ValueError('Unknown request')

    def move_keys_to_node(self, to_be_sent_node_num):
        """
        Gives the relevant keys to be moved to the new node that would be responsible for these keys.
        :param to_be_sent_node_num: Node responsible for the moved keys
        :return: Dictionary of key->row entries from files to be moved to the new joined node
        """
        to_be_sent_data = {}
        to_be_deleted = []
        for key, value in self.keys.items():
            if key not in ModRange(to_be_sent_node_num + 1, self.node + 1, NODES):
                to_be_sent_data[key] = value
                to_be_deleted.append(key)

        # Deletes the keys that were moved
        for toBeDeleted in to_be_deleted:
            del self.keys[toBeDeleted]

        print('Transferring {} number of keys to node {}'.format(len(to_be_sent_data), to_be_sent_node_num))
        return to_be_sent_data

    def get_value_of_given_key(self, key):
        """Gets the value for the given key."""
        key_identifier = int(hashlib.sha1(key.encode()).hexdigest(), 16)
        print('Query for the key {} with identifier: {}'.format(key, key_identifier))
        node_responsible_for_key = self.find_successor(key_identifier)
        return self.call_rpc(node_responsible_for_key, 'get_value_for_key', [key_identifier])

    def print_finger_table(self):
        """Helper function to print the finger table"""
        print('\tFinger table: start->successor(node, port)')
        for finger in self.finger:
            if finger is not None:
                print('\t {} {}'.format(finger.start, finger.node))

    @staticmethod
    def start_connection_to_peer(peer_address):
        """Starts a connection to the peer"""
        connect_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connect_soc.connect(peer_address)
        return connect_soc


if __name__ == '__main__':
    if len(sys.argv) != 2:
        # Example on how to run : python chord_node.py 0
        print("Usage: python chord_node.py EXISTING_PORT")
        exit(1)

    existing_port = int(sys.argv[1])

    # Get the system generated port number for the node
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as soc:
        soc.bind((HOST, 0))
        current_node_port = soc.getsockname()[1]

    to_be_hashed_addr = 'localhost/' + str(current_node_port)
    node_number = int(hashlib.sha1(to_be_hashed_addr.encode()).hexdigest(), 16)

    chord_node = ChordNode(node_number, current_node_port)
    chord_node.join(existing_port)
