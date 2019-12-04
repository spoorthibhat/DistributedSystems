"""
CPSC 5520, Seattle University
This is the assignment submission for Assignment2.
Note: How to run? lab2.py GCD_HOST GCD_PORT NEXT_BIRTHDAY SUID
Ex: lab2.py localhost 3000 2019-11-20 2337064
No extra credits were attempted.
:Authors: Spoorthi Bhat
:Version: f19-02
"""

from datetime import datetime
from enum import Enum
import pickle
import selectors
import socket
import sys


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    ELECTION_IN_PROGRESS = 'ELECTION_IN_PROGRESS'
    ELECTION_NOT_IN_PROGRESS = 'ELECTION_NOT_IN_PROGRESS'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


BACKLOG = 100  # Number of unaccepted connections to allow
BUF_SZ = 4096  # tcp receive buffer size
CHECK_INTERVAL = 0.2
TIME_OUT = 2  # The time before which a response should be returned(in seconds)
PEER_DIGITS = 100


class Lab2Node(object):
    """
    This class defines the node that will join a group by connecting to GCD and demonstrate Bully Algorithm.
    """

    def __init__(self, gcd_host, gcd_port, next_birthday, su_id):
        """
        Constructor on Lab2Node that initializes all the variables needed for the execution.
        :param gcd_host: GCD host name
        :param gcd_port: GCD port number
        :param next_birthday: Next birthday datetime
        :param su_id: SU ID
        """
        self.gcd_address = (gcd_host, gcd_port)
        days_to_birthday = (next_birthday - datetime.now()).days
        self.process_id = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.timer_start = None
        self.is_ok_received = False
        self.waiting_for_winner_timer = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def start_a_server(self):
        """Starts the server and initializes the listener socket"""
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        soc.bind(('localhost', 0))
        soc.listen(BACKLOG)
        soc.setblocking(False)
        self.selector.register(soc, selectors.EVENT_READ, data=None)

        return soc, soc.getsockname()

    def run(self):
        """Initial method that runs the node"""
        # join the group and start election
        self.join_group()
        self.start_election()

        # listen to events continuously
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message_from_peer(key.fileobj)
                else:
                    self.send_message_to_peer(key.fileobj)
            self.check_timeouts()

    def accept_peer(self):
        """ Accepts the connection from the peer and sets the event to wait for as read event """
        conn, addr = self.listener.accept()
        print('Accepted connection from', addr)
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, data=None)
        self.set_state(State.WAITING_FOR_ANY_MESSAGE)

    def start_connection_to_peer(self, peer_address, state):
        """
        Gets the connection to the peer and also sets the state with respect to the peer.
        :param peer_address: Peer address to connect to
        :param state: State to be set in relation with the peer
        """
        if peer_address != self.listener_address:
            soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            soc.setblocking(False)
            soc.connect_ex(peer_address)
            self.set_state(state, soc)  # soc used for connection -> state
            self.selector.register(soc, selectors.EVENT_WRITE, data=None)

    def send_message_to_peer(self, soc_peer):
        """
        Sends the message to the peer on the given peer socket.
        :param soc_peer: Peer's socket
        """
        state_value = self.get_state(soc_peer)
        print('{}: sending {} [{}]'.format(self.pr_socket(soc_peer), state_value, self.pr_now()))
        try:
            self.send(state_value, self.members, soc_peer)
        except ConnectionError as err:
            self.clear_socket(soc_peer)  # To stop polling again
            print('Failed to connect: ', err)
        except Exception as err:
            self.clear_socket(soc_peer)
            print(err)

        if state_value == State.SEND_ELECTION.value:
            self.set_state(State.WAITING_FOR_OK, soc_peer)

    def send(self, message_name, message_data, soc_peer):
        """
        Helper send function that sends the message to the peer.
        :param message_name: Message label
        :param message_data: Data to be sent with the message
        :param soc_peer: Socket to be used to send the message
        """
        if soc_peer == self or soc_peer == self.listener or State.is_incoming(message_name):
            pass

        if message_name == State.SEND_OK.value:
            message_data = None

        message = (message_name, message_data)
        soc_peer.sendall(pickle.dumps(message))

        if message_name == State.SEND_ELECTION.value:
            if self.timer_start is None:
                self.timer_start = datetime.now()
            self.set_state(State.WAITING_FOR_OK, soc_peer)
            self.selector.unregister(soc_peer)
            self.selector.register(soc_peer, selectors.EVENT_READ, data=None)  # since it is waiting for ok response
        else:  # Not waiting for any response so the socket should be closed
            self.selector.unregister(soc_peer)
            self.clear_memory_about_peer(soc_peer)

    def receive_message_from_peer(self, soc):
        """
        Receives the message from the socket being sent by the peer member.
        :param soc: Socket that is listening.
        """
        try:
            recv_data = soc.recv(BUF_SZ)
            if recv_data:
                message_name, message_data = pickle.loads(recv_data)
                print('Received {} {} : [{}]'.format(message_name, self.pr_socket(soc), self.pr_now()))

                # Some other node is sending an election
                if message_name == State.SEND_ELECTION.value:
                    self.set_state(State.SEND_OK, soc)
                    self.send_message_to_peer(soc)
                    self.set_state(State.WAITING_FOR_VICTOR)
                    self.update_memberships(message_data)
                    if not self.is_election_still_in_progress():
                        self.start_election()
                elif message_name == State.SEND_OK.value:  # Received ok response from some node
                    if self.waiting_for_winner_timer is None:  # Set timer only for first ok received
                        self.waiting_for_winner_timer = datetime.now()
                    self.suspend_election()
                    self.set_state(State.WAITING_FOR_VICTOR)
                elif message_name == State.SEND_VICTORY.value:
                    self.waiting_for_winner_timer = None  # clearing the timer
                    self.set_leader('someone else')
                    self.update_memberships(message_data)
                else:
                    print('Unknown message received')

                if message_name != State.SEND_ELECTION.value:
                    self.clear_socket(soc)

            else:
                self.clear_socket(soc)

        except Exception as err:
            print('Failed to receive: ', err)

    def start_election(self):
        """Start the election by sending the ELECTION message to all the members"""
        self.timer_start = None
        count_higher_members = 0
        print('Starting election with peers...')
        self.set_state(State.ELECTION_IN_PROGRESS)
        for pid, listen_address in self.members.items():
            if (pid[0] > self.process_id[0]) and self.is_election_still_in_progress():
                count_higher_members += 1
                self.start_connection_to_peer(listen_address, State.SEND_ELECTION)

        self.suspend_election()
        if count_higher_members == 0:
            self.declare_victory()

    def is_election_still_in_progress(self):
        """Check if election is still in progress"""
        if self.states[self] == State.ELECTION_IN_PROGRESS.value:
            return True
        return False

    def declare_victory(self):
        """Send coordinator message to all members crowning self as bully"""
        self.set_leader(self.process_id)
        for pid, listen_address in self.members.items():
            self.start_connection_to_peer(listen_address, State.SEND_VICTORY)

    def set_leader(self, leader_pid):
        """
        Sets the bully declared.
        :param leader_pid: Bully's process id
        """
        self.bully = leader_pid
        self.suspend_election()
        self.print_leader()

    def set_state(self, state, peer=None):
        """
        Set the state of the peer.
        :param state: State to be set
        :param peer: Peer socket
        """
        if peer is None:
            peer = self
        self.states[peer] = state.value

    def get_state(self, peer=None):
        """
        Get the state of the peer
        :param peer: peer socket
        :return: state obtained from states dictionary.
        """
        if peer is None:
            peer = self
        return self.states[peer]

    def update_memberships(self, member_list):
        """Update the member list whenever a new list is encountered"""
        print('Updating members...')
        for pid, listen_address in member_list.items():
            self.members[pid] = listen_address
        print('Current members: ', self.members)

    def check_timeouts(self):
        """Checks for any timeouts occurred """

        # Check if no ok response was received after the election was declared. Declare victory if timed out
        if self.timer_start is not None and (datetime.now() - self.timer_start).total_seconds() > TIME_OUT:
            self.declare_victory()

        # Check if we have received first OK response but not yet received coordinator message.
        # Start election if timed out
        if self.waiting_for_winner_timer is not None and (datetime.now() - self.waiting_for_winner_timer).total_seconds() > TIME_OUT:
            self.start_election()

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp"""
        return datetime.now().strftime('%H:%M:%S.%f')

    def pr_socket(self, sock):
        """Printing helper for given socket"""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    def print_leader(self):
        """Helper function to print the leader"""
        bully = self.bully
        if self.bully == self.process_id:
            bully = 'self'

        print('Bully declared: ', bully)

    @staticmethod
    def cpr_sock(sock):
        """Static version to print given socket"""
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = "???"
        return '{}->{} ({})'.format( l_port, r_port, id(sock))

    def join_group(self):
        """Join the group by connecting to gcd
        gcd gives member list in the format: {(68, 2337064): ('127.0.0.1', 50022)}
        """
        join_message = ('JOIN', (self.process_id, self.listener_address))
        print('Joining group by connecting to gcd:', self.gcd_address)
        try:
            soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            soc.connect(self.gcd_address)
            soc.sendall(pickle.dumps(join_message))
            data = soc.recv(BUF_SZ)
            if data:
                self.members = pickle.loads(data)
                print('Initial members given by gcd: ', self.members)
            else:
                raise ValueError('GCD did not respond!')
        except Exception as err:
            print('Failed to connect: ', err)

    def suspend_election(self):
        """Helper function to suspend the ongoing election and reset the state"""
        self.timer_start = None
        self.is_ok_received = False
        self.set_state(State.ELECTION_NOT_IN_PROGRESS)

    def clear_memory_about_peer(self, peer):
        """
        Clears the memory of the peer socket stored in the states dictionary
        :param peer: Peer socket to remove
        """
        del self.states[peer]

    def clear_socket(self, soc):
        """
        Unregisters and closes the socket
        :param soc: Socket
        """
        self.selector.unregister(soc)
        soc.close()


if __name__ == '__main__':
    if len(sys.argv) != 5:
        # Example on how to run : lab2.py localhost 3000 2019-11-20 2337064
        print("Usage: python lab2.py GCD_HOST GCD_PORT BIRTHDAY SU_ID")
        exit(1)

    gcd_host = sys.argv[1]
    gcd_port = int(sys.argv[2])
    birthday = datetime.strptime(sys.argv[3], "%Y-%m-%d")
    su_id = sys.argv[4]

    node = Lab2Node(gcd_host, gcd_port, birthday, su_id)
    node.run()
