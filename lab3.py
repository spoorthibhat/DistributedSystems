"""
CPSC 5520, Seattle University
This is the assignment submission for Assignment3 - PubSub.
Note: How to run? python lab3.py SUBSCRIBER_HOST SUBSCRIBER_PORT
Ex: python lab3.py localhost 8080

:Authors: Spoorthi Bhat
:Version: f19-02
"""

import bellman_ford
from datetime import datetime
import fxp_bytes_subscriber
import math
import socket
import sys

BUFF_SIZE = 4096  # Buffer size
TIME_OUT = 600  # 10 minutes
SERVER_ADDRESS = ('cs2.seattleu.edu', 50403)  # Publisher address
MARKET_VALIDITY = 1.5  # 1.5 seconds


class Lab3PubSub(object):
    """
    This class implements a subscriber that subscribes itself to the forex market
    and outputs any arbitrage opportunities.
    """
    def __init__(self, address):
        """
        Initializing the object with the input address passed.
        :param address: The address of the subscriber
        """
        self.listener = self.subscribe_to_publisher(SERVER_ADDRESS, address)
        self.subscribe_start_time = datetime.now()
        self.quotes = {}
        self.bellman_ford = bellman_ford.BellmanFord({})

    def subscribe_to_publisher(self, server_address, address):
        """
        Function to subscribe to the forex market by sending its address and outputs the listener.
        :param server_address: Publisher address
        :param address: Subscriber address
        :return: Listening socket
        """

        soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        soc.bind(address)
        try:
            soc.sendto(fxp_bytes_subscriber.serialize_address(soc.getsockname()[0], soc.getsockname()[1]), server_address)
        except Exception as err:
            print('Failed to connect', err)

        return soc

    def run(self):
        """
        Function that listens to the published events for 10 minutes and searches for any arbitrage opportunities.
        """
        while (datetime.now() - self.subscribe_start_time).total_seconds() < TIME_OUT:
            message_bytes, address = self.listener.recvfrom(BUFF_SIZE)
            for b in range(0, len(message_bytes), 32):
                self.process_message(message_bytes[b:b+32])

            # Check for any stale markets
            is_timeout_found = self.check_timeouts()
            if is_timeout_found:
                self.reconstruct_bellman_ford()

            # Compute shortest distance
            dist, pred, neg_edge = self.bellman_ford.compute_shortest_distance('USD')
            if neg_edge is not None:
                self.print_arbitrage(pred)

        self.listener.close()

    def process_message(self, message_bytes):
        """
        Function that takes the bytes as the input, deserializes into meaningful
        messages and records the quotes
        :param message_bytes: 32-byte stream
        """
        timestamp = fxp_bytes_subscriber.deserialize_utcdatetime(message_bytes[0:8])
        currency1 = fxp_bytes_subscriber.decode_currency(message_bytes[8:11])
        currency2 = fxp_bytes_subscriber.decode_currency(message_bytes[11:14])
        exchange_rate = fxp_bytes_subscriber.deserialize_price(message_bytes[14:22])
        data_record_time = datetime.now()
        print('{} {} {} {}'.format(timestamp, currency1, currency2, exchange_rate))

        quote = str(currency2) + '/' + str(currency1)
        if quote not in self.quotes:  # New quote found
            self.quotes[quote] = (timestamp, exchange_rate, currency1, currency2, data_record_time)
            self.bellman_ford.add_edge(currency1, currency2, math.log10(exchange_rate) * -1)
            self.bellman_ford.add_edge(currency2, currency1, math.log10(exchange_rate))
        else:
            if self.quotes[quote][0] < timestamp:  # replacing with the newer timestamped message
                self.quotes[quote] = (timestamp, exchange_rate, currency1, currency2, data_record_time)
            else:
                print('ignoring out-of-sequence message')

    def reconstruct_bellman_ford(self):
        """
        Reconstruct bellman ford graph completely.
        """
        self.bellman_ford = bellman_ford.BellmanFord({})
        for quote in self.quotes.values():
            self.bellman_ford.add_edge(quote[2], quote[3], math.log10(quote[1]) * -1)
            self.bellman_ford.add_edge(quote[3], quote[2], math.log10(quote[1]))

    def print_arbitrage(self, predecessors):
        """
        Print the arbitrage opportunity found
        :param predecessors: Predecessor list
        """
        path = []
        current = 'USD'
        while True:
            path.append(current)
            if len(path) > 1 and current == 'USD':
                break
            current = predecessors[current]
        print('Arbitrage opportunity:')
        print('     Start with USD 100')
        curr_rate = 100
        for i in range(len(path) - 1, 0, -1):
            quote = str(path[i-1]) + '/' + str(path[i])
            if quote not in self.quotes:
                quote = str(path[i]) + '/' + str(path[i-1])
                exchange_rate = 1/self.quotes[quote][1]
            else:
                exchange_rate = self.quotes[quote][1]
            after_exchange = curr_rate * exchange_rate
            print('     Exchange {} for {} at {} --> {} {}'.format(path[i], path[i-1], exchange_rate,
                                                              path[i-1], after_exchange))
            curr_rate = after_exchange

    def check_timeouts(self):
        """
        Function that checks if any quote is over 1.5 seconds and thus expired.
        Removes any such quotes
        :return: True if any timeout was found
        """
        is_timeout_occured = False
        delete = []
        for key, quote in self.quotes.items():
            if (datetime.now() - quote[4]).total_seconds() > MARKET_VALIDITY:
                print('removing stale quote for ', key)
                delete.append(key)
                is_timeout_occured = True
        for i in delete:
            del self.quotes[i]

        return is_timeout_occured


if __name__ == '__main__':
    if len(sys.argv) != 3:
        # Example on how to run : python lab3.py localhost 8080
        print("Usage: python lab3.py SUBSCRIBER_HOST SUBSCRIBER_PORT")
        exit(1)

    subscriber = Lab3PubSub((sys.argv[1], int(sys.argv[2])))
    subscriber.run()

