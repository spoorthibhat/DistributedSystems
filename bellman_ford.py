"""
This module contains the useful bellman ford algorithm needed to find
any arbitrage opportunities in the PubSub Assignment.
:Authors: Spoorthi Bhat
:Version: f19-02
"""

TOLERANCE = 1e-12

class Edge(object):
    """
    This class defines the edge of a graph.
    """
    def __init__(self, u, v, cost):
        """
        Constructs a directed edge with the vertices (u --> v) and the cost
        :param u: Vertex1
        :param v: Vertex2
        :param cost: Cost of the edge
        """
        self.u = u
        self.v = v
        self.cost = cost


class BellmanFord(object):
    """
    Bellman ford algorithm to computes shortest path to all the vertices
    from the the source vertex and detect any negative cycles if found.
    """
    def __init__(self, graph):
        """
        Initial graph that can be used for the bellman ford.
        :param graph: Initial graph fed
        """
        self.edges = {}
        self.vertices = set()

        for vertex_u, adjascency_list in graph.items():
            for vertex_v, cost in adjascency_list.items():
                edge = Edge(vertex_u, vertex_v, cost)
                key = str(vertex_u) + '->' + str(vertex_v)
                self.edges[key] = edge
                self.vertices.add(vertex_u)
                self.vertices.add(vertex_v)

    def compute_shortest_distance(self, source):
        """
        Computes the shortest distance to all the vertices from the source
        and detects negative cycle.
        :param source: Source vertex
        :return: shortest distances list, predecessor list, negative edge at which negative cycle was found.
        """
        if source not in self.vertices:
            raise ValueError('Source not part of the input graph')

        distance = {}
        predecessors = {}

        for vertex in self.vertices:
            distance[vertex] = float('inf')
            predecessors[vertex] = None

        distance[source] = 0
        for i in range(0, len(self.vertices) - 1):
            for edge in self.edges.values():
                u = edge.u
                v = edge.v
                cost = edge.cost
                if distance[u] != float('inf') and distance[v] > distance[u] + cost:
                    distance[v] = distance[u] + cost
                    predecessors[v] = u

        # Detecting cycles
        neg_edge = None
        for edge in self.edges.values():
            if distance[edge.u] != float('inf') and distance[edge.v] > distance[edge.u] + edge.cost:
                neg_edge = (edge.u, edge.v)
                predecessors[edge.v] = edge.u

        v = 'USD'
        sum_of_weights = 0
        if neg_edge is not None:
            while True:
                if sum_of_weights != 0 and v == 'USD':
                    break
                u = predecessors[v]
                key = str(u) + '->' + str(v)
                sum_of_weights += self.edges[key].cost
                v = u

        if sum_of_weights > TOLERANCE * -1:
            neg_edge = None

        return distance, predecessors, neg_edge

    def add_edge(self, u, v, cost):
        """
        Adds edge to the edges list and the vertices to the vertices list.
        :param u: vertex1
        :param v: vertex2
        :param cost: Cost of the edge
        """
        key = str(u) + '->' + str(v)
        self.edges[key] = Edge(u, v, cost)
        self.vertices.add(u)
        self.vertices.add(v)





