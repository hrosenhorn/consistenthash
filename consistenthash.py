# coding=UTF-8

#
# Small implementation of Consistent Hashing
# Inspiration taken from the Ketama and Concurrence implementation
# If you wish to know more about the topic here is some good reading
#
# http://www.last.fm/user/RJ/journal/2007/04/10/rz_libketama_-_a_consistent_hashing_algo_for_memcache_clients
# http://en.wikipedia.org/wiki/Consistent_hashing
#

import hashlib
import bisect
import logging

logger = logging.getLogger("continuum")

class ConsistentHash(object):
    def __init__(self, nodes = []):
        """
        Create a new consistent hash ring

        @param nodes: A list containing a tuple in the form [(node, vpoints)].

        Example:
          Continuum([("192.168.10.1:1234", 60)])
          or
          Continuum([(("192.168.10.1", 11211"), 60),
                      ("192.168.10.2", 11211"), 50)])

        """

        # The nodes added, and to keep track of vpoints
        self.nodes = {}
        self.continuum = []

        for node, vpoints in nodes:
            self.add_node(node, vpoints, False)

        # Force a rebuild of the hash ring
        self._rebuild()

    def _rebuild(self):
        lookup = {}

        for node, vpoints in self.nodes.iteritems():
            for vpoint in xrange(vpoints):
                key = "%s-%d" % (node, vpoint)
                point = self._point_from_key(key)
                lookup[point] = node

        self.continuum = sorted(lookup.items())

    def add_node(self, node, vpoints, rebuild=True):
        """
        Add a node to the consistent hash ring

        @param node: A representation of your node
        @param vpoints: The amount of virtual servers to add

        Example:
          continuum.add_node("127.0.0.4:11211", 3)
          or
          continuum.add_node(("127.0.0.4", "purple"), 3)
        """

        if self.nodes.has_key(node):
            logging.warn("Node %s has already been added, skipping." % node)
            return

        # Store the amount of virtual points this server has for later removal
        self.nodes[node] = vpoints


        logging.debug("Added node %s" % str(node))
        if rebuild:
            self._rebuild()

    def remove_node(self, node):
        """
        Remove a node to the consistent hash ring

        @param node: A string representing your node
        """

        node = str(node)
        vpoints = self.nodes.get(node, None)

        if not vpoints:
            logging.warn("Node %s was not found." % node)
            return

        del self.nodes[node]
        logging.debug("Removed node %s" % str(node))

        self._rebuild()

    def get_node(self, key):
        point = self._point_from_key(key)

        # Find the index for this node
        index = bisect.bisect_right(self.continuum, (point, ()))
        if index < len(self.continuum):
            return self.continuum[index]
        else:
            return self.continuum[0]

    def _point_from_key(self, key):
        tmp = hashlib.md5(str(key)).hexdigest()
        # Only keep 8 bytes
        return long(tmp[6:8] + tmp[4:6] + tmp[2:4] + tmp[0:2], 16)
