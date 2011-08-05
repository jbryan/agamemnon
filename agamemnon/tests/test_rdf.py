import unittest
import time

from tempfile import mkdtemp

from rdflib.term import URIRef, BNode, Literal
from rdflib.namespace import RDF, Namespace
from rdflib.graph import Graph

import rdflib.plugin

from agamemnon.rdf_store import AgamemnonStore
from nose.plugins.attrib import attr

import uuid

import logging
log = logging.getLogger(__name__)

class GraphTestCase(unittest.TestCase):
    store_name = 'Agamemnon'
    settings1 = {
        'agamemnon.keyspace' : 'testagamemnon1',
        #'agamemnon.keyspace' : 'memory',
        'agamemnon.host_list' : '["localhost:9160"]',
        'agamemnon.rdf_node_namespace_base' : 'http://www.example.org/',
        'agamemnon.rdf_relationship_namespace_base' : 'http://www.example.org/rels/',
    }
    settings2 = {
        'agamemnon.keyspace' : 'testagamemnon2',
        #'agamemnon.keyspace' : 'memory',
        'agamemnon.host_list' : '["localhost:9160"]',
        'agamemnon.rdf_node_namespace_base' : 'http://www.example.org/',
        'agamemnon.rdf_relationship_namespace_base' : 'http://www.example.org/rels/',
    }

    def setUp(self):
        self.graph1 = Graph(store=self.store_name)
        self.graph2 = Graph(store=self.store_name)

        self.graph1.open(self.settings1, True)
        self.graph2.open(self.settings2, True)
        self.oNS = Namespace("http://www.example.org/things#")
        self.sNS = Namespace("http://www.example.org/people#")
        self.pNS = Namespace("http://www.example.org/relations/")

        self.graph1.bind('people',self.sNS)
        self.graph1.bind('relations',self.pNS)
        self.graph1.bind('things',self.oNS)
        self.graph2.bind('people',self.sNS)
        self.graph2.bind('relations',self.pNS)
        self.graph2.bind('things',self.oNS)

        self.michel = self.sNS.michel
        self.tarek = self.sNS.tarek
        self.alice = self.sNS.alice
        self.bob = self.sNS.bob
        self.likes = self.pNS.likes
        self.hates = self.pNS.hates
        self.named = self.pNS.named
        self.pizza = self.oNS.pizza
        self.cheese = self.oNS.cheese

    def tearDown(self):
        self.graph1.close()

    def addStuff(self,graph):
        graph.add((self.tarek, self.likes, self.pizza))
        graph.add((self.tarek, self.likes, self.cheese))
        graph.add((self.michel, self.likes, self.pizza))
        graph.add((self.michel, self.likes, self.cheese))
        graph.add((self.bob, self.likes, self.cheese))
        graph.add((self.bob, self.hates, self.pizza))
        graph.add((self.bob, self.hates, self.michel)) # gasp!
        graph.add((self.bob, self.named, Literal("Bob")))

    def removeStuff(self,graph):
        graph.remove((self.tarek, self.likes, self.pizza))
        graph.remove((self.tarek, self.likes, self.cheese))
        graph.remove((self.michel, self.likes, self.pizza))
        graph.remove((self.michel, self.likes, self.cheese))
        graph.remove((self.bob, self.likes, self.cheese))
        graph.remove((self.bob, self.hates, self.pizza))
        graph.remove((self.bob, self.hates, self.michel)) # gasp!
        graph.remove((self.bob, self.named, Literal("Bob")))

    def testRelationshipToUri(self):
        uri = self.graph1.store.rel_type_to_uri('likes')
        self.assertEqual(uri, URIRef("http://www.example.org/rels/likes"))

        uri = self.graph1.store.rel_type_to_uri('emotions:likes')
        self.assertEqual(uri, URIRef("emotions:likes"))

        self.graph1.bind('emotions','http://www.emo.org/')
        uri = self.graph1.store.rel_type_to_uri('emotions:likes')
        self.assertEqual(uri, URIRef("http://www.emo.org/likes"))

    def testNodeToUri(self):
        node = self.graph1.store._ds.create_node('blah', 'bleh')
        uri = self.graph1.store.node_to_uri(node)
        self.assertEqual(uri, URIRef("http://www.example.org/blah#bleh"))

        self.graph1.bind("bibble", "http://www.bibble.com/rdf/bibble#")
        node = self.graph1.store._ds.create_node('bibble', 'babble')
        uri = self.graph1.store.node_to_uri(node)
        self.assertEqual(uri, URIRef("http://www.bibble.com/rdf/bibble#babble"))

    def testUriToRelationship(self):
        rel_type = self.graph1.store.uri_to_rel_type(URIRef("http://www.example.org/rels/likes"))
        self.assertEqual(rel_type, 'likes')

        rel_type = self.graph1.store.uri_to_rel_type(URIRef('emotions:likes'))
        prefix, rel_type = rel_type.split(":",1)
        uuid.UUID(prefix.replace("_","-"))
        self.assertEqual(rel_type, "likes")

        self.graph1.bind('emotions','http://www.emo.org/')
        rel_type = self.graph1.store.uri_to_rel_type(URIRef("http://www.emo.org/likes"))
        self.assertEqual(rel_type, 'emotions:likes')
        

    def testUriToNode(self):
        #test unbound uri
        uri = URIRef("http://www.example.org/blah#bleh")
        node = self.graph1.store.uri_to_node(uri, True)
        uuid.UUID(node.type.replace("_","-"))
        self.assertEqual(node.key, "bleh")

        # teset bound uri
        self.graph1.bind("bibble", "http://www.bibble.com/rdf/bibble#")
        uri = URIRef("http://www.bibble.com/rdf/bibble#babble")
        node = self.graph1.store.uri_to_node(uri, True)
        self.assertEqual(node.type, "bibble")
        self.assertEqual(node.key, "babble")

    def testAdd(self):
        self.addStuff(self.graph1)

    def testRemove(self):
        self.addStuff(self.graph1)
        self.removeStuff(self.graph1)

    def testTriples(self):
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
        asserte = self.assertEquals
        triples = self.graph1.triples
        named = self.named
        Any = None

        self.addStuff(self.graph1)

        # unbound subjects
        asserte(len(list(triples((Any, likes, pizza)))), 2)
        asserte(len(list(triples((Any, hates, pizza)))), 1)
        asserte(len(list(triples((Any, likes, cheese)))), 3)
        asserte(len(list(triples((Any, hates, cheese)))), 0)
        asserte(len(list(triples((Any, named, Literal("Bob"))))), 1)

        # unbound objects
        asserte(len(list(triples((michel, likes, Any)))), 2)
        asserte(len(list(triples((tarek, likes, Any)))), 2)
        asserte(len(list(triples((bob, hates, Any)))), 2)
        asserte(len(list(triples((bob, likes, Any)))), 1)

        # unbound predicates
        asserte(len(list(triples((michel, Any, cheese)))), 1)
        asserte(len(list(triples((tarek, Any, cheese)))), 1)
        asserte(len(list(triples((bob, Any, pizza)))), 1)
        asserte(len(list(triples((bob, Any, michel)))), 1)

        # unbound subject, objects
        asserte(len(list(triples((Any, hates, Any)))), 2)
        asserte(len(list(triples((Any, likes, Any)))), 5)

        # unbound predicates, objects
        asserte(len(list(triples((michel, Any, Any)))), 2)
        asserte(len(list(triples((bob, Any, Any)))), 4)
        asserte(len(list(triples((tarek, Any, Any)))), 2)

        # unbound subjects, predicates
        asserte(len(list(triples((Any, Any, pizza)))), 3)
        asserte(len(list(triples((Any, Any, cheese)))), 3)
        asserte(len(list(triples((Any, Any, michel)))), 1)

        # all unbound
        asserte(len(list(triples((Any, Any, Any)))), 8)
        self.removeStuff(self.graph1)
        asserte(len(list(triples((Any, Any, Any)))), 0)


    #def testStatementNode(self):
        #graph = self.graph1

        #from rdflib.term import Statement
        #c = URIRef("http://example.org/foo#c")
        #r = URIRef("http://example.org/foo#r")
        #s = Statement((self.michel, self.likes, self.pizza), c)
        #graph.add((s, RDF.value, r))
        #self.assertEquals(r, graph.value(s, RDF.value))
        #self.assertEquals(s, graph.value(predicate=RDF.value, object=r))

    #def testGraphValue(self):
        #from rdflib.graph import GraphValue

        #graph = self.graph1

        #g1 = Graph(store=self.store_name)
        #g1.open(self.settings1, True)
        #g1.add((self.alice, RDF.value, self.pizza))
        #g1.add((self.bob, RDF.value, self.cheese))
        #g1.add((self.bob, RDF.value, self.pizza))

        #g2 = Graph(store=self.store_name)
        #g2.open(self.settings2, True)
        #g2.add((self.bob, RDF.value, self.pizza))
        #g2.add((self.bob, RDF.value, self.cheese))
        #g2.add((self.alice, RDF.value, self.pizza))

        #gv1 = GraphValue(store=graph.store, graph=g1)
        #gv2 = GraphValue(store=graph.store, graph=g2)
        #graph.add((gv1, RDF.value, gv2))
        #v = graph.value(gv1)
        ##print type(v)
        #self.assertEquals(gv2, v)
        ##print list(gv2)
        ##print gv2.identifier
        #graph.remove((gv1, RDF.value, gv2))

    def testConnected(self):
        graph = self.graph1
        self.addStuff(self.graph1)
        self.assertEquals(True, graph.connected())

        jeroen = self.sNS.jeroen
        unconnected = self.oNS.unconnected

        graph.add((jeroen,self.likes,unconnected))

        self.assertEquals(False, graph.connected())

        # if we don't ignore reference nodes, the graph should be connected
        graph.store.ignore_reference_nodes = False

        self.assertEquals(True, graph.connected())


    def testSub(self):
        g1=self.graph1
        g2=self.graph2

        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
       
        g1.add((tarek, likes, pizza))
        g1.add((bob, likes, cheese))

        g2.add((bob, likes, cheese))

        g3=g1-g2

        self.assertEquals(len(g3), 1)
        self.assertEquals((tarek, likes, pizza) in g3, True)
        self.assertEquals((tarek, likes, cheese) in g3, False)

        self.assertEquals((bob, likes, cheese) in g3, False)

        g1-=g2

        self.assertEquals(len(g1), 1)
        self.assertEquals((tarek, likes, pizza) in g1, True)
        self.assertEquals((tarek, likes, cheese) in g1, False)

        self.assertEquals((bob, likes, cheese) in g1, False)

    def testGraphAdd(self):
        g1=self.graph1
        g2=self.graph2

        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
       
        g1.add((tarek, likes, pizza))

        g2.add((bob, likes, cheese))

        g3=g1+g2

        self.assertEquals(len(g3), 2)
        self.assertEquals((tarek, likes, pizza) in g3, True)
        self.assertEquals((tarek, likes, cheese) in g3, False)

        self.assertEquals((bob, likes, cheese) in g3, True)

        g1+=g2

        self.assertEquals(len(g1), 2)
        self.assertEquals((tarek, likes, pizza) in g1, True)
        self.assertEquals((tarek, likes, cheese) in g1, False)

        self.assertEquals((bob, likes, cheese) in g1, True)

    def testGraphIntersection(self):
        g1=self.graph1
        g2=self.graph2

        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
       
        g1.add((tarek, likes, pizza))
        g1.add((michel, likes, cheese))

        g2.add((bob, likes, cheese))
        g2.add((michel, likes, cheese))

        g3=g1*g2

        self.assertEquals(len(g3), 1)
        self.assertEquals((tarek, likes, pizza) in g3, False)
        self.assertEquals((tarek, likes, cheese) in g3, False)

        self.assertEquals((bob, likes, cheese) in g3, False)

        self.assertEquals((michel, likes, cheese) in g3, True)

        g1*=g2

        self.assertEquals(len(g1), 1)

        self.assertEquals((tarek, likes, pizza) in g1, False)
        self.assertEquals((tarek, likes, cheese) in g1, False)

        self.assertEquals((bob, likes, cheese) in g1, False)

        self.assertEquals((michel, likes, cheese) in g1, True)

    def testSerialize(self):
        self.addStuff(self.graph1)
        v = self.graph1.serialize()
        log.info(v)
        self.graph2.parse(data=v)

        for triple in self.graph1:
            self.assertTrue(triple in self.graph2)

        for triple in self.graph2:
            self.assertTrue(triple in self.graph1)

        




