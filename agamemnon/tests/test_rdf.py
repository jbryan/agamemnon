import unittest
from rdflib.term import URIRef, Literal
from rdflib.namespace import  Namespace
from rdflib.graph import Graph
import rdflib.plugin
import uuid
import yaml
from os import path 
from agamemnon import rdf_store

import logging
log = logging.getLogger(__name__)

TEST_CONFIG_FILE = path.join(path.dirname(__file__),'test_config.yml')

class GraphMemoryTestCase(unittest.TestCase):
    store_name = 'Agamemnon'
    settings1 = 'memory_config_1'
    settings2 = 'memory_config_2'

    def setUp(self):
        with open(TEST_CONFIG_FILE) as f:
            settings = yaml.load(f)

        self.graph1 = Graph(store=self.store_name)
        self.graph2 = Graph(store=self.store_name)

        self.graph1.open(settings[self.settings1], True)
        self.graph2.open(settings[self.settings2], True)

        self.oNS = Namespace("http://www.example.org/rdf/things#")
        self.sNS = Namespace("http://www.example.org/rdf/people#")
        self.pNS = Namespace("http://www.example.org/rdf/relations/")

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
        self.graph2.close()
        self.graph1.store.data_store.drop()
        self.graph2.store.data_store.drop()

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
        graph.remove((None, None, None))


    def testBind(self):
        store = self.graph1.store
        self.assertEqual(store.namespace(""), Namespace("http://www.example.org/rdf/"))
        self.assertEqual(store.namespace('people'),self.sNS)
        self.assertEqual(store.namespace('relations'),self.pNS)
        self.assertEqual(store.namespace('things'),self.oNS)
        self.assertEqual(store.namespace('blech'),None)

        self.assertEqual("", store.prefix(Namespace("http://www.example.org/rdf/")))
        self.assertEqual('people',store.prefix(self.sNS))
        self.assertEqual('relations',store.prefix(self.pNS))
        self.assertEqual('things',store.prefix(self.oNS))
        self.assertEqual(None,store.prefix("blech"))

        self.assertEqual(len(list(self.graph1.namespaces())), 7)

    def testRelationshipToUri(self):
        uri = self.graph1.store.rel_type_to_ident('likes')
        self.assertEqual(uri, URIRef("http://www.example.org/rdf/likes"))

        uri = self.graph1.store.rel_type_to_ident('emotions:likes')
        self.assertEqual(uri, URIRef("emotions:likes"))

        self.graph1.bind('emotions','http://www.emo.org/')
        uri = self.graph1.store.rel_type_to_ident('emotions:likes')
        self.assertEqual(uri, URIRef("http://www.emo.org/likes"))

    def testNodeToUri(self):
        node = self.graph1.store._ds.create_node('blah', 'bleh')
        uri = self.graph1.store.node_to_ident(node)
        self.assertEqual(uri, URIRef("http://www.example.org/rdf/blah#bleh"))

        self.graph1.bind("bibble", "http://www.bibble.com/rdf/bibble#")
        node = self.graph1.store._ds.create_node('bibble', 'babble')
        uri = self.graph1.store.node_to_ident(node)
        self.assertEqual(uri, URIRef("http://www.bibble.com/rdf/bibble#babble"))

    def testUriToRelationship(self):
        rel_type = self.graph1.store.ident_to_rel_type(URIRef("http://www.example.org/rdf/likes"))
        self.assertEqual(rel_type, 'likes')

        rel_type = self.graph1.store.ident_to_rel_type(URIRef('emotions:likes'))
        prefix, rel_type = rel_type.split(":",1)
        uuid.UUID(prefix.replace("_","-"))
        self.assertEqual(rel_type, "likes")

        self.graph1.bind('emotions','http://www.emo.org/')
        rel_type = self.graph1.store.ident_to_rel_type(URIRef("http://www.emo.org/likes"))
        self.assertEqual(rel_type, 'emotions:likes')
        

    def testUriToNode(self):
        #test unbound uri
        uri = URIRef("http://www.example.org/rdf/blah#bleh")
        node = self.graph1.store.ident_to_node(uri, True)
        uuid.UUID(node.type.replace("_","-"))
        self.assertEqual(node.key, "bleh")

        #test unbound uri with trailing /
        uri = URIRef("http://www.example.org/blah/bleh/")
        node = self.graph1.store.ident_to_node(uri, True)
        uuid.UUID(node.type.replace("_","-"))
        self.assertEqual(node.key, "bleh/")

        # teset bound uri
        self.graph1.bind("bibble", "http://www.bibble.com/rdf/bibble#")
        uri = URIRef("http://www.bibble.com/rdf/bibble#babble")
        node = self.graph1.store.ident_to_node(uri, True)
        self.assertEqual(node.type, "bibble")
        self.assertEqual(node.key, "babble")

        # make sure if we reference a predicate as a subject or object, we will
        # still be able recover the correct uri
        uri = URIRef("http://www.example.org/rdf/doit")
        node = self.graph1.store.ident_to_node(uri, True)
        self.assertEqual(self.graph1.store.node_to_ident(node), uri)

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
        asserte(len(list(triples((bob, named, Any)))), 1)

        # unbound predicates
        asserte(len(list(triples((michel, Any, cheese)))), 1)
        asserte(len(list(triples((tarek, Any, cheese)))), 1)
        asserte(len(list(triples((bob, Any, pizza)))), 1)
        asserte(len(list(triples((bob, Any, michel)))), 1)
        asserte(len(list(triples((bob, Any, Literal("Bob"))))), 1)

        # unbound subject, objects
        asserte(len(list(triples((Any, hates, Any)))), 2)
        asserte(len(list(triples((Any, likes, Any)))), 5)
        asserte(len(list(triples((Any, named, Any)))), 1)

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
        #r = Literal("blah")
        #s = Statement((self.michel, self.likes, self.pizza), None)
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

        # sanity check that we are ignoring reference nodes
        self.assertTrue(graph.store.ignore_reference_nodes)
        # if we don't ignore reference nodes, the graph should be connected
        graph.store.ignore_reference_nodes = False

        self.assertEquals(True, graph.connected())


    def testSub(self):
        g1=self.graph1
        g2=self.graph2

        tarek = self.tarek
        bob = self.bob
        likes = self.likes
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
        bob = self.bob
        likes = self.likes
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
        node = self.graph1.store.ident_to_node(self.pizza, True)
        log.info("Pizza Attr: %s" %node.attributes)


        parse_serial_pairs = [
            ('xml','xml'),
            # we will add more once we support context aware graphs
        ]
        for parse, serial in parse_serial_pairs:
            self.addStuff(self.graph1)

            v = self.graph1.serialize(format=serial)
            self.graph2.parse(data=v,format=parse)


            for triple in self.graph1:
                self.assertTrue(triple in self.graph2)

            for triple in self.graph2:
                self.assertTrue(triple in self.graph1)

            self.graph1.remove((None,None,None))
            self.graph2.remove((None,None,None))


    def testQuery(self):
        # parse from string
        # borrowed from http://en.wikipedia.org/wiki/Resource_Description_Framework
        rdf = """
            <rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:foaf="http://xmlns.com/foaf/0.1/" 
            xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <rdf:Description rdf:about="http://en.wikipedia.org/wiki/Tony_Benn">
                            <dc:title>Tony Benn</dc:title>
                            <dc:publisher>Wikipedia</dc:publisher>
                            <foaf:primaryTopic>
                                <foaf:Person>
                                    <foaf:name>Tony Benn</foaf:name>  
                                </foaf:Person>
                            </foaf:primaryTopic>
                    </rdf:Description>
            </rdf:RDF>
        """

        self.graph1.parse(data=rdf)
        rdflib.plugin.register('sparql', rdflib.query.Processor,
                        'rdfextras.sparql.processor', 'Processor')
        rdflib.plugin.register('sparql', rdflib.query.Result,
                        'rdfextras.sparql.query', 'SPARQLQueryResult')
        rows = self.graph1.query(
            """
            SELECT ?a WHERE 
            { ?a foaf:primaryTopic ?b . ?b foaf:name "Tony Benn" }
            """,
            initNs=dict(self.graph1.namespaces())
        )

        self.assertEqual(len(rows), 1)

    def testParse(self):
        # examples from w3c

        # TODO: this fails because of query string in url
        #self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/amp-in-url/test001.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/datatypes/test001.rdf")
        # TODO: this fails due to type parsing, probably an rdflib problem
        #self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/datatypes/test002.rdf")

        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdf-element-not-mandatory/test001.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-reification-required/test001.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-uri-substructure/test001.rdf")

        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test001.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test002.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test003.rdf")

        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test004.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test005.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/rdfms-xmllang/test006.rdf")

        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/unrecognised-xml-attributes/test001.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/unrecognised-xml-attributes/test002.rdf")
        self.graph1.parse("http://www.w3.org/2000/10/rdf-tests/rdfcore/xml-canon/test001.rdf")

        #additional examples for the fun of it
        self.graph1.parse("http://bigasterisk.com/foaf.rdf")
        self.graph1.parse("http://www.w3.org/People/Berners-Lee/card.rdf")
        #self.graph1.parse("http://danbri.livejournal.com/data/foaf") 

        self.graph1.serialize("serialized.rdf")


class GraphCassandraTestCase(GraphMemoryTestCase):
    settings1 = 'cassandra_config_1'
    settings2 = 'cassandra_config_2'


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(GraphMemoryTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

