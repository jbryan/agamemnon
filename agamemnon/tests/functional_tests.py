import json
from agamemnon.factory import load_from_settings

#config = {'agamemnon.keyspace':'memory'}
config = {'agamemnon.keyspace':'agamemnontest', 'agamemnon.host_list':'["localhost:9160"]'}
graph_db = load_from_settings(config)
spiderpig = graph_db.create_node('test_type', 'spiderpig', {'sound':'oink'})
spiderpig = graph_db.get_node('test_type', 'spiderpig')
print spiderpig['sound']
harry_plopper = graph_db.create_node('test_type', 'Harry Plopper', {'sound':'plop'})
harry_plopper = graph_db.get_node('test_type','Harry Plopper')
print harry_plopper['sound']
homer = graph_db.create_node('simpson', 'Homer', {'sound':'Doh', 'job':'Safety Technician'})
homer = graph_db.get_node('simpson', 'Homer')
print homer['sound']
print homer['job']
rel = spiderpig.friend(harry_plopper, key='spiderpig_harry_plopper_alliance', alter_ego=True, best=False)
reference_node = graph_db.get_reference_node('test_type')
print sorted([rel.target_node.key for rel in reference_node.instance.outgoing])
friends = [rel for rel in spiderpig.friend]
print len(friends)
print friends[0].target_node.key
print 'spiderpig' in harry_plopper.friend
friends = [rel for rel in harry_plopper.friend]
print len(friends)
print friends[0].source_node.key
print friends[0]['best']
rel = homer.friend(spiderpig, best=True, alter_ego=False, type='love', strength=100)
friends = [rel for rel in spiderpig.friend]
print len(friends)

homer_spiderpig_love = spiderpig.friend.relationships_with('Homer')
print len(homer_spiderpig_love)

print json.dumps(homer_spiderpig_love[0].attributes, indent=4)
#import unittest
#from nose.plugins.attrib import attr
#from agamemnon.factory import load_from_settings
#
#class GraphFunctionalTests(unittest.TestCase):
##This test requires a running cassandra instance at localhost:9160
#
#    @attr(module='graph',type='functional')
#    def test_graph_ops(self):
##        config = {'agamemnon.keyspace':'agamemnontest', 'agamemnon.host_list':'["localhost:9160"]'}
#        config = {'agamemnon.keyspace':'memory'}
#        graph_db = load_from_settings(config)
#        #Create a spiderpig node
#        graph_db.create_node('test_type', 'spiderpig', {'sound':'oink'})
#        #get a spiderpig node (creating does return the node, but this is for testing and illustrative purposes
#        spiderpig = graph_db.get_node('test_type', 'spiderpig')
#        #make sure the attributes were set correctly
#        self.failUnlessEqual(spiderpig['sound'], 'oink')
#        #create a cow node
#        graph_db.create_node('test_type', 'cow', {'sound':'moo'})
#        #get the cow node
#        cow = graph_db.get_node('test_type','cow')
#        #test that the attributes were set correctly
#        self.failUnlessEqual(cow['sound'], 'moo')
#        #create a node of a different type
#        graph_db.create_node('simpson', 'homer', {'sound':'Doh'})
#        homer = graph_db.get_node('simpson', 'homer')
#        self.failUnlessEqual(homer['sound'],'Doh')
#        #create a relationship with a custom key and relationship attributes
#        #this is a friend relationship
#        spiderpig.friend(cow, key='spiderpig_cow_alliance', best=False)
#        #get a reference node.  This is basically an index of all of the nodes of a given type
#        #This returns the reference node for the test type
#        reference_node = graph_db.get_reference_node('test_type')
#        instances = ['spiderpig', 'cow']
#        #Test to make sure that the correct instances are returned
#        for rel in reference_node.instance.outgoing:
#            instances.remove(rel.target_node.key)
#        #Get all of the outgoing friend relationships for spiderpig and make sure that cow is the only one
#        for rel in spiderpig.friend.outgoing:
#            self.failUnlessEqual(rel.key, 'spiderpig_cow_alliance')
#            self.failUnlessEqual(rel.target_node.key,'cow')
#        #Get all of the incoming friend relationships for cow and make sure that spiderpig is the only one
#        self.failUnless('spiderpig' in cow.friend)
#        for rel in cow.friend.incoming:
#            self.failUnlessEqual(rel.key, 'spiderpig_cow_alliance')
#            self.failUnlessEqual(rel.source_node.key, 'spiderpig')
#            self.failIf(rel['best'])
#
#        #Add Homer as a friend of spiderpig
#        spiderpig.friend(homer, 'loves', AKA='Harry Plopper', strength=100)
#        for rel in homer.friend:
#            self.failUnlessEqual(rel.source_node.key, 'spiderpig')
#
#        self.failUnlessEqual(len(spiderpig.friend), 2)
#
#        homer_spiderpig_love = spiderpig.friend.relationships_with('homer')
#        print homer_spiderpig_love[0].attributes
#        self.failUnless('cow' in spiderpig.friend)
#
#        #Make sure that the "single" operation on the relationship type returns only one relationship
#        self.failUnlessEqual(cow.friend.incoming.single.source_node.key, spiderpig.key)
#        self.failUnlessEqual(1, len(spiderpig.friend.relationships_with('cow')))
#        #delete the spiderpig node
#        spiderpig.delete()
#        #make sure that spiderpig is no logner in the cow's friend incoming list
#        self.failIf('spiderpig' in cow.friend)
#        self.failIf('spiderpig' in homer.friend)
