
from rdflib.store import Store, NO_STORE, VALID_STORE
from rdflib.plugin import register
from rdflib.namespace import Namespace, split_uri, urldefrag
from rdflib import URIRef, Literal
from agamemnon.factory import load_from_settings
from agamemnon.exceptions import NodeNotFoundException
import pycassa
import json
import uuid
import logging

register('Agamemnon', Store, 
                'agamemnon.rdf_store', 'AgamemnonStore')

log = logging.getLogger(__name__)

class AgamemnonStore(Store):
    """
    An agamemnon based triple store.
    
    This triple store uses agamemnon as the underlying graph representation.
    
    """

    
    def __init__(self, configuration=None, identifier=None, data_store=None):
        super(AgamemnonStore, self).__init__(configuration)
        self.identifier = identifier

        # namespace and prefix indexes
        self.__namespace = {}
        self.__prefix = {}
        self.node_namespace_base = "https://github.com/globusonline/agamemnon/nodes/"
        self.relationship_namespace_base = "https://github.com/globusonline/agamemnon/rels/"

        if configuration:
            self._process_config(configuration)

        if data_store:
            self.data_store = data_store


        self._ignored_node_types = set(['reference'])

    def open(self, configuration=None, create=False, repl_factor = 1):
        if configuration:
            self._process_config(configuration)

        keyspace = self.configuration['agamemnon.keyspace']
        if create and keyspace != "memory":
            hostlist = json.loads(self.configuration['agamemnon.host_list'])
            system_manager = pycassa.SystemManager(hostlist[0])
            try:
                log.info("Attempting to drop keyspace")
                system_manager.drop_keyspace(keyspace)
            except pycassa.cassandra.ttypes.InvalidRequestException:
                log.warn("Keyspace didn't exist")
            finally:
                log.info("Creating keyspace")
                system_manager.create_keyspace(keyspace, replication_factor=repl_factor)

        self.data_store = load_from_settings(self.configuration)
        return VALID_STORE

    @property
    def data_store(self):
        return self._ds

    @data_store.setter
    def data_store(self, ds):
        self._ds = ds
        #self.load_namespaces()

    @property
    def ignore_reference_nodes(self):
        return "reference" in self._ignored_node_types

    @ignore_reference_nodes.setter
    def ignore_reference_nodes(self, value):
        if value:
            self.ignore('reference')
        else:
            self.unignore('reference')

    @property
    def node_namespace_base(self):
        return self._node_namespace_base

    @node_namespace_base.setter
    def node_namespace_base(self, value):
        self._node_namespace_base = Namespace(value)

    @property
    def relationship_namespace_base(self):
        return self._relationship_namespace_base

    @relationship_namespace_base.setter
    def relationship_namespace_base(self, value):
        self._relationship_namespace_base = Namespace(value)
        # we need this bound as the default namespace
        self.bind("", self._relationship_namespace_base)
    
    def ignore(self, node_type):
        self._ignored_node_types.add(node_type)

    def unignore(self, node_type):
        self._ignored_node_types.remove(node_type)

    def _process_config(self, configuration):
        self.configuration = configuration
        config_prefix = "agamemnon.rdf_"
        for key, value in configuration.items():
            if key.startswith(config_prefix):
                setattr(self, key[len(config_prefix):], value)

    def add(self, (subject, predicate, object), context, quoted=False):
        if isinstance(subject, Literal):
            raise TypeError("Subject can't be literal")

        if isinstance(predicate, Literal):
            raise TypeError("Predicate can't be literal")

        p_rel_type = self.uri_to_rel_type(predicate) 
        s_node = self.uri_to_node(subject, True)

        #inline literals as attributes
        if isinstance(object, Literal):
            log.debug("Setting %r on %r" % (p_rel_type, s_node))
            s_node[p_rel_type] = object.toPython()
            s_node.commit()
        else:
            o_node = self.uri_to_node(object, True)

            log.debug("Creating relationship of type %s from %s on %s" % (p_rel_type, s_node, o_node))
            self.data_store.create_relationship(str(p_rel_type), s_node, o_node)

    def remove(self, triple, context=None):
        for (subject, predicate, object), c in self.triples(triple):
            log.debug("start delete")
            s_node = self.uri_to_node(subject)
            p_rel_type = self.uri_to_rel_type(predicate)
            if isinstance(object, Literal):
                if p_rel_type in s_node.attributes:
                    if s_node[p_rel_type] == object.toPython():
                        del s_node[p_rel_type]
                        s_node.commit()
            else:
                o_node_type, o_node_id = self.uri_to_node_def(object) 
                if o_node_type in self._ignored_node_types: return
                for rel in getattr(s_node, p_rel_type).relationships_with(o_node_id):
                    if rel.target_node.type == o_node_type:
                        rel.delete()

    def triples(self, (subject, predicate, object), context=None):
        log.debug("Looking for triple %s, %s, %s" % (subject, predicate, object))
        if isinstance(subject, Literal) or isinstance(predicate, Literal):
            # subject and predicate can't be literal silly rabbit
            return 

        # Determine what mechanism to use to do lookup
        try:
            if predicate is not None:
                if subject is not None:
                    if object is not None:
                        triples = self._triples_by_spo(subject, predicate, object)
                    else:
                        triples = self._triples_by_sp(subject, predicate)
                else:
                    if object is not None:
                        triples = self._triples_by_po(predicate, object)
                    else:
                        triples = self._triples_by_p(predicate)
            else:
                if subject is not None:
                    if object is not None:
                        triples = self._triples_by_so(subject, object)
                    else:
                        triples = self._triples_by_s(subject)
                else:
                    if object is not None:
                        triples = self._triples_by_o(object)
                    else:
                        triples = self._all_triples()

            for triple in triples:
                yield triple, None
        except NodeNotFoundException:
            # exit generator as we found no triples
            log.debug("Failed to find any triples.")
            return

    def _triples_by_spo(self, subject, predicate, object):
        log.debug("Finding triple by spo")
        p_rel_type = self.uri_to_rel_type(predicate) 
        s_node = self.uri_to_node(subject)
        if s_node.type in self._ignored_node_types: return
        if isinstance(object, Literal):
            if p_rel_type in s_node.attributes:
                if s_node[p_rel_type] == object.toPython():
                    log.debug("Found %s, %s, %s" % (subject, predicate, object))
                    yield subject, predicate, object
        else:
            o_node_type, o_node_id = self.uri_to_node_def(object) 
            if o_node_type in self._ignored_node_types: return
            for rel in getattr(s_node, p_rel_type).relationships_with(o_node_id):
                if rel.target_node.type == o_node_type:
                    log.debug("Found %s, %s, %s" % (subject, predicate, object))
                    yield subject, predicate, object

    def _triples_by_sp(self, subject, predicate):
        log.debug("Finding triple by sp")
        p_rel_type = self.uri_to_rel_type(predicate) 
        s_node = self.uri_to_node(subject) 
        if s_node.type in self._ignored_node_types: return
        for rel in getattr(s_node, p_rel_type).outgoing:
            object = self.node_to_uri(rel.target_node)
            log.debug("Found %s, %s, %s" % (subject, predicate, object))
            yield subject, predicate, object

        if p_rel_type in s_node.attributes:
            object = Literal(s_node[p_rel_type])
            log.debug("Found %s, %s, %s" % (subject, predicate, object))
            yield subject, predicate, object

    def _triples_by_po(self, predicate, object):
        log.debug("Finding triple by po")
        p_rel_type = self.uri_to_rel_type(predicate) 
        if isinstance(object, Literal):
            log.warn("Your query requires full graph traversal do to Agamemnon datastructure.")
            for s_node in self._all_nodes():
                subject = self.node_to_uri(s_node)
                if p_rel_type in s_node.attributes:
                    if s_node[p_rel_type] == object.toPython():
                        log.debug("Found %s, %s, %s" % (subject, predicate, object))
                        yield subject, predicate, object
        else:
            o_node = self.uri_to_node(object) 
            for rel in getattr(o_node, p_rel_type).incoming:
                subject = self.node_to_uri(rel.source_node)
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield subject, predicate, object

    def _triples_by_so(self, subject, object):
        log.debug("Finding triple by so.")
        s_node = self.uri_to_node(subject) 
        if s_node.type in self._ignored_node_types: return
        if isinstance(object, Literal):
            for p_rel_type in s_node.attributes.keys():
                if p_rel_type.startswith("__"): continue #ignore special names
                if s_node[p_rel_type] == object.toPython():
                    predicate = self.rel_type_to_uri(p_rel_type)
                    log.debug("Found %s, %s, %r" % (subject, predicate, object))
                    yield subject, predicate, object
        else:
            o_node = self.uri_to_node(object) 
            if o_node.type in self._ignored_node_types: return
            for rel in s_node.relationships.outgoing:
                if rel.target_node == o_node:
                    predicate = self.rel_type_to_uri(rel.type)
                    log.debug("Found %s, %s, %s" % (subject, predicate, object))
                    yield subject, predicate, object

    def _triples_by_s(self, subject):
        log.debug("Finding triple by s")
        s_node = self.uri_to_node(subject) 
        if s_node.type in self._ignored_node_types: return
        for rel in s_node.relationships.outgoing:
            if rel.target_node.type in self._ignored_node_types: continue
            predicate = self.rel_type_to_uri(rel.type)
            object = self.node_to_uri(rel.target_node)
            log.debug("Found %s, %s, %s" % (subject, predicate, object))
            yield subject, predicate, object

        for p_rel_type in s_node.attributes.keys():
            if p_rel_type.startswith("__"): continue #ignore special names
            predicate = self.rel_type_to_uri(p_rel_type)
            object = Literal(s_node[p_rel_type])
            log.debug("Found %s, %s, %r" % (subject, predicate, object))
            yield subject, predicate, object

    def _triples_by_p(self, predicate):
        log.debug("Finding triple by p")
        log.warn("Your query requires full graph traversal do to Agamemnon datastructure.")

        p_rel_type = self.uri_to_rel_type(predicate) 
        for s_node in self._all_nodes():
            if s_node.type in self._ignored_node_types: continue
            subject = self.node_to_uri(s_node)
            for rel in getattr(s_node, p_rel_type).outgoing:
                if rel.target_node.type in self._ignored_node_types: continue
                object = self.node_to_uri(rel.target_node)
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield subject, predicate, object

            if p_rel_type in s_node.attributes:
                object = Literal(s_node[p_rel_type])
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield (subject, predicate, object ), None

    def _triples_by_o(self, object):
        log.debug("Finding triple by o")
        if isinstance(object, Literal):
            log.warn("Your query requires full graph traversal do to Agamemnon datastructure.")
            for s_node in self._all_nodes():
                if s_node.type in self._ignored_node_types: continue
                subject = self.node_to_uri(s_node)
                for p_rel_type in s_node.attributes.keys():
                    if p_rel_type.startswith("__"): continue #ignore special names
                    if s_node[p_rel_type] == object.toPython():
                        predicate = self.rel_type_to_uri(p_rel_type)
                        log.debug("Found %s, %s, %s" % (subject, predicate, object))
                        yield subject, predicate, object

        else:
            o_node = self.uri_to_node(object) 
            for rel in o_node.relationships.incoming:
                if rel.source_node.type in self._ignored_node_types: continue
                predicate = self.rel_type_to_uri(rel.type)
                subject = self.node_to_uri(rel.source_node)
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield subject, predicate, object

    def _all_triples(self):
        log.debug("Finding all triples.")
        log.warn("Your query requires full graph traversal do to Agamemnon datastructure.")
        for s_node in self._all_nodes():
            if s_node.type in self._ignored_node_types: continue
            subject = self.node_to_uri(s_node)
            for rel in s_node.relationships.outgoing:
                if rel.target_node.type in self._ignored_node_types: continue
                predicate = self.rel_type_to_uri(rel.type)
                object = self.node_to_uri(rel.target_node)
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield subject, predicate, object

            for p_rel_type in s_node.attributes.keys():
                if p_rel_type.startswith("__"): continue #ignore special names
                predicate = self.rel_type_to_uri(p_rel_type)
                object = Literal(s_node[p_rel_type])
                log.debug("Found %s, %s, %s" % (subject, predicate, object))
                yield subject, predicate, object

    def _all_nodes(self):
        ref_ref_node = self.data_store.get_reference_node()
        for ref in ref_ref_node.instance.outgoing:
            if ref.target_node.key in self._ignored_node_types: continue
            for instance in ref.target_node.instance.outgoing:
                yield instance.target_node

    def node_to_uri(self, node):
        ns = self.namespace(node.type)
        if ns is None:
            ns = Namespace(self.node_namespace_base[node.type + "#"])
            self.bind(node.type, ns)
        uri = ns[node.key]
        log.debug("Converted node %s to uri %s" % (node, uri))
        return uri

    def uri_to_node(self, uri, create=False):
        node_type, node_id = self.uri_to_node_def(uri)
        try:
            log.debug("Looking up node: %s => %s" % (node_type,node_id))
            return self.data_store.get_node(node_type, node_id)
        except NodeNotFoundException:
            if create:
                node = self.data_store.create_node(node_type, node_id)
                log.debug("Created node: %s" % node)
            else:
                raise
            return node

    def uri_to_node_def(self, uri):
        if "#" in uri:
            # if we have a fragment, we will split there
            namespace, node_id = urldefrag(uri)
            namespace += "#"
        else:
            # we make a best guess using split_uri logic
            namespace, node_id = split_uri(uri)

        node_type = self.prefix(namespace)
        if node_type is None:
            node_type = str(uuid.uuid1()).replace("-","_")
            self.bind(node_type, namespace)
        return node_type, node_id

    def rel_type_to_uri(self, rel_type):
        if ":" in rel_type:
            prefix, suffix = rel_type.split(":",1)
            namespace = self.namespace(prefix)
            if namespace:
                uri = namespace[suffix]
            else:
                uri = URIRef(rel_type)
        else:
            uri = self.relationship_namespace_base[rel_type]

        return uri

    def uri_to_rel_type(self, uri):
        namespace, rel_type = split_uri(uri)
        prefix = self.prefix(namespace)
        if prefix is None:
            prefix = str(uuid.uuid1()).replace("-","_")
            self.bind(prefix, rel_type)
        
        if prefix != "":
            rel_type = ":".join((prefix,rel_type))
        
        return rel_type.encode('utf-8')

    def bind(self, prefix, namespace):
        self.__prefix[Namespace(namespace)] = unicode(prefix)
        self.__namespace[prefix] = Namespace(namespace)


    def namespace(self, prefix):
        return self.__namespace.get(prefix, None)

    def prefix(self, namespace):
        return self.__prefix.get(Namespace(namespace), None)

    def namespaces(self):
        for prefix, namespace in self.__namespace.iteritems():
            yield prefix, namespace

    def __contexts(self):
        return (c for c in []) # TODO: best way to return empty generator

    def __len__(self, context=None):
        return len(list(self._all_triples()))

