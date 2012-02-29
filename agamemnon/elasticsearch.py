from pyes.es import ES
from pyes import exceptions
from pyes.query import WildcardQuery
from agamemnon.exceptions import NodeNotFoundException
import string

class FullTextSearch(object):
    def __init__(self,es_server):
        self.conn = ES(es_server)
        self.indices = {}

    def search_index_wrapped(self, type, index_names, query_string, datastore, num_results=-1):
        ns_index_names= [str(type) + "-_-" + index_name for index_name in index_names]
        q = WildcardQuery('_all',query_string)
        results = self.conn.search(query=q, indices=ns_index_names, doc_types=type)
        try:
            nodelist = [datastore.get_node(type,r['_id']) for r in results['hits']['hits'][0:num_results]+[results['hits']['hits'][num_results]]]
        except IndexError:
            nodelist = [datastore.get_node(type,r['_id']) for r in results['hits']['hits'][0:num_results]]
        return nodelist

    def create_index_wrapped(self, type, indexed_variables, index_name, datastore):
        ns_index_name = str(type) + "-_-" + index_name
        self.conn.delete_index_if_exists(ns_index_name)
        settings = { 'index': {
            'analysis' : {
                'analyzer' : {                             
                    'ngram_analyzer' : {                   
                        'tokenizer' : 'standard',
                        'filter' : ['standard', 'filter_ngram'],
                        'type' : 'custom'
                    }  
                },
                'filter' : {
                    'filter_ngram' : {                                 
                    'type' : 'nGram',
                    'max_gram' : 30,
                    'min_gram' : 1                                 
#TODO: delegate -> delegate.d
                    }                           
                }
            }
        }}
        self.conn.create_index(ns_index_name,settings)
        mapping = {}
        for arg in indexed_variables:
            mapping[arg] = {'boost':1.0,
                            'analyzer' : 'ngram_analyzer',
                            'type': u'string',
                            'term_vector': 'with_positions_offsets'}
        index_settings = {'index_analyzer':'ngram_analyzer','search_analyzer':'standard','properties':mapping}
        self.conn.put_mapping(str(type),index_settings,[ns_index_name])
        self.refresh_index_cache()
        self.populate_index(type, index_name, datastore)

    def refresh_index_cache(self):
        self.indices = self.conn.get_indices()

    def delete_index(self,type,index_name):
        ns_index_name = str(type) + "-_-" + index_name
        self.conn.delete_index_if_exists(ns_index_name)
        self.refresh_index_cache()

    def populate_index(self, type, index_name, datastore):
        #add all the currently existing nodes into the index
        ns_index_name = str(type) + "-_-" + index_name
        ref_node = datastore.get_reference_node(type)
        node_list = [rel.target_node for rel in ref_node.instance.outgoing]
        mapping = self.conn.get_mapping(type,ns_index_name)
        for node in node_list:
            key = node.key
            index_dict = self.populate_index_document(type,ns_index_name,node.attributes,mapping)
            try:
                self.conn.delete(ns_index_name,type,key)
            except exceptions.NotFoundException:
                pass
            self.conn.index(index_dict,ns_index_name,type,key)
        self.conn.refresh([ns_index_name])

    def insert_node_into_indices(self,type,node):
        type_indices = self.get_indices_of_type(type)
        for ns_index_name in type_indices:
            mapping = self.conn.get_mapping(type,ns_index_name)
            index_dict = self.populate_index_document(type,ns_index_name,node.attributes,mapping)
            self.conn.index(index_dict,ns_index_name,type,node.key)
            self.conn.refresh([ns_index_name])

    def remove_node_from_indices(self, node):
        type_indices = self.get_indices_of_type(node.type)
        for ns_index_name in type_indices:
            try:
                self.conn.delete(ns_index_name,node.type,node.key)
                self.conn.refresh([ns_index_name])
            except exceptions.NotFoundException:
                pass
           
    #find a given node and modify it in all the indices it is in
    def modify_node_in_indices(self, type, node):
        type_indices = self.get_indices_of_type(type)
        for ns_index_name in type_indices:
            mapping = self.conn.get_mapping(type,ns_index_name)
            index_dict = self.populate_index_document(type,ns_index_name,node.attributes,mapping)
            try:
                self.conn.delete(ns_index_name,type,node.key)
                self.conn.index(index_dict,ns_index_name,type,node.key)
                self.conn.refresh([ns_index_name])
            except exceptions.NotFoundException:
                pass

    def get_indices_of_type(self,type):
        type_indices = []
        for index in self.indices.keys():
            if index.startswith(type+"-_-"):
                type_indices.append(index)
        return type_indices

    def populate_index_document(self,type,ns_index_name,attributes,mapping):
        indexed_variables = mapping[type]['properties'].keys()
        index_dict = {}
        for arg in indexed_variables:
            try:
                index_dict[arg] = attributes[arg]
            except KeyError:
                #if this attribute doesn't exist for this node, just pass
                pass
        return index_dict
