from pyes.es import ES
from pyes import exceptions
from pyes import query
from itertools import groupby
from operator import itemgetter

class FullTextSearch(object):
    def __init__(self,server,settings = None ):
        self.conn = ES(server)
        self.indices = {}
        if settings:
            self.settings = settings
        else:
            self.settings = { 
                'index': {
                    'analysis' : {
                        'analyzer' : {                             
                            'ngram_analyzer' : {                   
                                'tokenizer' : 'keyword',
                                'filter' : ['lowercase', 'filter_ngram'],
                                'type' : 'custom'
                            }  
                        },
                        'filter' : {
                            'filter_ngram' : {                                 
                                'type' : 'nGram',
                                'max_gram' : 30,
                                'min_gram' : 1                                 
                            }                           
                        }
                    }
                }
            }

    def search_index(self, type, index_names, query_string, num_results=None, fields="_all"):
        q = query.WildcardQuery(fields ,query_string)
        results = self.conn.search(query=q, indices=index_names, doc_types=type)
        type_id_list = [(r['_type'] ,r['_id']) for r in results['hits']['hits'][0:num_results]]
        node_dict = {}

        # fetch nodes grouped by type to reduce number of db calls
        for t, id_list in groupby(sorted(type_id_list), key=itemgetter(0)):
            ids = [n[1] for n in id_list]
            for node in self.datastore.get_nodes(t, ids):
                node_dict[(node.type, node.key)] = node

        # return nodes in original order
        nodelist = [node_dict[key] for key in type_id_list]

        return nodelist

    def create_index(self, type, indexed_variables, index_name):
        self.conn.delete_index_if_exists(index_name)
        self.conn.create_index(index_name,self.settings)
        mapping = {}
        for arg in indexed_variables:
            mapping[arg] = {'boost':1.0,
                            'analyzer' : 'ngram_analyzer',
                            'type': 'string',
                            'term_vector': 'with_positions_offsets'}
        index_settings = {'index_analyzer':'ngram_analyzer',
                          'search_analyzer':'standard',
                          'properties':mapping}
        self.conn.put_mapping(str(type),index_settings,[index_name])
        self.refresh_index_cache()
        self.populate_index(type, index_name)

    def refresh_index_cache(self):
        self.indices = self.conn.get_mapping()

    def delete_index(self,type, index_name):
        self.conn.delete_index_if_exists(index_name)
        self.refresh_index_cache()

    def populate_index(self, type, index_name):
        #add all the currently existing nodes into the index
        ref_node = self.datastore.get_reference_node(type)
        node_list = [rel.target_node for rel in ref_node.instance.outgoing]
        mapping = self.conn.get_mapping(type, index_name)
        for node in node_list:
            key = node.key
            index_dict = self.populate_index_document(type, index_name, node.attributes,mapping)
            try:
                self.conn.delete(index_name,type,key)
            except exceptions.NotFoundException:
                pass
            self.conn.index(index_dict,index_name,type,key)
        self.conn.refresh([index_name])

    def on_create(self,node):
        type_indices = self.get_indices_of_type(node.type)
        for index_name in type_indices:
            mapping = self.conn.get_mapping(node.type,index_name)
            index_dict = self.populate_index_document(node.type,index_name,node.attributes,mapping)
            self.conn.index(index_dict,index_name,node.type,node.key)
            self.conn.refresh([index_name])

    def on_delete(self, node):
        type_indices = self.get_indices_of_type(node.type)
        for index_name in type_indices:
            try:
                self.conn.delete(index_name,node.type,node.key)
                self.conn.refresh([index_name])
            except exceptions.NotFoundException:
                pass
           
    def on_modify(self, node):
        type_indices = self.get_indices_of_type(node.type)
        for index_name in type_indices:
            mapping = self.conn.get_mapping(node.type,index_name)
            index_dict = self.populate_index_document(node.type,index_name,node.attributes,mapping)
            try:
                self.conn.delete(index_name,node.type,node.key)
                self.conn.index(index_dict,index_name,node.type,node.key)
                self.conn.refresh([index_name])
            except exceptions.NotFoundException:
                pass

    def get_indices_of_type(self,type):
        type_indices = [
            key for key, value in self.indices.items()
            if type in value
        ]
        return type_indices

    def populate_index_document(self,type,index_name,attributes,mapping):
        indexed_variables = mapping[type]['properties'].keys()
        index_dict = {}
        for arg in indexed_variables:
            try:
                index_dict[arg] = attributes[arg]
            except KeyError:
                #if this attribute doesn't exist for this node, just pass
                pass
        return index_dict