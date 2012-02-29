from agamemnon.cassandra import CassandraDataStore
from agamemnon.memory import InMemoryDataStore
from agamemnon.elasticsearch import FullTextSearch
from agamemnon.exceptions import ElasticSearchDisabled
import pycassa
import json


class Delegate(object):
    def __init__(self,settings,prefix,es_server):
        if settings["%skeyspace" % prefix] == 'memory':
            self.d = InMemoryDataStore()
        else:
            self.d = CassandraDataStore(settings['%skeyspace' % prefix],
                                        pycassa.connect(settings["%skeyspace" % prefix],
                                                        json.loads(settings["%shost_list" % prefix])),
                                        system_manager=pycassa.system_manager.SystemManager(
                                            json.loads(settings["%shost_list" % prefix])[0]))
            if(es_server != 'disable'):
                self.elastic_search = FullTextSearch(es_server)
            else:
                self.elastic_search = None

    def __getattr__(self, item):
        es_functions = ['create_index_wrapped','search_index_wrapped','refresh_index_cache',
                        'populate_index','delete_index','insert_node_into_indices',
                        'remove_node_from_indices','modify_node_in_indices',
                        'get_indices_of_type','populate_index_document','conn']
        if item in es_functions:
            if self.elastic_search != None:
                try:
                    return getattr(self.elastic_search,item)
                except:
                    raise AttributeError(item)
            else:
                raise ElasticSearchDisabled
                pass
        else:
            return getattr(self.d,item)
