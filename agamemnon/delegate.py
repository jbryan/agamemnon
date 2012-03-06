from agamemnon.cassandra import CassandraDataStore
from agamemnon.memory import InMemoryDataStore
from agamemnon.elasticsearch import FullTextSearch
from agamemnon.exceptions import PluginDisabled
import pycassa
import json


class Delegate(object):
    def __init__(self,settings,prefix,plugin_dict):
        if settings["%skeyspace" % prefix] == 'memory':
            self.d = InMemoryDataStore()
        else:
            self.d = CassandraDataStore(settings['%skeyspace' % prefix],
                                        pycassa.connect(settings["%skeyspace" % prefix],
                                                        json.loads(settings["%shost_list" % prefix])),
                                        system_manager=pycassa.system_manager.SystemManager(
                                            json.loads(settings["%shost_list" % prefix])[0]))
            self.plugins = []
            for key,plugin in plugin_dict.items():
                self.__dict__[key]=plugin
                self.plugins.append(key)


    def on_create(self,node):
        for plugin in self.plugins:
            plugin_object = self.__dict__[plugin]
            plugin_object.on_create(node)

    def on_delete(self,node):
        for plugin in self.plugins:
            plugin_object = self.__dict__[plugin]
            plugin_object.on_delete(node)

    def on_modify(self,node):
        for plugin in self.plugins:
            plugin_object = self.__dict__[plugin]
            plugin_object.on_modify(node)

    def __getattr__(self, item):
        try:
            attr = getattr(self.d,item)
            return attr
        except AttributeError:
            for plugin in self.plugins:
                plugin_object_wrapper = self.__dict__[plugin]
                try:
                    attr = getattr(plugin_object_wrapper,item)
                    return attr
                except AttributeError:
                    pass
            raise PluginDisabled

