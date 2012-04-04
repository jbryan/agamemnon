from agamemnon.elasticsearch import FullTextSearch
from agamemnon.exceptions import PluginDisabled
import pycassa
import json


class Delegate(object):
    def __init__(self):
        self.plugins = []

    def load_plugins(self,plugin_dict):
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
        if not item in self.__dict__:
            for plugin in self.plugins:
                plugin_object_wrapper = self.__dict__[plugin]
                try:
                    attr = getattr(plugin_object_wrapper,item)
                    return attr
                except AttributeError:
                    pass
            raise PluginDisabled

