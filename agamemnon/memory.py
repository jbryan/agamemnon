import operator
from ordereddict import OrderedDict
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.index import LT, LTE, EQ, GTE, GT
from agamemnon.graph_constants import ASCII
import logging

log = logging.getLogger()

class InMemoryDataStore(object):
    def __init__(self):
        self.tables = OrderedDict()
        self.transactions = []
        self.batch_count = 0
        self.in_batch = False

    def get_count(self, type, row, columns=None, column_start=None, super_column=None, column_finish=None):
        return self.get_cf(type).get_count(row, columns=columns, column_start=column_start,
                                           column_finish=column_finish, super_column=super_column)

    def get_cf(self, cf_name):
        if not cf_name in self.tables:
            self.tables[cf_name] = self.create_cf(cf_name)
        return self.tables[cf_name]

    def create_cf(self, type, column_type=ASCII, super=False, index_columns=list()):
        self.tables[type] = ColumnFamily(type, column_type)
        return self.tables[type]
    
    def create_secondary_index(self, type, column, column_type=None):
        # DO NOTHING, for now we just do complete scans since memory is "fast enough"
        pass

    def cf_exists(self, type):
        return type in self.tables.keys()

    def insert(self, cf, row, columns):
        def execute():
            cf.insert(row, columns)

        if self.in_batch:
            self.transactions.append(execute)
        else:
            execute()

    def remove(self, cf, row, columns=None, super_column=None):
        def execute():
            cf.remove(row, columns=columns, super_column=super_column)
            
        if self.in_batch:
            self.transactions.append(execute)
        else:
            execute()

    def start_batch(self, queue_size = 0):
        self.in_batch = True
        self.batch_count += 1

    def commit_batch(self):
        self.batch_count -= 1
        if not self.batch_count:
            for item in self.transactions:
                item()
            self.transactions = []
            self.in_batch = False


class ColumnFamily(object):
    def __init__(self, name, sort, super=False):
        self.data = OrderedDict()
        self.sort = sort
        self.name = name
        self.super = super

    def get_count(self, row, columns=None, column_start=None, super_column=None, column_finish=None):
        count = float("inf")
        results = self.get(row, columns, column_start, super_column, column_finish, count)
        return len(results)

    def multiget(self, row_keys, **kwargs):
        return OrderedDict([
            (row, self.get(row, **kwargs))
            for row in row_keys
        ])
        
    def get(self, row, columns=None, column_start=None, super_column=None, column_finish=None, column_count=100):
        try:
            if super_column is None:
                data_columns = self.data[row]
            else:
                data_columns = self.data[row][super_column]
            results = OrderedDict()
            count = 0
            if columns is not None:
                for c in columns:
                    results[c] = data_columns[c]
            else:
                for c in sorted(data_columns.keys()):
                    if count > column_count:
                        break
                    if column_start and cmp(c,column_start) < 0:
                        continue
                    if column_finish and cmp(c, column_finish) > 0:
                        break

                    results[c] = data_columns[c]
                    count += 1
            if not len(results):
                raise NotFoundException
            for key, value in results.items():
                if isinstance(value, dict) and len(value) == 0:
                    del(results[key])
                if value is None:
                    del(results[key])
            return results
        except KeyError:
            raise NotFoundException

    def insert(self, row, columns, ttl=None):
        if not row in self.data:
            self.data[row] = OrderedDict()
        if self.sort == ASCII:
            sorted_columns = sorted(columns.iteritems(), key=operator.itemgetter(0))
            for column in sorted_columns:
                if column[0] not in self.data[row] or not isinstance(column[1], dict):
                    self.data[row][column[0]] = column[1]
                else:
                    self.data[row][column[0]].update(column[1])

                #        if ttl is not None:
                #            def delete():
                #                for c in columns.keys():
                #                    del(self.data[row][c])
                #            Timer(ttl, delete, ()).start()

    def remove(self, row, columns=None, super_column=None):
        try:
            if columns is None and super_column is None:
                row_data = self.data[row]
                for key, value in row_data.items():
                    if isinstance(value, OrderedDict):
                        value.clear()
                    else:
                        del(row_data[key])
            elif columns is not None and super_column is None:
                row_data = self.data[row]
                for c in columns:
                    if c in row_data:
                        value = row_data[c]
                        if isinstance(value, OrderedDict):
                            value.clear()
                        else:
                            del(row_data[c])
            elif columns is None and super_column is not None:
                for key, value in self.data[row][super_column].items():
                    if isinstance(value, dict):
                        value.clear()
                    else:
                        del(self.data[row][super_column][key])
            else:
                sc = self.data[row][super_column]
                for c in columns:
                    if c in sc:
                        del(sc[c])
        except KeyError:
            raise NotFoundException

    def get_indexed_slices(self, index_clause):
        for i in self.data.items():
            for expression in index_clause.expressions:
                if expression.column_name not in i[1]:
                    break
                value = i[1][expression.column_name]
                comp = {
                    LT: value.__lt__,
                    LTE: value.__le__,
                    EQ: value.__eq__,
                    GTE: value.__ge__,
                    GT: value.__gt__,
                }[expression.op]

                if not comp(expression.value):
                    # break out of the expression loop and try next data item
                    break
            else:
                # passed all expressions, this one is good
                yield i[0], i[1]
  
