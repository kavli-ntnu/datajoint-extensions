import inspect
import datajoint as dj
import re


# =========== key-source filler =============


class KeySourceFiller:
    """
    An extension to the datajoint Schema object

    The KeySourceFiller modifies how Autopopulate works. Typically, Autopopulate
    works by computing the key_source, and then comparing the existing table to
    that key_source. However, for tables with complex key_sources, this can
    become a considerable, unnecessary burden on the database server.

    Instead, a table that is decorated with KeySourceFiller defines a new, permanent,
    table, that acts as the key source, and a worker must be assigned to populate
    that table. When Autopopulate runs, the database must still compare the
    key_source table to the existing table, but does not have to construct a
    potentially complex temporary key_source table *first*

    This is a performance gain in that the complex key_source construction need
    only happen *once*, by a single worker, instead of being repeated by _every_
    worker touching that table. However, it consumes storage space, and may require
    logic changes

    STANDARD TABLE DEFINITIONS:
        ```python
        schema = dj.schema("schema_name")
        @schema
        class MyTable(dj.Imported):
            definition = '''...'''
            key_source = MyOtherTable * MyOtherOtherTable ...
            def make(self, key):
                ...
        ```

    KEY SOURCE FILLER EXAMPLE
        ```python
        schema = dj.schema("schema_name")
        ksf = KeySourceFiller(schema)
        @ksf
        class MyTable(dj.Imported):
            definition = '''...'''
            key_source = MyOtherTable * MyOtherOtherTable ...
            def make(self, key):
                ...

        # somewhere else:
        MyTable.key_source.populate()
        # OR
        # ksf.populate_all()  # to process all related key_sources at once.
        ```

    INFORMATION OF WHICH TO BE AWARE

    Currently, the key_source -WILL NOT- be touched when the main table is dropped
    or deleted. Therefore, if dropping a table, be sure to also purge the key_source
    table as well:
        ```python
        MyTable.drop()
        MyTable.key_source.drop()
        ```

    However, the key_source table does use foreign keys to its parents, and therefore
    when rows in its parent tables are dropped; or the parent table itself is dropped,
    the keys in the key_source will be removed

    Because this key_source table is NOT ephemeral, additional care may need to be taken
    with key_source logic. For instance, consider a table that behaves like this:
        ```python
        class MyTable(dj.Computed):
            @property
            def key_source(self):
                return TableA - TableB

            def make(self, key):
                ....
                if condition:
                    TableB.insert1(key)
                else:
                    self.insert1(key)
        ```
    i.e. the make() function (that happens _after_ key_source populate) itself modifies
    the tables that are considered in the key_source
    In this case, the persistent nature of the key_source table becomes relevant, because
    the key that is "diverted" into TableB remains in the key_source of MyTable. It must be
    manually deleted from the key_source, like so:
        ```python
        ...
        def make(self, key):
            ....
            if condition:
                TableB.insert1(key)
                with dj.config(safemode=False):
                    (self.key_source & key).delete()
            else:
                self.insert1(key)
        ```

    Written by Thinh Nguyen at Vathes LLC
    Improved by Simon Ball at NTNU
    """

    def __init__(self, schema):
        self._schema = schema
        self.tables = {}

    def populate_all(self, **populate_args):
        for k, v in self.tables.items():
            print('Populating {}'.format(k))
            v.populate(**populate_args)

    def __call__(self, cls):
        context = inspect.currentframe().f_back.f_locals
        cls = self._schema(cls, context=context)
        name = cls.__name__
        filler_name = '{}KeySource'.format(name)
        cls.base_key_source = cls.key_source
        defn = ""
        for line in re.split(r'\s*\n\s*', cls.definition.strip()):
            if line.startswith("->"):
                defn = "{}{}\n".format(defn, line)

        class KeySource(dj.Computed):
            definition = defn

            @property
            def key_source(self):
                return cls().base_key_source

            def make(self, key):
                self.insert1(key)

        KeySource.__name__ = filler_name
        self.tables[filler_name] = self._schema(KeySource, context=context)

        cls.key_source = self.tables[filler_name]()

        return cls
