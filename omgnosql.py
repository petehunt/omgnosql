# TODO: mysql/typed indexes (geo?)
# TODO: memcached
# TODO: sharding
# TODO: replicas (master/slave)
# TODO: map/red

# TODO: more full query language implementation
# TODO: valid table names


import sqlite3
import uuid
import jsonpickle
import operator

class ObjectId(object):
  def __init__(self, id):
    self.id = id
  def __repr__(self):
    return 'ObjectId(%r)' % self.id

class Connection(object):
  SYSTEM_COLUMNS = ('_id', '_misc')
  def __init__(self, file):
    self.conn = sqlite3.connect(file)
    self.index_cache = {} # (collection, database) => [columns...]
    self.create_schema()
  def create_schema(self):
    c = self.conn.cursor()
    c.execute('create table if not exists databases (name varchar(64) primary key)')
    self.conn.commit()
  def create_collection(self, name, database):
    c = self.conn.cursor()
    c.execute('create table if not exists collection_%s_%s (_id char(32) primary key, _misc text)' % (database, name))
    self.conn.commit()
  def create_database(self, name):
    c = self.conn.cursor()
    c.execute('insert into databases (name) values (?)', (name,))
    self.conn.commit()
  def create_indexes(self, collection, database, columns):
    # first, create a column
    # next, create a SQL index for this column
    # finally, we are done
    c = self.conn.cursor()
    new_columns = []
    for column in columns:
      if not column in self.index_cache[(collection, database)]:
        # create column since it doesn't exist
        # TODO: types of columns
        c.execute('alter table collection_%s_%s add %s' % (database, collection, column))
        new_columns.append(column)
    if len(new_columns) > 0:
      # reload the index cache so that when we insert the data again it will be indexed
      self.read_indexes(collection, database)
      # this will re-index for us
      for doc in self._find(collection, database, {}, ignore_cols=new_columns):
        self.insert(collection, database, doc)
    # next, create the sql index
    index_str = 'index_%s_%s_%s' % (database, collection, '_'.join(columns))
    columns_str = ', '.join(columns)
    c.execute('create index %s on collection_%s_%s(%s)' % (index_str, database, collection, columns_str))
    self.conn.commit()
  def get_indexes(self, collection, database):
    if (collection, database) not in self.index_cache:
      self.read_indexes(collection, database)
    return self.index_cache[(collection, database)]
  def read_indexes(self, collection, database):
    return self._read_indexes(collection, database, self.conn.cursor())
  def _read_indexes(self, collection, database, c):
    c.execute('pragma table_info(collection_%s_%s)' % (database, collection))
    columns = []
    for _,name,_,_,_,_ in c:
      if name in self.SYSTEM_COLUMNS:
        continue
      columns.append(name)
    self.index_cache[(collection, database)] = columns
  def matches_query(self, doc, query):
    for (k, v) in query.items():
      value = doc[k]
      if isinstance(v, dict):
        for (operation, param) in v.items():
          if operation == '$lt':
            if not value < param:
              return False
          elif operation == '$gt':
            if not value > param:
              return False
      else:
        if value != v:
          return False
    return True
  def insert(self, collection, database, docs):
    is_list = True
    if not isinstance(docs, list):
      is_list = False
      docs = [docs]
    ids = []
    for doc in docs:
      if isinstance(doc.get('_id'), ObjectId):
        id = doc['_id']
      else:
        id = ObjectId(uuid.uuid4().hex)
      c = self.conn.cursor()
      columns = ['_id', '_misc'] + self.get_indexes(collection, database)
      column_str = ', '.join(columns)
      params = tuple([id.id, jsonpickle.encode(doc)] + [doc[k] for k in self.get_indexes(collection, database)])
      param_str = ', '.join(['?' for _ in params])
      c.execute('replace into collection_%s_%s (%s) values (%s)' % (database, collection, column_str, param_str), params)
      self.conn.commit()
      ids.append(id)
    if not is_list:
      return ids[0]
    else:
      return ids
  def find_one(self, collection, database, query={}):
    return iter(self.find(collection, database, query)).next()
  def _find(self, collection, database, query, ignore_cols=[]):
    c = self.conn.cursor()
    other_columns = self.get_indexes(collection, database)
    for column in ignore_cols:
      other_columns.remove(column)
    if len(other_columns) > 0:
      columns_str = ', ' + ', '.join(other_columns)
    else:
      columns_str = ''
    c.execute('select _id, _misc%s from collection_%s_%s' % (columns_str, database, collection))
    for row in c:
      id = row[0]
      misc = row[1]
      other_fields = dict(zip(other_columns, row[2:]))
      data = jsonpickle.decode(misc)
      data.update(other_fields) # pull from index
      data['_id'] = ObjectId(id)
      if self.matches_query(data, query):
        yield data
  def find(self, collection, database, query={}):
    return Result(self._find(collection, database, query))
  def __getattribute__(self, k):
    try:
      return object.__getattribute__(self, k)
    except AttributeError:
      return self[k]
  def __getitem__(self, k):
    db = Database(self, k)
    setattr(self, k, db)
    return db

class Result(object):
  def __init__(self, iter):
    self.iter = iter
  def __iter__(self):
    return self.iter
  def sort(self, k):
    return Result(sorted(self.iter, key=operations.itemgetter(k)))
  def count(self):
    self.iter = list(self.iter)
    return len(self.iter)

class Database(object):
  def __init__(self, connection, name):
    self.connection = connection
    self.name = name
    self.connection.create_database(name)
  def __getattribute__(self, k):
    try:
      return object.__getattribute__(self, k)
    except AttributeError:
      return self[k]
  def __getitem__(self, k):
    c = Collection(self, k)
    setattr(self, k, c)
    return c

class Collection(object):
  def __init__(self, database, name):
    self.database = database
    self.name = name
    self.connection = database.connection
    self.connection.create_collection(self.name, self.database.name)
  def insert(self, doc):
    self.connection.insert(self.name, self.database.name, doc)
  def find_one(self, query={}):
    return self.connection.find_one(self.name, self.database.name, query)
  def find(self, query={}):
    return self.connection.find(self.name, self.database.name, query)
  def create_index(self, params):
    if not isinstance(params, list):
      params = [params]
    return self.connection.create_indexes(self.name, self.database.name, params)

def test():
  import datetime
  conn = Connection(':memory:')
  new_posts = [{"author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime.datetime(2009, 11, 12, 11, 14)},
               {"author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime.datetime(2009, 11, 10, 10, 45)}]
  db = conn['test_db']
  db.posts.insert(new_posts)
  db.posts.create_index('date')
  print list(db.posts.find({'author': 'Mike'}))
  print list(db.posts.find({'date': {'$lt': datetime.datetime(2009, 11, 12,10,14)}}))

if __name__ == "__main__":
  test()
