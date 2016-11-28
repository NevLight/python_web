import asyncio,logging
import aiomysql

@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info('create db')
	global __pool
	__pool=yield from aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('post',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
	)

@asyncio.coroutine
def select(sql,args,size=None):
	log(sql,args)
	global __pool
	with (yield from __pool) as conn:
		cur=yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.excute(sql.replace('?','%s'),args or ())
		if size:
			rs=yield from cur.fetchmany(size)
		else:
			rs=yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows returned: %s' %len(rs))
		return rs

@asyncio.coroutine
def execute(sql,args):
	# log(sql)
	with (yield from __pool) as conn:
		try:
			cur=yield from conn.cursor()
			yield from cur.execute(sql.replace('?','%s'),args)
			affected=cur.rowcount
			yield from cur.close()
		except BaseException as e:
			raise
		return affected

def create_args_string(num):
	L=[]
	for n in range(num):
		L.append('?')
	return ','.join(L)

class ModelMetaclass(type):
	def __new__(cls,name,bases,attrs):
		#排除model
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		#获取table民称
		tableName=attrs.get('__table__',None) or name
		logging.info('found model: %s(table %s)' %(name,tableName))
		#获取所有的Field和主键名
		mappings=dict()
		fields=[]
		primaryKey=None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info('found mapping: %s => %s' %(k,v))
				mappings[k]=v
				if v.primary_key:
					#找到主键
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s' %k)
					primaryKey=k
				else:
					fields.append(k)
		if not primaryKey:
			raise RuntimeError('primary key not found')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields=list(map(lambda f:'`%s`' %f,fields))
		attrs['__mappings__']=mappings
		attrs['__table__']=tableName
		attrs['__primary_key__']=primaryKey
		attrs['__fields__']=fields
		#sql语句
		attrs['__select__']='select `%s`, %s from `%s`' %(primaryKey,','.join(escaped_fields),tableName)
		attrs['__insert__']='insert into `%s` (%s,`%s`) values (%s)' %(tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		attrs['__update__']='update `%s` set %s where `%s`=?' %(tableName,','.join(map(lambda f:'`%s`=?' %(mappings.get(f).name or f),fields)),primaryKey)
		attrs['__delete__']='delete from `%s` where `%s`=?' %(tableName,primaryKey)
		return type.__new__(cls,name,bases,attrs)

class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		dict.__init__(self,**kw)
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no key '%s'" %key )
	def __setattr__(self,key,value):
		self[key]=value
	def getValue(self,key):
		return getattr(self,key,None)
	def getValueOrDefault(self , key):
		value=getattr(self,key,None)
		if value is None:
			field=self.__mappings__[key]
			if field.default is not None:
				value=field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' %(key,str(value)))
				setattr(self,key,value)
		return value
	@classmethod
	@asyncio.coroutine
	def findAll(cls,where=None,args=None,**kw):
		sql=[cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args=[]
		orderBy=kw.get('orderBy',None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit=kw.get('limit',None)
		if limit:
			sql.append('limit')
			if isinstance(limit,int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit,tuple) and len(limit)==2:
				sql.append('?,?')
				args.extend(limit)
			else:
				raise ValueError('Inavlid limit value: %s' %str(limit))
		rs=yield from select(' '.join(sql),args)
		return [cls(**r) for r in rs]
	@classmethod
	@asyncio.coroutine
	def findNumber(cls,selectField,where=None,args=None):
		sql=['select %s _num_ from `%s` % (selectField,cls.__table__)']
		if where:
			sql.append('where')
			sql.append(where)
		rs=yield from select(' '.join(sql),args,1)
		if len(rs)==0:
			return None
		return rs[0]['_num_']
	@classmethod
	@asyncio.coroutine
	def find(cls,pk):
		rs=yield from select('%s where `%s`=?' % (cls.__select__,cls.__primary_key__),[pk],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])
	@asyncio.coroutine
	def save(self):
		args=list(map(self.getValueOrDefault,self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows=yield from execute(self.__insert__,args)
		if rows!=1:
			logging.warn('failed to insert by primary key:affected rows:%s' %rows)
	@asyncio.coroutine
	def update(self):
		args=list(map(self.getValueOrDefault,self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows=yield from execute(self.__update__,args)
		if rows!=1:
			logging.warn('failed to update by primary ket:affected rows: %s' %rows)
	@asyncio.coroutine
	def remove(self):
		args=[self.getValue(self.__primary_key__)]
		rows=yield from execute(self.__delete__,args)
		if rows!=1:
			logging.warn('failed to remove by primary key: affected rows: %s' %rows)

class Field(object):
	def __init__(self,name,column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default
	def __str__(self):
		return '<%s,%s:%s>' %(self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		Field.__init__(self,name,ddl,primary_key,default)

class BooleanField(Field):
	def __init__(self,name=None,default=False):
		Field.__init__(self,name,'boolean',False,default)

class IntegerField(Field):
	def __init__(self,name=None,primary_key=False,default=0):
		Field.__init__(self,name,'bigint',primary_key,default)

class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		Field.__init__(self,name,'real',primary_key,default)

class TextField(Field):
	def __init__(self,name=None,default=None):
		Field.__init__(self,name,'text',False,default)

