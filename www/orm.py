import asyncio,logging
import aiomysql

@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info('create db')
	global __pool
	__pool=yield from aiomysql.creat_pool(
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
	log(sql)
	with (yield from __pool) as conn:
		try:
			cur=yield from conn.cursor()
			yield from cur.execute(sql.replace('?','%s'),args)
			affected=cur.rowcount
			yield from cur.close()
		except BaseException as e:
			raise
		return affected


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
				mapping[k]=v
			if v.primary_key:
				#找到主键
				if primaryKey:
					raise RuntimeError('Duplicate primary key for field: %s' %k)
				primaryKey=k
			else:
				field.append(k)
		if not primaryKey:
			raise RuntimeError('primary key not found')
		for k in mappings.key():
			attrs.pop(k)
		escaped_fields=list(map(lambda f:'`%s`' %f,fields))
		attrs['__mappings__']=mappings
		attrs['__table__']=tableName
		attrs['primary_key__']=primaryKey
		attrs['__fields__']=fields
		#sql语句
		attrs['__select__']='select `%s`, %s from `%s`' %(primaryKey,','.join(escaped_fields),tableName)
		attrs['__insert__']='insert into `%s` (%s,`%s`) values (%s)' %(tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		attrs['__update__']='update `%s` set %s where `%s`=?' %(tableName,','.join(map(lambda f:'`%s`=?' %(mappings.get(f).name or f),fields)),primaryKey)
		attrs['__delete__']='delete from `%s` where `%s`=?' %(tableName,primaryKey)
		return type.__new__(cls,name,bases,attrs)

class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		dict.__init__(**kw)
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
		''

class Field(object):
	def __init__(self,name,column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default
	def __str__(self):
		return '<%s,%s:%s>' %(self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
	def __init__(self,name=None,Primary_key=False,default=None,ddl='varchar(100)'):
		Field.__init__(name,ddl,primary_key,default)

class BooleanField(Field):
	def __init__(self,name=None,default=False):
		Field.__init__(name,'boolean',False,default)

class IntegerField(Field):
	def __init__(self,name=None,primary_key=False,default=0):
		Field.__init__(name,'bigint',primary_key,default)

class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		Field.__init__(name,'real',primary_key,default)

class TextField(Field):
	def __init__(self,name=None,default=None):
		Field.__init__(name,'text',False,default)

