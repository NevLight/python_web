import orm
from models import User,Blog,Comment
import asyncio
import sys
def mytest(loop):
	yield from orm.create_pool(loop=loop,user='www-data',password='www-data',db='awesome')
	u=User(name='my',email='4443d@qq.com',passwd='wushi',image='1111')
	yield from u.save()

if __name__=='__main__':
	loop=asyncio.get_event_loop()
	loop.run_until_complete(mytest(loop))
	# loop.close()
