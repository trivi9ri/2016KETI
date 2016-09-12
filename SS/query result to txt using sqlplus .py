from sqlplus_commando import SqlplusCommando
from subprocess import *


def runSqlplus(conn, sql):
	session = Popen(['sqlplus','-S',conn], stdin=PIPE, stdout=PIPE, stderr = PIPE)
	session.stdin.write(sql)
	return session.communicate()

con_str = 'sample/sample@123.123.123.123/1234:sample'
sql = "@/home/yung/2016KETI/SS/discardQuery.sql"
res = runSqlplus(con_str,sql)
print res

