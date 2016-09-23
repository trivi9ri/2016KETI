from sqlplus_commando import SqlplusCommando
from subprocess import *
from pandas import Series, DataFrame
import pandas as pd

db = 'sample db'


def sql_connection(conn, sql):
	session = Popen(['sqlplus','-S',conn], stdin=PIPE, stdout=PIPE, stderr = PIPE)
	session.stdin.write(sql)
	return session.communicate()


def get_table_info():
	table_query = '@/home/yung/2016KETI/SS/tables.sql;'
	#print sql_connection(db, table_query)
	tb_txt = open('tables.txt', 'r')
	tb_file =  'table_info.txt'
	table_info = open(tb_file, 'w')
	while 1:
		line = tb_txt.readline()
		if not line: break
		temp =  line.strip().split(' ')
		while '' in temp:
			temp.remove('')
		str_tmp = ''.join(str(tmp+' ') for tmp in temp)
		table_info.write(str_tmp+'\n')
	tb_txt.close()
	table_info.close()
	return tb_file


def make_TbInfo_df(filename):
	table_file = open(filename, 'r')
	owners = []
	tb_names = []
	while 1:
		line = table_file.readline()
		if not line: break
		if '--' not in line:
			temp = line.strip().split(' ')
			if len(temp) > 3:
				owners.append(temp[0])
				tb_names.append(temp[1])
	data = { owners.pop(0): owners, tb_names.pop(0) :tb_names}
	table_data = pd.DataFrame(data)
	print table_data
	return table_data



def get_column_info():
	make_TbInfo_df(get_table_info())
	#column_query = 'DESC %s;' % (table_name)
	#print sql_connection(db, column_query)



def main():
	get_column_info()


if __name__ == '__main__':
	main()


