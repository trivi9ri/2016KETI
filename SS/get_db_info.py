from sqlplus_commando import SqlplusCommando
from subprocess import *
from pandas import Series, DataFrame
import pandas as pd
import re
import sys
import binascii

db = 'mos_view/mos_view@106.240.225.99:1521/orcl'


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
	#print table_data
	return table_data


def hex_to_str(hexstr):
	hexstr_n = hexstr.split('\\n')
	hex_n_list = []
	for hexn in hexstr_n:
		hexstr_t = hexn.split('\\t')
		hex_n_list += hexstr_t
	print hex_n_list
	# # hexstrs = re.split(r'([ ,.?\'],[\\n])', hexstr)
	# resstr = ''
	# for hexs in hexstrs:
	# 	if '\\x' in hexs:
	# 		hexs = re.sub(r"[\\x]","",hexs)
	# 		hexs = hexs.strip()
			
	# 		strtmp = binascii.unhexlify(hexs)
	# 		resstr+= strtmp
	# 	else: resstr+= hexs
	# return resstr


def get_column_info():
	table_data = pd.DataFrame()
	table_data = make_TbInfo_df(get_table_info())
	strtmp = ''

	#tb_df_r = table_data.shape[0]
	#tb_df_c = table_data.shape[1]

	tables = table_data[table_data.columns[0]]
	i = 0
	for table_name in tables:
		column_query = 'DESC %s;' % (table_name)
		qur_res = sql_connection(db, column_query)
		if '\\x' in str(qur_res):
			strtmp =hex_to_str(str(qur_res))
		# 	print strtmp
		# else: print qur_res

		i+=1
		if i == 10:
			break


def main():
	get_column_info()


if __name__ == '__main__':
	main()

