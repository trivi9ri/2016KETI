#-*- coding: utf-8 -*-
from sqlplus_commando import SqlplusCommando
from subprocess import *
from pandas import Series, DataFrame
import pandas as pd
import re
import sys
import binascii
import nltk
import time
from collections import Counter


reload(sys)
sys.setdefaultencoding('utf-8')

db_data_file = open("db_data.txt",'w')
db = 'sample'


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
	total_tb = 0
	while 1:
		line = table_file.readline()
		if not line: break
		if 'TABLE' in line:
			total_tb += 1
		if '--' not in line:
			temp = line.strip().split(' ')
			if len(temp) > 3:
				owners.append(temp[0])
				tb_names.append(temp[1])
	data = { owners.pop(0): owners, tb_names.pop(0) :tb_names}
	table_data = pd.DataFrame(data)
	#print table_data
	db_data_file.write("Total Table: %d\n" % total_tb)
	return table_data


def hex_to_str(hexstr):
	hexs = re.sub(r"[\\x?]","",hexstr)
	hexs = hexs.strip()
	strtmp = binascii.unhexlify(hexs)
	return strtmp


def get_column_info():
	table_data = pd.DataFrame()
	table_data = make_TbInfo_df(get_table_info())
	td_file = open("table_data.txt",'w')
	res_sent = ''
	total_info = ''
	idx = 0;
	#tb_df_r = table_data.shape[0]
	#tb_df_c = table_data.shape[1]

	tables = table_data[table_data.columns[0]]
	owners = table_data[table_data.columns[1]]
	print owners
	# i = 0
	r_table = 0
	start_time = time.time()
	#i = 0
	for table_name in tables:
		column_query = 'DESC %s;' % (table_name)
		qur_res = sql_connection(db, column_query)
		if "ERROR:" not in str(qur_res):
			r_table += 1
			#i += 1
			# table_columns = re.sub(r'\\t', '\t', str(qur_res))
			# table_columns = re.sub(r'\\n', '\n', table_columns )
			tmp_a = re.split("(\\\\t| |\\\\n)", str(qur_res))
			for tmp in tmp_a:
				tmps = re.sub(r'([,.\'\(\)])','',tmp) 
				if '\\x' in tmps:
					res_sent += hex_to_str(tmps)
				else:
					tmp_t = re.sub(r'\\t', '\t',tmps)
					tmp_n = re.sub(r'\\n', '\n', tmp_t)
					res_sent += tmp_n	
			if res_sent !='':
				tmp_sent = ("Table name: %s\nOwner name: %s\n" % (table_name,owners[idx])) + res_sent + "\n"
				td_file.write(tmp_sent.encode('utf-8'))
				total_info += tmp_sent
				idx+=1
			#if i == 15: break;
		
	print ("time: %s\n"% (time.time() - start_time))
	print ("table: %d" % r_table)
	td_file.close()
	return

def table_analysis():
	get_column_info()
	tb_file = open('table_data.txt','r')
	tb_data = tb_file.read()
	tbs_data = tb_data.split('Table name')
	data_df = pd.DataFrame()
	table_df = pd.DataFrame()
	owner_df = pd.DataFrame()
	data_list = []
	table_dt = []
	owner_dt = []
	i = 0
	for data in tbs_data:
		i+=1
		en_data = data.decode('utf-8')
		tmp_tb = re.split(r'([:\n])',en_data)
		if len(tmp_tb) > 3:
			table_dt.append(tmp_tb[2])
		if 'Owner' in data:
			own_tmp = re.search("Owner name: \w+",data)
			own_group = own_tmp.group()
			own_group = own_group.strip().split(' ')[2]
			owner_dt.append(own_group)
		# temp_list = re.split('[ ,._]',en_data)
		# data_list += temp_list
	table_dic = {'Table':table_dt}
	owner_dic = {'Owner':owner_dt}
	table_df = pd.DataFrame(table_dic)
	owner_df = pd.DataFrame(owner_dic)
	

	result_tb=pd.Series(' '.join(table_df['Table']).split('_')).value_counts()
	key_tb = pd.Series(' '.join(table_df['Table']).split('_')).value_counts().keys()
	result_own = pd.Series(' '.join(owner_df['Owner']).lower().split()).value_counts()
	key_own = owner_df['Owner'].value_counts().keys()
	
	tmp_tb_dt = {'Table_data':key_tb,'Table frequency':result_tb}
			
	tmp_on_dt = {'Owner data':key_own, 'Owner frequency':result_own}
	
	tb_dt =  pd.DataFrame(tmp_tb_dt)
	on_dt = pd.DataFrame(tmp_on_dt)

	db_data_file.write("Owner data\n")
	for b in range(len(on_dt)):
		db_data_file.write(str(on_dt['Owner data'][b])+" has "+str(on_dt['Owner frequency'][b])+" tables\n")
	db_data_file.write("\n\nTabla data\n")
	for a in range(len(tb_dt)):
		db_data_file.write(str(tb_dt['Table_data'][a])+" "+str(tb_dt['Table frequency'][a])+"\n")
	
	db_data_file.close()
	

def main():
	table_analysis()


if __name__ == '__main__':
	main()

