import sys
import os
import time
import re
from pandas import Series, DataFrame
import pandas as pd

now_time = int(time.time())
Source_Name = 'KETI%20Motes'
commands = 'curl \'http://new.openbms.org/backend/api/prev/Metadata__SourceName/'+Source_Name+'/Metadata__Location__Building/SDH?starttime=%d\'' % now_time


def create_data():
	f = open("KETIMote.txt",'w')
	write_to =  '> KETIMote.txt'
	cmds = commands + write_to
	os.system(cmds)
	f.close()


def make_keti_data_to_df(idx_param):
	create_data()
	uuid = []
	reading_value = []
	f2 = open("KETIMote.txt","r")
	data = f2.read()	
	for a in data.strip().split('}'):
		b = re.sub(r"[\"\[\]{},:]","",a)
		if len(b.strip().split(' ')) == 5 :
			uuid.append(b.strip().split(' ')[1])
			reading_value.append(b.strip().split(' ')[4])
		elif len(b.strip().split(' ')) == 3:
			uuid.append(b.strip().split(' ')[1])
			reading_value.append("")
	if idx_param == 0:
		table = {'uuid' : uuid, "value %d" % idx_param:reading_value}
		frame = DataFrame(table)
	else:
		table = {"value %d" % idx_param:reading_value}
		frame = DataFrame(table)
	return frame


def runnig_check():
	result = DataFrame()
	tmp = DataFrame()
	for i in range(0,3):
		if i == 0:
			result = make_keti_data_to_df(i)
		else:
			tmp = result
			result = tmp.join(make_keti_data_to_df(i))
		time.sleep(2)
	return result


def find_idle_sensor():
	data = DataFrame()
	data = runnig_check()
	idle = []
	column_names = data.columns.values
	for x in range(len(data.index)):
		idle_check = ""
		for y in column_names:
			if y != 'uuid':
				idle_check += data[y][x]

		if idle_check == "":
			idle.append(data['uuid'][x])
	return idle

def get_sensor_number():
	idle_uuid = find_idle_sensor()
	idle_sensor_number = []
	fl = open("idle_uuid.txt",'w')
	for x in range(len(idle_uuid)):
		curl_cmds = 'curl http://new.openbms.org/backend/api/tags/uuid/%s >> idle_uuid.txt' % idle_uuid[x]
		os.system(curl_cmds)
	fl.close()
	fl2 = open("idle_uuid.txt",'r')
	fl3 = open("idle_sensor.txt",'w')
	idle_data = fl2.read()
	for i in idle_data.strip().split(']'):
		 j = re.sub(r"[\"\[\]{},:]","", i)
		 j = j.strip()
		 idle_uuid_detail = j.split(' ')
		 if len(idle_uuid_detail) > 5 :
		 	num_idx =  idle_uuid_detail.index('Path')
		 	floor_idx =  idle_uuid_detail.index('Floor')
		 	sensor_num = idle_uuid_detail[num_idx+1].split('/')[2]
		 	floor_num = idle_uuid_detail[floor_idx+1]
		 	sentence = "sensor number: %s  floor: %s\n" % (sensor_num,floor_num)
		 	fl3.write(sentence)
	fl3.close()
	fl2.close()
	os.system('rm idle_uuid.txt')


def main():	
	get_sensor_number()


if __name__ == '__main__':
	main()