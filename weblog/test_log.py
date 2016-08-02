#!/usr/bin/python
import cgi
import cgitb
import xlrd

file_location = '~/anal_log/no_option_log.xls'
workbook = xlrd.open_workbook('/home/yung/anal_log/no_option_log.xls')
sheet = workbook.sheet_by_index(0)
data = [[sheet.cell_value(r,c) for c in range(sheet.ncols)] for r in range(sheet.nrows)]
rows = len(data)
cols = len(data[0])
max_len = []
sum_cols = []
for a in range(cols):
	temp = []
	sumOf = 0;

	for b in range(rows):
		temp.append(len(str(data[b][a])))
		if b != 0:
			sumOf = sumOf+len(str(data[b][a]))
	sum_cols.append(sumOf)

	
	max_len.append(max(temp))


print "Content-type: text/html\n"
print "<html>"
print "<head>"
print "</head>"
print "<body style='"'overflow-x:scroll;'"'>"
for i in range(rows):
	for j in range(cols):
		if sum_cols[j] != 0:
			print "%s" % data[i][j]
			print (max_len[j]-len(str(data[i][j])))*"&ensp;"
	print "<br/><br/>"
print "</body>"
print "</html>"