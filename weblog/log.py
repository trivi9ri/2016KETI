
import os
import sys
import re
import xlsxwriter as excel
import xlrd
#221.167.207.20 - tinyos [17/Jul/2016:08:07:21 +0900] "PROPFIND /webdav HTTP/1.1" 301 552 "-" "NetDrive 2.6.9"

#\D\d+\w+\d{4}[:]\d{2}[:]\d{2}[:]\d{2}
def parsing():
   #m = re.findall(r"(\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\D) (\D+) (\D\d+[/]\w+[/]\d{4}[:]\d{2}[:]\d{2}[:]\d{2} \D\d{4}\D) (\D\w+) ((\D\w+)+) " ,'221.167.207.20 - tinyos [17/Jul/2016:08:07:21 +0900] "PROPFIND /webdav/webdav/media/movie/ HTTP/1.1" 301 552 "-" "NetDrive 2.6.9')
   #m = re.findall(r"(\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\D) (\D+) (\D\d+[/]\w+[/]\d{4}[:]\d{2}[:]\d{2}[:]\d{2} \D\d{4}\D) (\D\w+) (\S*) (\S*) (\d* \d*) (\D\S*) (\D(\s?)\S*)" ,'221.167.207.20 - tinyos [17/Jul/2016:08:07:21 +0900] "PROPFIND /webdav/webdav/media/movie/ HTTP/1.1" 301 552 "-" "NetDrive 3.4.5"') #success
   
   '''
   clientIP
   clientInfo
   userID
   endTime
   method
   reqSource
   protocol
   statusCode
   size
   refererHeader
   userAgentHeader
   '''
   
   log_format = re.compile(r"(\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\D) (\D+) (\D\d+[/]\w+[/]\d{4}[:]\d{2}[:]\d{2}[:]\d{2} \D\d{4}\D) (\D\w+) (\S*) (\S*) (\d* \d*) (\D\S*) (\D(\s?)\S*)")
   log_format_naming = re.compile(r"(?P<clientIP>\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (?P<clientInfo>\D*) (?P<userID>\D*) (?P<endTime>\D\d+[/]\w+[/]\d{4}[:]\d{2}[:]\d{2}[:]\d{2} \D\d{4}\D) (?P<method>\D\w+) (?P<reqSource>\S*) (?P<protocol>\S*) (?P<statusCode>\d*) (?P<size>\d*) (?P<refererHeader>\D\S*) (?P<userAgentHeader>\D(\s?)\S*)")

   clientIP_list = []
   count_list = []
   userID = []
   endDate = []
   method = {}
   reqSource = {}
   statusCode = {}
   size = {}
   refererHeader = {}
   userAgentHeader = {}

   tmp = []
   reqSource_tmp = []
   statusCode_tmp = []
   size_tmp = []
   userAgentHeader_tmp = []
   refererHeader_tmp = []
   

   open('log_grouping.txt','w')
   
   with open('log_contents.txt','r') as f:
      for line in f.readlines():
         i = 0
         anal_log = log_format_naming.search(line)
         with open('log_grouping.txt','a') as grouping_file:
            if anal_log:
               grouping_file.write(str(anal_log.groups()))
               grouping_file.write('\n')

               if clientIP_list.count(anal_log.groups()[0]) != 0:   
                  idx = clientIP_list.index(anal_log.groups()[0])
                  count_list[idx] = count_list[idx] + 1   
                  userID[idx] = anal_log.groups()[2]
                  endDate[idx] = anal_log.groups()[3]

                  tmp.append(anal_log.groups()[4])
                  method[idx] = tmp

                  reqSource_tmp.append(anal_log.groups()[5])
                  reqSource[idx] = reqSource_tmp

                  statusCode_tmp.append(anal_log.groups()[7])
                  statusCode[idx] = statusCode_tmp

                  size_tmp.append(anal_log.groups()[8])
                  size[idx] = size_tmp

                  refererHeader_tmp.append(anal_log.groups()[8])
                  refererHeader[idx] = refererHeader_tmp

                  userAgentHeader_tmp.append(anal_log.groups()[8])
                  userAgentHeader[idx] = userAgentHeader_tmp


               else:
                  clientIP_list.append(anal_log.groups()[0])
                  idx = clientIP_list.index(anal_log.groups()[0])
                  count_list.insert(idx,1)   
                  userID.insert(idx,anal_log.groups()[2])
                  endDate.insert(idx,anal_log.groups()[3])
                  
                  tmp.insert(idx, anal_log.groups()[4])
                  method[idx] = tmp
                  
                  reqSource_tmp.insert(idx, anal_log.groups()[5])
                  reqSource[idx] = reqSource_tmp

                  statusCode_tmp.insert(idx, anal_log.groups()[7])
                  statusCode[idx] = statusCode_tmp

                  size_tmp.insert(idx, anal_log.groups()[8])
                  size[idx] = size_tmp

                  refererHeader_tmp.insert(idx, anal_log.groups()[9])
                  refererHeader[idx] = refererHeader_tmp

                  userAgentHeader_tmp.insert(idx, anal_log.groups()[10])
                  userAgentHeader[idx] = userAgentHeader_tmp
                  
   '''
   print clientIP_list
   print count_list
   print userID
   print endDate
   print method 
   print reqSource 
   print statusCode
   print size 
   print refererHeader 
   print userAgentHeader
   ''' 
   log(clientIP_list,count_list,userID, endDate, method, reqSource, statusCode, size, refererHeader, userAgentHeader)
   
def log(clientIP_list,count_list,userID, endDate, method, reqSource, statusCode, size, refererHeader, userAgentHeader):
   row = 1
   start_row = 1
   max_row = 1

   workbook = excel.Workbook('no_option_log.xls')
   global ws
   ws = workbook.add_worksheet()

   tmp_list = []
   
   ws.write('A1', 'UserID')
   ws.write('B1', 'ClientIP')
   ws.write('C1', 'Access Number')
   ws.write('D1', 'Final Access Time')
   ws.write('E1', 'HTTP Method')
   ws.write('F1', 'Request source')
   ws.write('H1', 'Status code from server to client')
   ws.write('I1', 'Size of content without response header')
   ws.write('J1', 'HTTP request header')
   ws.write('K1', 'User-Agent HTTP request header')

   
   for i in userID:
      ws.write(max_row,0,i)
      index = userID.index(i)
   #   print index

      row = max_row
      ws.write(row,1,str(clientIP_list[index]))
      ws.write(row,2,str(count_list[index]))
      #ws.write(row,4,str(method.get(index)))
   
      max_row = print_list(reqSource.get(index), row, 5,max_row)
      max_row = print_list(statusCode.get(index), row, 7,max_row)
      max_row = print_list(size.get(index), row, 8,max_row)
      #print_list(refererHeader.get(index), row, max_row)
      #print_list(userAgentHeader.get(index), row, max_row)
      ws.write(max_row-1,3,str(endDate[index]))
   #   print max_row
      
 #  print "Success analyze log file.\nCheck 'log_anla.txt' file.\n"
   workbook.close()
   os.system("rm log_grouping.txt")
   os.system("rm log_contents.txt")


def print_list(l,row,col, max_row):
   for b in l:
      ws.write(row,col,b)
      row = row+1

   if row > max_row:
      max_row = row
   return max_row

def main():
   #os.system( "cd /var/log/apache2")
   os.system("cd ~/anal_log")
   os.system ("ls access* > log_title.txt")
   contents = open('log_contents.txt','w')   #for init

   with open('log_title.txt','rb')  as file:
      lines = file.readlines()   #lines -> list
      for a in lines:
         a = a[:-1]   #extract \n
         #os.system("cp /var/log/apache2/"+a+ "/home/starmichelle/log/"+'temp.txt')
         os.system("cp ~/anal_log/" +a+ " ~/anal_log/"+'temp.txt')
         with open('~/analtemp.txt','r') as f:
            contents = open('log_contents.txt','a')
            contents.write(f.read())
         
         contents.close()
   os.system("rm temp.txt")
   parsing()
   
   os.system("rm log_title.txt")

if __name__ == '__main__':
   main()
