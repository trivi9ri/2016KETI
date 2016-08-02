#!/usr/bin/python
import os
import sys

os.system("sudo python ~/anal_log/log.py")
os.system("ls /home/yung/anal_log/no_option_log.xlsx| xargs unoconv -f xls")
print "Content-type: text/html\n"
print "<meta http-equiv='"'refresh'"' content='"'0;url=test_log.py'"'>"