#-*- coding: utf-8 -*-
import pandas as pd
import pandas.io.sql as psql
import cx_Oracle as odb
import numpy
import math
import matplotlib

class PreProcess():

   def makeDF(self):
      user = 'user'
      pw = 'user'
      dbenv = 'db'
      conn = odb.connect(user + '/' + pw + '@' + dbenv)
      sqlF = open('/home/trivi9ri/2016KETI/SS/LineUPH3.sql')
      LineSql = sqlF.read()
      sql_commands = LineSql.replace('\n','').split(';')
    
      curs = conn.cursor()
#      print LineSql
      curs.execute(LineSql)

      df = pd.DataFrame(curs.fetchall())
      df.columns = [rec[0] for rec in curs.description]

#     df.to_csv('test.csv')

      return df

   def DataPre(self):
      DataF = pd.DataFrame()
      DataF = self.makeDF()
      Cum_Load = []
      Cum_Run = []
      Ave_Grad = []
      tmp_Load = 0
      tmp_Run = 0
      for data in DataF:
	if 'TYPE' in data:
	   DataF =  DataF.drop(data, 1)

    
      for x in DataF:
	 if ('QTY' in x)|('TIME' in x) | ('RATE' in x):
		DataF = DataF[DataF[x] >= 0]
     
      row_cnt =  DataF.shape[0]
     
      for i in range(row_cnt):
	 tmp_Load += DataF['LOAD_TIME'].iloc[i]
         Cum_Load.append(tmp_Load)
         tmp_Run += DataF['RUN_TIME'].iloc[i]
         Cum_Run.append(tmp_Run)

      DataF['CUM_LOAD'] = Cum_Load
      DataF['CUM_RUN'] = Cum_Run
      
      Ave_Grad.append(0)

      for j in range(1,row_cnt):
         tmp_u = DataF['CUM_LOAD'].iloc[j-1]*DataF['RUN_TIME'].iloc[j] - DataF['CUM_RUN'].iloc[j-1]*DataF['LOAD_TIME'].iloc[j]
         tmp_d = DataF['CUM_RUN'].iloc[j-1]*DataF['CUM_LOAD'].iloc[j]
         tmp = abs(float(tmp_u)/float(tmp_d)) * 100.0 
         Ave_Grad.append(tmp)

      DataF['Ave_Grad'] = Ave_Grad

      cnt = 0
      for k in range(6):
         if  DataF['Ave_Grad'].iloc[row_cnt-1-k] <= 1:
            cnt += 1
          
      assert (cnt >= 5), '적절치 못한 데이터!'
      
       
      Pi = []
      Pmean = []
      Spi = []
      Zi = []
      Ri = []
      Rmean = []
      Sz = []
      Sd = []
      total_Ri = 0
      Ri.append(0)
      Rmean.append(0)
      Sz.append(0)
   

      for l in range(row_cnt):
         if DataF['LOAD_TIME'].iloc[l] != 0:
            Pi.append(float(DataF['RUN_TIME'].iloc[l])/float(DataF['LOAD_TIME'].iloc[l]))
         else: Pi.append(0)
   
         Pmean.append(float(DataF['CUM_RUN'].iloc[l])/float(DataF['CUM_LOAD'].iloc[l]))
    
      for m in range(row_cnt):
          if (DataF['LOAD_TIME'].iloc[m] != 0):
            Spi.append(math.sqrt(float(Pmean[row_cnt-1]*(1-Pmean[row_cnt-1]))/float(DataF['LOAD_TIME'].iloc[m])))
          else: Spi.append(0)
          if (Spi[m] != 0):
            Zi.append(float((Pi[m]-Pmean[row_cnt-1]))/float(Spi[m]))
          else: Zi.append(0)
          if m > 0:
            Ri.append(abs(Zi[m]-Zi[m-1]))
            total_Ri += Ri[m]
            Rmean.append(float(total_Ri)/float(m))
            Sz.append(float(Rmean[m])/1.128)
        
      for n in range(row_cnt):
          Sd.append(Spi[n]*Sz[row_cnt-1])
        
      

      DataF['Pi'] = Pi
      DataF['Pmean'] = Pmean
      DataF['Spi'] = Spi
      DataF['Zi'] = Zi
      DataF['Ri'] = Ri
      DataF['Rmean'] = Rmean
      DataF['Sz'] = Sz
      DataF['Sd'] = Sd
      print DataF        
 #     DataF.to_csv('example.csv')

if __name__ == '__main__':
   dpp = PreProcess()
   dpp.DataPre()
     
