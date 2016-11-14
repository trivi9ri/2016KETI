#-*- coding: utf-8 -*-
import matplotlib
matplotlib.use('Agg')
import pandas as pd
import pandas.io.sql as psql
import cx_Oracle as odb
import numpy
import math
import matplotlib.pyplot as plt
import matplotlib.legend as legend



class PreProcess():

   def makeDF(self):
      user = 'USER'
      pw = 'USER'
      dbenv = 'DB'
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

   def DataPre(self, RateType):
      print RateType
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
	   if x=='LOAD_TIME':
              DataF = DataF[DataF['LOAD_TIME'] > 0]
           else:
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
      UL3 = []
      UL2 = []
      UL = []
      LL3 = []
      LL2 = []
      LL = []
      total_Ri = 0
      Ri.append(0)
      Rmean.append(0)
      Sz.append(0)
   

      for l in range(row_cnt):
         Pmean.append(float(DataF['CUM_RUN'].iloc[l])/float(DataF['CUM_LOAD'].iloc[l]))
         if RateType == 'TIME':
            Pi.append(float(DataF['TIME_RATE'].iloc[l]/100.0))
         elif RateType == 'PERF':
            Pi.append(float(DataF['PERFORMANCE_RATE'].iloc[l]/100.0))              

      for m in range(row_cnt):
            Spi.append(math.sqrt(float(Pmean[row_cnt-1]*(1-Pmean[row_cnt-1]))/float(DataF['LOAD_TIME'].iloc[m])))
            Zi.append(float((Pi[m]-Pmean[row_cnt-1]))/float(Spi[m]))
            if m > 0:
               Ri.append(abs(Zi[m]-Zi[m-1]))
               total_Ri += Ri[m]
               Rmean.append(float(total_Ri)/float(m))
               Sz.append(float(Rmean[m])/1.128)
        
      for n in range(row_cnt):
          Sd.append(Spi[n]*Sz[row_cnt-1])
          UL3.append(Pmean[row_cnt-1]+3 * Sd[n])
          UL2.append(Pmean[row_cnt-1]+2 * Sd[n])
          UL.append(Pmean[row_cnt-1]+Sd[n])
          LL3.append(Pmean[row_cnt-1]-3*Sd[n])
          LL2.append(Pmean[row_cnt-1]-2*Sd[n])
          LL.append(Pmean[row_cnt-1]-Sd[n])
        
      DataF['Pi'] = Pi
      DataF['Pmean'] = Pmean
      DataF['Spi'] = Spi
      DataF['Zi'] = Zi
      DataF['Ri'] = Ri
      DataF['Rmean'] = Rmean
      DataF['Sz'] = Sz
      DataF['Sd'] = Sd
      DataF['CL'] = Pmean
      DataF['3UL'] = UL3
      DataF['2UL'] = UL2
      DataF['UL'] = UL
      DataF['3LL'] = LL3
      DataF['2LL'] = LL2
      DataF['LL'] = LL
    
      self.drawGraph(DataF,RateType)        
 #     DataF.to_csv('example.csv')i
   def drawGraph(self,df,RateType):
      print RateType
      LocArr = df.LOCATION_ID.unique()
      for a in LocArr:
         df_LN = df[df['LOCATION_ID'] == a]
         LNPi = df_LN['Pi'].tolist()
         LNCL = df_LN['CL'].tolist()
         LN3UL = df_LN['3UL'].tolist()
         LN3LL = df_LN['3LL'].tolist()
         ax = []
         for x in df_LN['REQUEST_TIME']:
            ax.append(x[5:])
         x = range(len(ax)) 
         plt.xticks(x,ax)
         plt.xticks(rotation=60)
         plt.plot(x, LNPi,x, LNCL,x,LN3UL,x,LN3LL)
         plt.title(a)
         plt.xlabel('Date')
         plt.ylabel('Proportion')
         plt.legend(['Pi','CL','3UL','3LL'],loc='best')
         if RateType == 'TIME':
      #      print "tt"
            plt.savefig("/home/trivi9ri/2016KETI/SS/T"+a+".png",dpi = 72)
         elif RateType == 'PERF':
      #      print "pp"
            plt.savefig("/home/trivi9ri/2016KETI/SS/P"+a+".png",dpi = 72)
         plt.cla()
         plt.clf()      

if __name__ == '__main__':
   dpp = PreProcess()
   dpp.DataPre('TIME')
   dpp.DataPre('PERF')
     
