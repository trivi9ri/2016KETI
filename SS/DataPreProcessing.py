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
import datetime

class PreProcess:

   #데이터베이스와 연결하고 쿼리 결과를 데이터프레임으로 저장합니다.
   def MakeDf(self):
      user = ''
      pw = ''
      db = ''
      conn = odb.connect(user + '/' + pw + '@' + db)
      sqlF = open('/home/trivi9ri/2016KETI/SS/LineUPH3.sql')
      LineSql = sqlF.read()
      sqlCommand = LineSql.replace('\n','').split(';')
    
      curs = conn.cursor()
      curs.execute(LineSql)

      df = pd.DataFrame(curs.fetchall())
      df.columns = [rec[0] for rec in curs.description]

     #데이터프레임을 csv 파일로 저장합니다.
     #df.to_csv('Result.csv')
      return df

   #적정 데이터 검사
   def DataPre(self):
      DataF = pd.DataFrame()
      DataF = self.MakeDf()
      Cum_Load = [] #누적부하시간
      Cum_Run = []  #누적가동시간
      Ave_Grad = []  #평균기울기
      tmp_Load = 0
      tmp_Run = 0
      
      for data in DataF:
         if 'TYPE' in data:
            DataF = DataF.drop(data, 1) #타입별로 발생한 무작업시간 데이터를 제거

      #어떤 시간정보나 양, 백분율을 나타내는 데이터 중 0 미만의 값을 제거
      for x in DataF:
         if ('QTY' in x) | ('TIME' in x) | ('RATE' in x):
            if x == 'LOAD_TIME':
               DataF = DataF[DataF['LOAD_TIME'] > 0]
            else:
               DataF = DataF[DataF[x] >= 0]


      row_cnt = DataF.shape[0]

      #누적부하, 가동시간 계산
      for i in range(row_cnt):
         tmp_Load += DataF['LOAD_TIME'].iloc[i]
         Cum_Load.append(tmp_Load)
         tmp_Run += DataF['RUN_TIME'].iloc[i]
         Cum_Run.append(tmp_Run)

      DataF['CUM_LOAD'] = Cum_Load
      DataF['CUM_RUN'] = Cum_Run

      Ave_Grad.append(0)

 
      #순간기울기 계산
      for j in range(1,row_cnt):
         tmp_u = DataF['CUM_LOAD'].iloc[j-1] * DataF['RUN_TIME'].iloc[j] - DataF['CUM_RUN'].iloc[j-1] * DataF['LOAD_TIME'].iloc[j]
         tmp_d = DataF['CUM_RUN'].iloc[j-1]*DataF['CUM_LOAD'].iloc[j]
         tmp = abs(float(tmp_u)/float(tmp_d)) * 100.0
         Ave_Grad.append(tmp)

      DataF['Ave_Grad'] = Ave_Grad

      Cnt = [] 
      
      for h in range(0, row_cnt):
         tmp_cnt = 0
         if h-5 >= 0:
            for k in range(0,6):
               if DataF['Ave_Grad'].iloc[h-k] <= 1:
                  tmp_cnt += 1
         Cnt.append(tmp_cnt)

      DataF['CNT'] = Cnt
      #순간기울기 값에 대해서 적정데이터 검출
      DataF = DataF[DataF['CNT'] >= 5]

      return DataF

   #차트를 그리기 위해 필요한 값들을 구함
   #2nd param - TIME(시간가동률), PERF(성능가동률), RUN(가동률)
   #3nd param - Line, Item
   def CalcData(self, RateType, IL):
      print ("Rate: %s, IL: %s") % (RateType,IL)
      Data = pd.DataFrame()
      Data = self.DataPre()
               
      Pi = []
      Pmean = []
      Spi = []
      Zi = []
      Ri = []
      Rmean = []
      Sz = []
      Sd = []
      UL = []
      UL2 = []
      UL3 = []
      LL = []
      LL2 = []
      LL3 = []
      total_Ri = 0
      Ri.append(0)
      Rmean.append(0)
      Sz.append(0)

      row_cnt = Data.shape[0]
      
      #입력 데이터의 종류에 따라 Pi 구함
      for i in range(row_cnt):
         Pmean.append(float(Data['CUM_RUN'].iloc[i])/float(Data['CUM_LOAD'].iloc[i]))
         if RateType == 'TIME':
            Pi.append(float(Data['TIME_RATE'].iloc[i]/100.0))
         elif RateType == 'PERF':
            Pi.append(float(Data['PERFORMANCE_RATE'].iloc[i]/100.0))
         #elif RateType == 'RUN':
         #   Pi.append()

      for j in range(row_cnt):
         Spi.append(math.sqrt(float(Pmean[row_cnt-1]*(1-Pmean[row_cnt-1]))/float(Data['LOAD_TIME'].iloc[j])))
         Zi.append(float((Pi[j] - Pmean[row_cnt-1]))/float(Spi[j]))
         if j > 0:
            Ri.append(abs(Zi[j] - Zi[j-1]))
            total_Ri += Ri[j]
            Rmean.append(float(total_Ri)/float(j))
            Sz.append(float(Rmean[j])/1.128)


      for h in range(row_cnt):
         Sd.append(Spi[h]*Sz[row_cnt-1]) 
         if (RateType == 'TIME') | (RateType == 'RUN'):
            if Pmean[row_cnt-1]+3 * Sd[h] > 1:
               UL3.append(1.0)
            else:
               UL3.append(Pmean[row_cnt-1]+ 3*Sd[h])
         elif RateType == 'PERF':
            UL3.append(Pmean[row_cnt-1]+ 3*Sd[h])
         
         UL2.append(Pmean[row_cnt-1]+ 2*Sd[h])
         UL.append(Pmean[row_cnt-1] * Sd[h])
         LL3.append(Pmean[row_cnt-1] - 3*Sd[h])
         LL2.append(Pmean[row_cnt-1] - 2*Sd[h])
         LL.append(Pmean[row_cnt-1] - Sd[h])

      Data['Pi'] = Pi
      Data['Pmean'] = Pmean
      Data['Spi'] = Spi
      Data['Zi'] = Zi
      Data['Ri'] = Ri
      Data['Rmean'] = Rmean
      Data['Sz'] = Sz
      Data['Sd'] = Sd
      Data['CL'] = Pmean
      Data['3UL'] = UL3
      Data['2UL'] = UL3
      Data['UL'] = UL
      Data['3LL'] = LL3
      Data['2LL'] = LL2
      Data['LL'] = LL

     #구한 값들을 csv 파일로 저장
     # Data.to_csv('%s_%s_Data.csv'%(IL, RateType))

      self.DrawGraph(Data, RateType, IL, 'D')
      self.DrawGraph(Data, RateType, IL, 'W')
      self.DrawGraph(Data, RateType, IL, 'M')


   #그래프를 그리는 함수
   #4th param - 'D'(Daily), 'W'(Weekly), 'M'(Monthly)        
   def DrawGraph(self, df, RateType, IL, duration):
      print "RateType: %s, Item or Line: %s, Duration: %s" % (RateType, IL, duration)
      if IL == 'L':
         Arr = df.LOCATION_ID.unique()
     # else:
     #    Arr = df.ITEM_ID.unique()

      for i in Arr:
         if IL == 'L':
            Data = df[df['LOCATION_ID'] == i]
        # else:
        #    Data = Data[Data['ITEM_ID'] == i]
         Pi = []
         CL = []
         UB = []
         LB = []
         ax = []
         row_cnt = Data.shape[0]
         idx = 1
     
         if duration == 'D':
            for j in Data['REQUEST_TIME']:
               ax.append(j[5:])
            Pi = Data['Pi'].tolist()
            CL = Data['CL'].tolist()
            UB = Data['3UL'].tolist()
            LB = Data['3LL'].tolist()
         else:
            x = 0
            while( x < row_cnt):
               reqDay = datetime.datetime.strptime(Data['REQUEST_TIME'].iloc[x], '%Y-%m-%d')
               if(duration == 'W'):
                  dura = datetime.timedelta(days = 6)
                  enDay = reqDay + dura
               elif (duration == 'M'):
                  y = x
                  while( y < row_cnt ):
                     if str(reqDay)[5:7] == str(Data['REQUEST_TIME'].iloc[y])[5:7]:
                        enDay = datetime.datetime.strptime(Data['REQUEST_TIME'].iloc[y],'%Y-%m-%d')
                     elif str(reqDay)[5:7] != str(Data['REQUEST_TIME'].iloc[y])[5:7]:
                        break
                     y += 1

               cnt = 0
               sum_pi = 0
               sum_cl = 0
               sum_ub = 0
               sum_lb = 0

               while((reqDay <= enDay) & ( x < row_cnt)):
                  cnt += 1.0
                  sum_pi += Data['Pi'].iloc[x]
                  sum_cl += Data['CL'].iloc[x]
                  sum_ub += Data['3UL'].iloc[x]
                  sum_lb += Data['3LL'].iloc[x]
                  x += 1
                  if x < row_cnt :
                     reqDay = datetime.datetime.strptime(Data['REQUEST_TIME'].iloc[x], '%Y-%m-%d')

               Pi.append(float(sum_pi/cnt))
               CL.append(float(sum_cl/cnt))
               UB.append(float(sum_ub/cnt))
               LB.append(float(sum_lb/cnt))
               if duration == 'W':
                  ax.append('Week %d'%idx)
                  idx += 1
               elif duration == 'M':
                  ax.append(str(enDay)[5:7])
         x = range(len(ax))
         plt.xticks(x, ax)
         plt.xticks(rotation = 60)
         plt.ylim(min(LB), max(UB) + 0.2)
         plt.plot(x, Pi, x, CL, x, UB, x, LB, marker = 'o')
         plt.title("Laney P' Chart %s %s %s" % (duration, IL, RateType))
         plt.xlabel('Date')
         plt.ylabel('Proportion')
         plt.text(0, max(UB) - 0.1, 'st=%s en=%s'%(Data['REQUEST_TIME'].iloc[0], Data['REQUEST_TIME'].iloc[row_cnt-1]), horizontalalignment = 'left', verticalalignment = 'center')
         plt.legend(['Pi','CL','UB','LB'],loc = 'best')
         plt.savefig("/home/trivi9ri/2016KETI/SS/%s_%s_%s_%s_Graph.png"%(duration,IL,RateType,i), dpi = 72)
         plt.cla()
         plt.clf()
         

if __name__ == '__main__':
   dpp = PreProcess()
   dpp.CalcData('TIME','L')
   dpp.CalcData('PERF','L')

