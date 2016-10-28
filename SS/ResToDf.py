import pandas as pd
import pandas.io.sql as psql
import cx_Oracle as odb

user = ''
pw = ''
dbenv = ''
conn = odb.connect(user + '/' + pw + '@' + dbenv)

curs = conn.cursor()
curs.execute("SELECT * FROM SAMPLE_tb")

df = pd.DataFrame(curs.fetchall())
df.columns = [rec[0] for rec in curs.description]

df.to_csv('test.csv')
