import requests
import pandas as pd
import time
from datetime import datetime
import mysql.connector as connector

# API Credentials

url = "https://livescore6.p.rapidapi.com/matches/v2/list-by-date"

querystring = {"Category":"cricket","Date":"20230528","Timezone":"-7"}

headers = {"X-RapidAPI-Key": "55fa14c560mshd919993bff116a3p19dddejsn5c35ee03958a","X-RapidAPI-Host": "livescore6.p.rapidapi.com"}

#API Call

response = requests.get(url, headers=headers, params=querystring)


response.json()

dict = response.json()['Stages'][0]['Events'][0]
dict

EID = dict['Eid']
T1_scr = dict['Tr1C1']
T1_wk = dict['Tr1CW1']
T1_ov = dict['Tr1CO1']
T2_scr = dict['Tr2C1']
T2_wk = dict['Tr2CW1']
T2_ov = dict['Tr2CO1']
Status = dict['ECo']
T1_name = dict['T1'][0]['Nm']
T2_name = dict['T2'][0]['Nm']

print(EID)
print(T1_scr)
print(T1_wk)
print(T1_ov)
print(T2_scr)
print(T2_wk)
print(T2_ov)
print(Status)
print(T1_name)
print(T2_name)


Test = {
       "T1_scr": T1_scr,
       "T1_wk": T1_wk,
       "T1_ov": T1_ov,
       "T2_scr": T2_scr,
       "T2_wk": T2_wk,
       "T2_ov": T2_ov,
       "Status": Status,
       "T1_name": T1_name,
       "T2_name": T2_name,
        "EID": EID}
Test



df = pd.DataFrame(columns= ["T1_scr", "T1_wk", "T1_ov","T2_scr","T2_wk","T2_ov","Status","T1_name","T2_name", "EID"])

df.loc[0] = Test


def connect_to_db(host_name, port, username, password, db_name):
 try:
  conn = connector.connect(host=host_name, port=port, user=username, password=password, database=db_name)

 except connector.OperationalError as e:
  raise e

 else:
  print('Connected!')

 return conn

host_name = 'localhost'
port = int(3306)
username = 'root'
password = 'Pallav@_123'
db_name = 'IPL'
conn = None

conn = connect_to_db(host_name, port, username, password, db_name)

curr = conn.cursor(buffered = True)

# def create_table(curr):
#     create_table_command = "CREATE TABLE IF NOT EXISTS IPL_Finals( T1_scr INT NOT NULL, T1_wk INT NOT NULL, T1_ov FLOAT NOT NULL, T2_scr INT NOT NULL, T2_wk INT Not NULL, T2_ov Float NOT NULL, Status varchar(200) NOT NULL, T1_name varchar(50) NOT NULL, T2_name varchar(50) NOT NULL, EID INT NOT NULL)"

#     curr.execute(create_table_command)


# create_table(curr)

def check_if_EID_exists(curr, EID):
 query = ("""  SELECT EID from ipl_finals WHERE EID = %s""")
 curr.execute(query, (EID,))

 return curr.fetchone() is not None


def update_DB(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID):
 if check_if_EID_exists(curr, EID):
  update_row(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID)
  print("Table Updated!")

 else:
  insert_into_table(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID)
  print("Values inserted in table!")


def update_row(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID):
 query = (
  """UPDATE IPL_Finals SET T1_scr = %s, T1_wk = %s, T1_ov = %s, T2_scr = %s, T2_wk = %s, T2_ov = %s, Status = %s, T1_name = %s, T2_name = %s WHERE EID = %s;""")

 vars_to_update = (T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID)
 curr.execute(query, vars_to_update)


def insert_into_table(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID):
 insert_into_comm = (
  """ INSERT INTO ipl_finals (T1_scr , T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")

 row_to_insert = (T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID)
 curr.execute(insert_into_comm, row_to_insert)

update_DB(curr, T1_scr, T1_wk, T1_ov, T2_scr, T2_wk, T2_ov, Status, T1_name, T2_name, EID)

conn.commit()