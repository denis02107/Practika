import os
import sqlite3
import config
from datetime import datetime
import json


# Функция для парсинга строки лога и извлечения IP-адреса и времени
def create_logs_db():
    if os.path.exists(config.database_name):
        os.remove(config.database_name)
    conn = sqlite3.connect(config.database_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE logs
                 (ip text, date text, request text, status_code integer, size integer)''')
    with open(config.file_to_pars) as f:
        for line in f:
            parts = line.split()
            ip = parts[0]
            date_str = parts[3][1:] + " " + parts[4][:-1]
            date = datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S %z')
            date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
            request = parts[5] + " " + parts[6] + " " + parts[7]
            status_code = int(parts[8])
            size = int(parts[9])
            c.execute(f"INSERT INTO logs VALUES ('{ip}', '{date_formatted}', '{request}', {status_code}, {size})")
    conn.commit()
    conn.close()

def create_logs_db_json():
  if os.path.exists(config.database_name_json):
      os.remove(config.database_name_json)
  conn = sqlite3.connect(config.database_name_json)
  c = conn.cursor()
  c.execute('''CREATE TABLE logs_json
               (ip text, date text, request text, status_code integer, size integer)''')
  with open(config.file_to_pars_json) as f:
      data = json.load(f)
      for log in data:
          ip = log['IP']
          date_str = log['Date'] + ' ' + log['Time']
          date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
          date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
          request = log['First_Line']
          status_code = log['Status']
          size = log['Size']
          c.execute(f"INSERT INTO logs_json VALUES ('{ip}', '{date_formatted}', '{request}', {status_code}, {size})")
  conn.commit()
  conn.close()

def select_by_ip(ip):
    conn = sqlite3.connect(config.database_name)
    c = conn.cursor()
    c.execute(f"SELECT * FROM logs WHERE ip='{ip}'")
    rows = c.fetchall()
    conn.close()
    return rows

def select_by_date(date):
    conn = sqlite3.connect(config.database_name)
    c = conn.cursor()
    date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
    c.execute(f"SELECT * FROM logs WHERE date='{date_formatted}'")
    rows = c.fetchall()
    conn.close()
    return rows

def select_by_date_range(start_date, end_date):
    conn = sqlite3.connect(config.database_name)
    c = conn.cursor()
    start_date_formatted = start_date.strftime('%d/%b/%Y:%H:%M:%S %z')
    end_date_formatted = end_date.strftime('%d/%b/%Y:%H:%M:%S %z')
    c.execute(f"SELECT * FROM logs WHERE date BETWEEN '{start_date_formatted}' AND '{end_date_formatted}'")
    rows = c.fetchall()
    conn.close()
    return rows

def get_logs_by_ip_json(ip_address):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if log['IP'] == ip_address]

def get_logs_by_date_json(date):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if log['Date'] == date]

def get_logs_by_date_range_json(start_date, end_date):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if start_date <= log['Date'] <= end_date]

create_logs_db()
create_logs_db_json()

# Пример реализации функций
print(select_by_ip('93.55.2.233'))
print(select_by_date(datetime.strptime('01/Aug/2018:21:34:23 +0300', '%d/%b/%Y:%H:%M:%S %z')))
print(select_by_date_range(datetime.strptime('17/Dec/2017:17:04:37 +0300', '%d/%b/%Y:%H:%M:%S %z'), datetime.strptime('20/Sep/2037:16:02:54 +0300', '%d/%b/%Y:%H:%M:%S %z')))

print(get_logs_by_ip_json('192.168.2.23'))
print(get_logs_by_date_json('2015-04-18'))
print(get_logs_by_date_range_json('2009-10-15', '2015-04-18'))
