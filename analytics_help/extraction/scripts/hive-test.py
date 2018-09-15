from pyhive import hive

cursor = hive.connect('cdc-main-client00.bfd.walmart.com').cursor()
cursor.execute('SELECT * FROM ae_reporting_tableau_tbl LIMIT 50')
print cursor.fetchall()