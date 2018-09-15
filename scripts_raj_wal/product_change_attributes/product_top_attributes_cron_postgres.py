import os
import sys
import subprocess
import json
# import calendar
from datetime import datetime
# import math
import logging
import socket
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# import re
import commands
import base64
import pprint
import psycopg2
import os
import re
 
#####################################################################################
# Author: Raj Kumar
# OBJECTIVE:  From qarth_product_daily table:
#   1. Find those products which changed between two dates using product_attributes.  
#   2. Identify what attributes changed and their counts.
# This script requires python2.6 version.
#####################################################################################
class ProductTopAttributesCron:
    def __init__(self):
        self.postgres_query_files = [
                                     '/home/catint/qarth_product_top_attributes/postgres/postgres_drilldown_queries.sql'  ##for drilldowns
                                     #,'/home/catint/qarth_product_top_attributes/postgres_query.sql' ##for email reports
                                     ]
        self.last_report_file = '/home/catint/qarth_product_top_attributes/last_report.txt'

    def createTableDrillDown1(self):
        query = ('CREATE TABLE temp2_qarth_analytics_drilldownx1 (curr_date date, '
                 'attribute text, source text);')
        #insert into temp2_qarth_analyltics_drilldown values('20180705','brand', 700, 's6', 500, 's9', 7100, 's3', 2700);
    
    def createViewDrillDown2(self):
        query = ("CREATE VIEW temp2_qarth_analytics_drilldown2 AS "
                 "SELECT curr_date, attr_name, attr_count FROM temp2_qarth_analytics_drilldown1 "
                 "ORDER BY attr_count DESC Limit 250;")   
         
    def createViewDrillDown3(self):
        query = ("CREATE VIEW temp2_qarth_analytics_drilldown3 AS "
                 "SELECT curr_date, source_name1, source_count1, source_name2, source_count2, "
                 "source_name3, source_count3, attr_count FROM temp2_qarth_analytics_drilldown1 "
                 "ORDER BY attr_count DESC Limit 25;")   
         
    def createViewDrillDown4(self):
        query = ("CREATE VIEW temp2_qarth_analytics_drilldown4 AS "
                 "SELECT curr_date, source_name, attr_count FROM temp2_qarth_analytics_drilldown1 "
                 "ORDER BY attr_count DESC Limit 250;")   
  
        
    def writePostgres(self, file):
        queries = ''
        try:
            with open(file, 'r') as f:
                query = f.readline()
                queries += query + ';'
                i = 0
                while queries:
                    if i == 1000:
                        self.execPostgres(queries)
                        queries = ''
                        i = 0 
                    query = f.readline()
                    queries += query + ';'
                    i += 1
                self.execPostgres(queries) #process leftovers
        except Exception as e:
            print 'ERROR:  Exception thrown:  Can not read sql file'
        
    def removeSQLfile(self, file): 
        try:
            os.remove(file) 
        except Exception as e:
            print 'ERROR:  Exception thrown:  Can not remove sql file'
        
    def execPostgres(self, query):
        print '[postgres query]: ', query
        try:
            conn = psycopg2.connect("dbname='postgres' user='postgres' host='10.65.167.217' password='postgres'")
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print 'ERROR: Exception thrown: Postgres query failed', e 
            
    def get_last_update_date(self):
        last_update_date = ''
        with open(self.last_report_file, 'r') as f:
            data = f.read()
            last_update_date = data.replace('-', '')
        return last_update_date
            
    def get_current_update_date(self):
        current_update_date = ''
        with open(self.postgres_query_files[0], 'r') as f:
            data = f.read()
            current_update_date = re.findall('20[0-9]{6}', data)[0]
        return current_update_date
            
    def is_new_data(self):
        last_update_date = int(self.get_last_update_date())
        current_update_date = int(self.get_current_update_date())
        print '[last_update_date]: ', last_update_date
        print '[current_update_date]: ', current_update_date
        return current_update_date > last_update_date
    
    def update_last_report_file(self):
        new_date = self.get_current_update_date()
        print '[new_date]: ', new_date
        new_date2 = new_date[0:4] + '-' + new_date[4:6] + '-' + new_date[6:8]
        print '[new_date2]: ', new_date2
        with open(self.last_report_file, 'w') as f:
            f.write(new_date2)
################################################################################ 
def run():
    print "[---------------------Job Started---------------------------]"
    pADD = ProductTopAttributesCron()
    for file in pADD.postgres_query_files:
        if pADD.is_new_data() == True:
            pADD.writePostgres(file)    
            pADD.update_last_report_file()
            pADD.removeSQLfile(file)   
    #-------------------------------------------------------------- 
    print "[--------------------Job Completed--------------------]"
##################################################################################
if __name__ == "__main__":
    run()
###################################################################################


