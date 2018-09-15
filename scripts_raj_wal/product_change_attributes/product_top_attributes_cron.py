import os
import sys
import subprocess
import json
# import calendar
import datetime
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
#import psycopg2
 
#####################################################################################
# Author: Raj Kumar
# OBJECTIVE:  From qarth_product_daily table:
#   1. Find those products which changed between two dates using product_attributes.  
#   2. Identify what attributes changed and their counts.
######################################################################################
class ProductTopAttributesCron:
    def __init__(self):
        self.db_catint = 'catint'
        self.db = 'catint'
        self.udf_path = '.'
       
        self.top_attributes_static = [
                                    "brand",   #Top to display attributes 
                                    "product_name",
                                    "number_of_cust_ratings",
                                    "average_customer_rating",
                                    "color",
                                    "actual_color",
                                    "price_per_unit_uom",
                                    "marketing_description",
                                    "size",
                                    "gender",
                                    "title",
                                    "model",
                                    "new",
                                    "manufacturer"
                               ]
        self.top_attributes_identification = [                                    
                                    "sku",   #Second Priority to Display 
                                    "product_type",
                                    "gtin",
                                    "item_id",
                                    "upc",
                                    "wupc"
                               ]
        self.top_attributes_size = [
                                    "assembled_product_length",  #Third priority Level 
                                    "clothing_size",
                                    "assembled_product_width",
                                    "assembled_product_height",
                                    "clothing_size_group",
                                    "shoe_size",
                                    "ring_size",
                                    "shirt_size"
                               ]
        
        self.top_attributes_dynamic_tableau = [  ##useful for Tableau dashboards...
                                    #'product_url_text',    
                                    'product_name',   
                                    'wmt_category',    
                                    'merchant_sku',    
                                    'assembled_product_weight',    
                                    #'jet_browse_node_id',    
                                    'assembled_product_width',    
                                    'primary_shelf',    
                                    'all_shelves',    
                                    'assembled_product_length',   
                                    'assembled_product_height',    
                                    'manufacturer_suggested_retail_price',   
                                    #'primary_shelf_id',    
                                    #'product_pt_family'
                                ]
        
        self.receiver_dist = [
                        'aj@Jet.com',
                        'anil@walmartlabs.com',
                        'edison@jet.com',
                        'ASipher@walmartlabs.com',
                        'AVarma@walmartlabs.com',
                        'Brijesh.Singh@walmart.com', 
                        'BSingh1@walmartlabs.com',
                        'CFrisch@walmartlabs.com',
                        'charles@jet.com',
                        'CSeto@walmart.com',          
                        'DGoyret@walmart.com', 
                        'DNewman@walmart.com',
                        'DSerpico@walmartlabs.com',
                        'emachleder@walmartlabs.com',
                        'FPoorooshasb@walmart.com',
                        'GAhluwalia@walmart.com',
                        'GKALL@email.wal-mart.com',
                        'GKhandelwal1@walmartlabs.com', 
                        'GMacdonald@walmart.com',
                        'gregd@walmartlabs.com', 
                        'GRekhi@walmartlabs.com', 
                        'JEfron@walmartlabs.com', 
                        'JFerraro@walmartlabs.com', 
                        'KBermudes@walmart.com',
                        'MGarimella@walmart.com', 
                        'OSiddiqui@walmartlabs.com', 
                        'PMessina@walmartlabs.com',
                        'qarthteam@email.wal-mart.com',
                        'RHua@walmart.com',
                        #'SKumar3@walmartlabs.com', ##this user asked for taking out his name from the list
                        'SMatt@walmartlabs.com', 
                        'STeng@walmartlabs.com',
                        'USingh0@walmartlabs.com',
                        #'VenkyTeam@wal-mart.com',
                        'VKALL@email.wal-mart.com',
                        'Zuzar.Nafar@walmartlabs.com',
                        'Rkumar1@walmartlabs.com',
                        'SSingikulam@walmartlabs.com'
                        ]
        
        self.temp2_product_attributes_counts3_lines = []
        self.attrsMap = {}
        self.orgIdsList = []  ##contains all the org_ids for seller_names
        self.postgres_number_of_days = 30
        self.sources_top_n_attributes = 25
        self.postgres_query_file = 'postgres_query.sql'
        self.postgres_query_test_file = 'postgres_query_test.sql'
        self.queries = ''
        self.yarn_cmd = 'export MAPR_MAPREDUCE_MODE=yarn '
        self.graphite_test_schema_path = 'qarth.test.product.metric.update.stats'
        self.graphite_prod_schema_path = 'qarth.prod.product.metric.update.stats'
        self.graphite_post_cmd = 'nc graphite.prod.monitoring.cdqarth.prod.walmart.com 2003'
        self.carbon_server = 'graphite.prod.monitoring.cdqarth.prod.walmart.com'
        self.carbon_port = '2003'
        self.topAttrDictL2 = {}
        self.topProdTypeDictL2 = {}
        self.top_stats_7days = {}
        self.top_stats_all = {}
        self.is_data_validation = 'False'  ##For testing based on product_ids provided by Lan
        self.is_validation = 'False' # for data validation tests, fetches only 1 record into temp2_product_attributes
        self.hour_post = 0 # 00 hours == 5PM (4PM- Winter) (default)
        self.debug_queries_only = 0 # 
        self.top_attributes_limit10 = 10 # baseline is 10
        self.top_attributes_limit3 = 3 # baseline is 3
        self.debug_limit = 0
        self.debug_limit_count = 10000
        self.retry_count = 3
        #self.uber_value_attribute_types_count = 4319   ###total number of all values attribute types   20171211
        #self.uber_attributes_count = 456024088  ###total number of all values attributes for all products 20171211
        self.uber_value_attribute_types_count = 4383   ###total number of all values attribute types   20171214
        self.uber_value_attributes_count = 564399994  ###total number of all values attributes for all products 20171214
        self.top_stat_prefix_int = 'int_'
        self.top_stat_delimiter = '####'
        self.top_stat_outdir = './top_stats'
        self.top_stat_outfile = 'top_stats_outfile'
        self.test_suffix = '' ### '' for prod; '_test' for test tables
        #self.top_stat_prefix_int = ''
        self.top_stat_prefix_qpd = 'qpd_'
        self.temp2_product_attributes = 'temp2_product_attributes'
        self.temp2_product_attributes2 = 'temp2_product_attributes2'
        self.temp2_product_attributes3 = 'temp2_product_attributes3'
        self.temp2_product_attributesX = 'temp2_product_attributes'
        self.temp2_org_ids = 'temp2_org_ids'
        self.temp2_product_attributes_counts1 = 'temp2_product_attributes_counts1'
        self.temp2_product_attributes_counts2 = 'temp2_product_attributes_counts2'
        self.temp2_product_attributes_counts3i = 'temp2_product_attributes_counts3i'
        self.temp2_product_attributes_counts3 = 'temp2_product_attributes_counts3'
        self.temp2_qarth_analytics_drilldown1 = 'temp2_qarth_analytics_drilldownx1'
        self.temp2_top_product_attributes = 'temp2_top_product_attributes'
        self.qarth_product_daily = 'qarth_product_daily'
        self.partition2_offset = 2 #min value is 1, for most recent partition
        self.pars = self.getMostRecentPartitions(self.db_catint, self.qarth_product_daily)
        self.dateid1 = self.pars[0]
        self.dateid2 = self.pars[1] #dateid2 is more recent than dateid1
        print '[dateid1, dateid2]: ', self.dateid1, ' : ', self.dateid2
        self.priority_level = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']
        self.split_size = "51200000000"
        self.heap_size = "24G"
        self.setOptionsAll = (
                        "SET mapred.job.priority=" + self.priority_level[0] + ";" 
                        "SET mapred.max.split.size=" + self.split_size + ";" 
                        "SET mapred.child.java.opts=-server -Xmx" + self.heap_size + ";" 
                        "SET mapred.map.child.java.opts=-server -Xmx" + self.heap_size + ";" 
                        "SET mapred.reduce.child.java.opts=-server -Xmx" + self.heap_size + ";" 
                        "SET hive.exec.compress.output=true;" 
                        "SET mapred.compress.map.output=true;" 
                        "SET mapred.output.compress=true;" 
                        "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;" 
                        
                        #Vectorized (batched) operations:
                        "set hive.vectorized.execution.enabled = true;" 
                        "set hive.vectorized.execution.reduce.enabled = true;"
                        
                        #Cost Based Optimization:
                        #"set hive.cbo.enable=true;"
                        #"set hive.compute.query.using.stats=true;"
                        #"set hive.stats.fetch.column.stats=true;"
                        #"set hive.stats.fetch.partition.stats=true;"
                        #"analyze table temp2_product_attributes compute statistics;"
                        "SET MAPR_MAPREDUCE_MODE=yarn;"
                        "\n\n"
                    )
        self.setOptions = (
                        "SET mapred.job.priority=" + self.priority_level[0] + ";" 
                        "SET mapred.max.split.size=" + self.split_size + ";" 
                        "SET mapred.child.java.opts=-server -Xmx" + self.heap_size + ";" 
                        "SET mapred.map.child.java.opts=-server -Xmx" + self.heap_size + ";" 
                        #"SET hive.auto.convert.join.noconditionaltask=false;" +  ## map joins...
                        "SET mapred.reduce.child.java.opts=-server -Xmx" + self.heap_size + ";"
                        
                        #Vectorized (batched) operations:
                        "set hive.vectorized.execution.enabled = true;" 
                        "set hive.vectorized.execution.reduce.enabled = true;"
                        "SET MAPR_MAPREDUCE_MODE=yarn;"
                        "\n\n"
                    )
        self.setOptionsPriority = (
                        "SET mapred.job.priority=" + self.priority_level[0] + ";"
                        "SET MAPR_MAPREDUCE_MODE=yarn;"
                        "\n\n"
                    )
        self.setup_proxy()
    
    def getMostRecentPartitions(self, db, table_name):
        query = "show partitions " + db + "." + table_name + ";"
        partitions_s = self.execHiveCheck(query, self.retry_count)
        if self.debug_queries_only == 1:
            return ("1", "2")
        partitions = partitions_s.split("\n")
        print "[tableName1]: ", table_name, ", [numOfPartitions_raw] : ", len(partitions), ", [partitions_raw]: ", partitions
        if partitions[len(partitions) - 1].strip() == '':
            partitions = partitions[0:-1] #remove empty entry
        numOfPartitions = len(partitions)
        print "[tableName2]: ", table_name, ", [numOfPartitions] : ", numOfPartitions, ", [partitions]: ", partitions
        partition2 = numOfPartitions - self.partition2_offset
        partition1 = partition2 - 1 #partition2 is more recent than partition1
        if (partition1 < 0) or (partition2 < 0):
            sys.exit("Error: Not enough Partitions present")      
        par1 = partitions[partition1].split('=')[1]
        par2 = partitions[partition2].split('=')[1]
        return (par1, par2)
    
    def setup_proxy(self):
        #export https_proxy="http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
        #export http_proxy="http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
        https_proxy = "http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
        http_proxy = "http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
        os.environ['http_proxy'] = http_proxy
        os.environ['HTTP_PROXY'] = http_proxy
        os.environ['https_proxy'] = https_proxy
        os.environ['HTTPS_PROXY'] = https_proxy
        command = "export PYSPARK_PYTHON=/usr/bin/python27"
        self.execCmdOS(command) 
        
    def execCmdOS(self, command):
        print '[execCmdOS]: ', command
        self.queries += command + "\n\n"
        try:
            os.system(command)     
        except Exception as e:
            print 'Exception thrown: ', e
            #sys.exit(1)
            
    def execHiveFile(self, filename):
        command = "/usr/local/bin/hive -f \"" + filename + "\""
        self.queries += command + "\n\n"
        print '[COMMAND]:  ', command
        try:
            subprocess.call(command, shell=True)
        except Exception as e:
            print "[error]:  An Error has occurred: execHiveFile, ", e
        return None
            
    def execHiveCall(self, query):
        try:
            self.queries += query + "\n\n"
            command = "/usr/local/bin/hive -e \"" + query + "\""
            print '[COMMAND]:  ', command
            subprocess.call(command, shell=True)
        except Exception as e:
            print "[error]:  An Error has occurred: execHiveQuerySUB", e
            #sys.exit(1)
        return None
    
    def execPostgres(self, query):
        self.queries += query + "\n\n"
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
    
    def execHiveCallRetry(self, query, retry_count):
        self.queries += query + "\n\n"
        command = "/usr/local/bin/hive -e \"" + query + "\""
        print '[COMMAND]:  ', command
        try:
            subprocess.call(command, shell=True)
        except Exception as e:
            print "[error]:  An Error has occurred: execHiveQuerySUB, ", 'retry_count: ', retry_count, ', msg: ', e
            if retry_count > 0:
                self.execHiveCallRetry(query, retry_count - 1)
            else:
                #pass
                sys.exit(1)
        return None
    
    def execHiveCheck(self, query, retry_count):
        self.queries += query + "\n\n"
        cmd = "/usr/local/bin/hive"
        option1 = '-e'
        option2 = query
        command = [cmd, option1, option2]
        print '[COMMAND]:  ', ", ".join(command)
        result = ''
        try:
            result = subprocess.check_output(command)
        except Exception as e:
            print 'ERROR: Exception thrown: Hive query failed', e
            if retry_count > 0:
                self.execHiveCheck(query, retry_count -1)
            else:
                sys.exit(1)
        return result
    
    def execCmdCheck(self, cmd, options=[]):
        command = [cmd] + options
        self.queries += cmd + "\n\n"
        print '[COMMAND]:  ', command
        result = ''
        try:
            result = subprocess.check_output(command)
        except Exception as e:
            print 'ERROR: Exception thrown: Hive query failed', e
            #sys.exit(1)
        return result
    
    def droptable(self, db, table_name):
        table_name = table_name + self.test_suffix
        query = "drop table if exists " + db + "." + table_name + ";"
        self.execHiveCheck(query, self.retry_count)
        
    def droptableExt(self, db, table_name):
        table_name = table_name + self.test_suffix
        cmd = 'hadoop fs -rm -r maprfs:/usr/catint/' + table_name + ';'
        self.execCmdCheck(cmd)
        
    def createTempTableProdAttr(self, db, table_name):
        table_name = table_name + self.test_suffix
        query =  ("CREATE table " + db + "." + table_name + " (" +
        "product_id string, " +
        "source string, " +
        "updated_at string, " +
        "product_attributes string, " + 
        "org_id string"
        ") " +
        "LOCATION " +
        "'maprfs:/user/" + db + "/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def createTempTableOrgId(self, db, table_name):
        table_name = table_name + self.test_suffix
        query =  ("CREATE  table " + db + "." + table_name + " (" +
        "org_id string, "
        "seller_name string"
        ") " +
        "LOCATION " +
        "'maprfs:/user/" + db + "/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def insertIntoTempTableOrgId(self, db, src_table, tgt_table, dateid):
        tgt_table = tgt_table + self.test_suffix
        query = ("INSERT INTO table " + db + "." + tgt_table + " " + 
                 "SELECT type_id AS org_id, display_name AS seller_name " + 
                 "FROM " + db + "." + src_table + " WHERE date_id='" + dateid + "';")
        
        query = self.setOptions + query
        self.execHiveCheck(query, self.retry_count)
        
    def insertTempTableProdAttr(self, db, dateid1, table_insert, table_select, day, is_scaled):
        table_insert = table_insert + self.test_suffix
        table_select = table_select ## qarth_product_daily (same for prod and dev envs)
        if self.debug_limit:
            lmt = " limit " + str(self.debug_limit_count)
        else:
            lmt = " "
            
        fast_clause = ' '
        scaling_factor = 100 * 1000  ## 10k leads to 32k records in temp2_product_attributes table
        if is_scaled == 'True':
            fast_clause = "and (hash(get_json_object(product_json, '$.product_id')) % " + str(scaling_factor) + " = 0) "
             
            
        query = ("INSERT into table " + db + "." + table_insert + "  " +
        "SELECT " + 
        "get_json_object(product_json, '$.product_id') AS product_id, " + 
        "COALESCE(get_json_object(product_json, '$.source'), 'unknown') AS source, " +
        "get_json_object(product_json, '$.updated_at') AS updated_at, " + 
        "CONCAT('{ \"pcf\" : ', get_json_object(product_json, '$.product_attributes'), ', \"day\":\"" + day + '"}\'' + ')  as product_attributes, '  +
        "COALESCE(get_json_object(product_json, '$.meta.org_id'), 'unknown') AS org_id " +
        "FROM " + db + "." + table_select + " " +
        "WHERE (date_id='" + dateid1 + "') and " + 
        "(get_json_object(product_json, '$.tenant_id')='0') " +
        fast_clause +
        " "  + ###this clause is needed to avoid duplicate inserts by hive: https://github.com/elastic/elasticsearch-hadoop/issues/448
        lmt +
        ";")
        
        query = self.setOptions + query
        self.execHiveCheck(query, 1)
        
    def createTempTableProductIds(self, db, table_name):
        table_name = table_name + self.test_suffix
        query = ("CREATE  table " + db + "." + table_name + " (product_id string) " +
                "LOCATION " +
                "'maprfs:/user/catint/" + table_name + "'" + ";")
        self.execHiveCheck(query, self.retry_count)

    def getProductsCreatedDeletedCount(self, db, create_delete):  
        table_name1 = 'temp2_product_ids1' + self.test_suffix
        table_name2 = 'temp2_product_ids2' + self.test_suffix
        if create_delete == "created":
            query = ("SELECT count(S2.product_id) FROM " +
            "(SELECT product_id " +
            "FROM " + db + "." + table_name2 + ")S2 " +
            "LEFT OUTER JOIN " +
            "(SELECT product_id " +
            "FROM " + db + "." + table_name1 + ")S1 " +
            "ON (S1.product_id = S2.product_id) " +
            "WHERE S1.product_id is null;")
        elif create_delete == 'deleted':
            query = ("SELECT count(S1.product_id) FROM " +
            "(SELECT product_id " +
            "FROM " + db + "." + table_name2 + ")S2 " +
            "LEFT OUTER JOIN " +
            "(SELECT product_id " +
            "FROM " + db + "." + table_name1 + ")S1 " +
            "ON (S1.product_id = S2.product_id) " +
            "WHERE S2.product_id is null;")  
        else:
            print "[getProductsCreatedDeletedCount/Error]: Operation not allowed! create_delete: ", create_delete
            sys.exit(1)

        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count) 
        if results.strip() == '':
            results = 0
        return results 
    
    def insertIntoTempProductIds(self, db, table_name, dateid):
        table_name = table_name + self.test_suffix
        query = 'use catint; TRUNCATE TABLE  ' + table_name + ';'  ##works with retry logic
        query += ("INSERT INTO table " + db + "." + table_name + " " +
                 "SELECT get_json_object(product_json, '$.product_id') as product_id " +
                 "FROM catint.qarth_product_daily " +
                 "WHERE (date_id='" + dateid + "') AND " +
                 "get_json_object(product_json, '$.tenant_id') = '0';")
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count) 
        return results 
        
    def createTempTableCounts1(self, db, table_name):
        table_name = table_name + self.test_suffix
        query =  ("CREATE  table " + db + "." + table_name + " (" +
        "fqkey string) " +
        "ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY '\\t' " +
        "LOCATION " +
        "'maprfs:/user/" + db + "/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def createTempTableCounts2(self, db, table_name):
        table_name = table_name + self.test_suffix
        query =  ("CREATE  table " + db + "." + table_name + " (" +
        "fqkey2 string) " +
        "ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY '\\t' " +
        "LOCATION " +
        "'maprfs:/user/" + db + "/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def createTempTableCounts3(self, db, table_name):
        table_name = table_name + self.test_suffix
        query =  ("CREATE  table " + db + "." + table_name + " (" +
        "fqkey3 string," + 
        "product_type string," + 
        "source string," + 
        "org_id string," + 
        "update_ string," + 
        "insert_ string," + 
        "delete_ string," + 
        "marker string" + 
        ") " +
        "ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY '\\t' " +
        "LOCATION " +
        "'maprfs:/user/" + db + "/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def createTempTableTopProductAttributes(self, db, table_name):     
        table_name = table_name + self.test_suffix
        query =  ("CREATE table IF NOT EXISTS " + db + "." + table_name + " (" +
        "current_date timestamp, " +
        "top_json string " +
        ") " +
        "ROW FORMAT DELIMITED " +
        "FIELDS TERMINATED BY '\\t' " +
        "LOCATION " +
        "'maprfs:/hive/" + db + ".db/" + table_name + "';")
        self.execHiveCheck(query, self.retry_count)
        
    def insertTempTableTopProductAttributes(self, db, current_date,  top_attributes_dynamic_map, top_attributes_dynamic_tableau_map, 
                                            top_attributes_static_map, 
                                            top_attributes_identification_map, top_attributes_size_map, top_sources_map, 
                                            top_product_types_map, totalCountAttributes, 
                                            total_crud_counts_map, topAttrDictL2,
                                            product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready):  
          
        table_name = 'temp2_top_product_attributes' + self.test_suffix
        top_json = {
                    'top_attributes_dynamic' : top_attributes_dynamic_map,
                    'top_attributes_dynamic_tableau' : top_attributes_dynamic_tableau_map,
                    'top_attributes_static' : top_attributes_static_map,
                    'top_attributes_identification' : top_attributes_identification_map,
                    'top_attributes_size' : top_attributes_size_map,
                    'top_sources' : top_sources_map,
                    'top_product_types_map' : top_product_types_map,
                    'total_count_attributes' : totalCountAttributes.replace('\n', ''),
                    'total_crud_counts_map' : total_crud_counts_map,
                    'topAttrDictL2' : topAttrDictL2,
                    'topProdTypeDictL2' : topProdTypeDictL2,
                    'product_ids_created_count' : product_ids_created_count.replace('\n', ''),
                    'product_ids_deleted_count' : product_ids_deleted_count.replace('\n', ''),
                    'data_ready' : data_ready
                    }
        print '[top_json]: ', top_json
        #top_json_s = json.dumps(top_json)
        top_json_s = str(top_json)
        print '[top_json_s1]: ', top_json_s
        top_json_s = top_json_s.replace('\\t', '').replace('\\n', '') #'\t' is used as a delimiter in table def...
        print '[top_json_s2]: ', top_json_s
        top_json_s_64 = base64.b64encode(top_json_s)
        top_json_s_64 = "'" + top_json_s_64 + "'"
        print '[top_json_s1_64]: ', top_json_s_64
        #top_json_s = top_json_s.replace("'", "")  ##added on 2018-02-09
        #top_json_s = top_json_s.replace('""', '"')  ##added on 2018-04-11
        
#         top_json_s = top_json_s.replace('"\\"', '"') ##added on 2018-04-11  
#         print '[top_json_s3]: ', top_json_s
#         top_json_s = top_json_s.replace('\\""', '"')  ##added on 2018-04-11  
#         print '[top_json_s4]: ', top_json_s
        ###do not truncate this table!
        query = ("INSERT INTO  table " + db + "." + table_name + " " +
                "SELECT S.current_date, S.top_json from " + 
                "( select from_unixtime(unix_timestamp('" + current_date + "', 'yyyyMMdd')) as current_date, " + 
                top_json_s_64 + " as top_json " +
                   ")S;"
                )   
        print '[total_crud_counts_map_s1]: ', total_crud_counts_map
        print '[query1]: ', query
        query = self.setOptions + query
        #self.execHiveCallRetry(query, 1) ##do not truncate this table 
        self.execHiveCallRetry(query, self.retry_count) ##do not truncate this table 
        
    def insertTempTableCounts1(self, db, table_insert, table_select):  
        
        table_insert = table_insert + self.test_suffix
        table_select = table_select + self.test_suffix
          
        udfFile = self.udf_path + "/" + "product_top_attributes_udf" + ".py"  ##same udf file for prod and dev
        deleteCommand = "delete file " + udfFile + "; "
        addCommand = "add file " + udfFile + "; "
        
        query1 = deleteCommand + addCommand + self.setOptionsAll
        #----------------------------------------------------------------------------------
        if self.is_data_validation == 'False':  ##normal way
            selectClause =   "SELECT TRANSFORM(S.pas[0], S.pas[1], S.product_id) "
            whereClause = " "
            groupByClause = "GROUP BY product_id "
        else:   ##test case
            selectClause =   "SELECT TRANSFORM(S.pas[0], S.pas[1], S.product_id) "
            #whereClause = "where product_id in ('6XP3Y90VCYQ0') "
            whereClause = " "
            groupByClause = "GROUP BY product_id "
        #----------------------------------------------------------------------------------
        query2 = 'use catint; TRUNCATE TABLE ' + table_insert + ';'
        query = (query1 + query2 + "INSERT INTO table " + db + "." + table_insert + " " +
        selectClause +
        "USING '/usr/bin/python " + udfFile + "' AS (fqkey string) " +
        "FROM " +
        "(SELECT product_id, COLLECT_SET(product_attributes) pas " +
        "FROM " + db + "." + table_select + " " + 
        whereClause + 
        groupByClause + " HAVING size(pas) > 1)S;")
        self.execHiveCallRetry(query, self.retry_count) 
        
    def insertTempTableCounts2(self, db):    
        table_name2 = 'temp2_product_attributes_counts2' + self.test_suffix
        table_name = 'temp2_product_attributes_counts1' + self.test_suffix
        query = 'use catint; TRUNCATE TABLE ' + table_name2 + ';'   ##works with retry logic
        query += ("INSERT INTO  table " + db + "." + table_name2 + " " +
                 "SELECT S.fqkey2 FROM " +
                 "(SELECT fqkey2 " +
                 "FROM " + db + "." + table_name + " " +
                 "lateral view explode(split(fqkey,',')) fqkey AS fqkey2)S;")
        query = self.setOptions + query
        self.execHiveCheck(query, self.retry_count)  
        
    def insertTempTableCounts3(self, db):    
        table_name3 = 'temp2_product_attributes_counts3' + self.test_suffix
        table_name2 = 'temp2_product_attributes_counts2' + self.test_suffix
        query = 'use catint; TRUNCATE TABLE ' + table_name3 + ';'   ##works with retry logic
        
        query += ("INSERT INTO  table " + db + "." + table_name3 + " " +
                 "SELECT S.fqkey3, S.product_type, S.source, S.org_id, S.update_, S.insert_, S.delete_, S.marker FROM " +
                 "(SELECT " + 
                 "split(fqkey2, '@')[0] as fqkey3, " +
                 "split(fqkey2, '@')[1] as product_type, " +
                 "split(fqkey2, '@')[2] as source, " +       
                 "split(fqkey2, '@')[3] as org_id, " +       
                 "split(fqkey2, '@')[4] as update_, " +       
                 "split(fqkey2, '@')[5] as insert_, " +       
                 "split(fqkey2, '@')[6] as delete_, " +       
                 "split(fqkey2, '@')[7] as marker " +       
                 "FROM " + db + "." + table_name2 + " " +
                 ")S;")
        query = self.setOptions + query
        self.execHiveCheck(query, self.retry_count)  
    
    def getTopStats(self, db, attribute, topAttributes):
        table_name3 = 'temp2_product_attributes_counts3' + self.test_suffix
        lmt = 'LIMIT ' + str(self.top_attributes_limit10)
        if attribute == 'fqkey3':
            lmt = 'LIMIT ' + str(len(self.top_attributes_static))
        
        if topAttributes == "":
            topAttrClause = " "
        elif type(topAttributes) == list:
            top_attribute_keys = ""
            for topAttribute in topAttributes:
                top_attribute_keys += "'" + topAttribute + "',"
            top_attribute_keys2 = top_attribute_keys[0:-1]
            topAttrClause = "AND LOWER(regexp_replace(regexp_replace(fqkey3,' ','_'), '&', 'and')) IN  (" + top_attribute_keys2 + ") "
        else:
            top_attributes_map = self.getTopStatsMap(topAttributes)
            top_attribute_keys = top_attributes_map.keys()
            top_attribute_keys2 = ""
            for top_attribute_key in top_attribute_keys:
                top_attribute_key = top_attribute_key.replace('"', '')
                top_attribute_keys2 = top_attribute_keys2 + "'" + top_attribute_key.lower() + "',"
            top_attribute_keys2 = top_attribute_keys2[0:-1]
            topAttrClause = "AND LOWER(regexp_replace(regexp_replace(fqkey3,' ','_'), '&', 'and')) IN  (" + top_attribute_keys2 + ") "
            
        query = ("SELECT regexp_replace(regexp_replace(" + attribute + ",' ','_'), '&', 'and'), count(" + attribute + ") ct " + 
                 "FROM " + db + "." + table_name3 + " " +
                 "WHERE (fqkey3 is not null) AND (source is not null) AND (product_type is not null) " +
                 "AND (source != 'unknown') AND (product_type != 'unknown') " +
                 "AND (fqkey3 != '') AND (update_ == '1') " +  ###<<<<added 2018-04-20
                 topAttrClause +
                 "GROUP BY " + attribute + " ORDER BY ct DESC " + lmt + ";")
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count)  
        return results
    
    def getTotalCountAttributes(self, db):
        table_name3 = 'temp2_product_attributes_counts3' + self.test_suffix
        query = ('SELECT sum(S.ct) FROM ' +
                '(SELECT fqkey3, count(fqkey3) ct ' +
                'FROM ' + db + '.' + table_name3 + ' ' +
                'WHERE (fqkey3 is not null) AND ' +
                '(source is not null) AND ' +
                '(product_type is not null) AND ' +
                '(source != "unknown") AND ' +
                '(product_type != "unknown") ' +
                "AND (fqkey3 != '') " +
                'GROUP BY fqkey3)S') 
        
        query = "SET mapred.job.priority=" + self.priority_level[0] + ";" + query
        results = self.execHiveCheck(query, self.retry_count)  
        results = 0 if results == 'NULL' else results
        return results
    
    def getStatAndCount(self, line):
        fields = line.split()
        top_stat = fields[0].strip()
        if len(fields) > 1:
            top_stat_count = fields[1].strip()
        else:
            top_stat_count = "0"
        return (top_stat, top_stat_count)
    
    def getTopStatsMap(self, top_stats_s):
        top_stats_map = {}
        unique_stats_set = set()
        top_stats_list = top_stats_s.split("\n")
        for line in top_stats_list:
            line = line.strip()
            if line == "":
                continue
            top_stat, top_stat_count = self.getStatAndCount(line)
            if top_stat not in unique_stats_set:
                #top_stat = '"' + top_stat + '"'  ## enclose keys with double quotes
                top_stats_map[top_stat] = top_stat_count
                unique_stats_set.add(top_stat)  
        return top_stats_map
            
    def write_to_graphite(self, message):
        try:
            print "[INFO] Sending message to %s => %s" % (self.carbon_server, message)
            sock = socket.socket()
            sock.connect((self.carbon_server, int(self.carbon_port)))
            sock.sendall(message)
        except socket.error, exc:
            print "[ERROR] Caught exception socket.error : %s" % exc
        sock.close()
            
    def postGraphite_nc(self):
        #echo "schema value timestamp" | nc graphite.prod.monitoring.cdqarth.prod.walmart.com 2003
        command = ("echo qarth.test.product2.junk 650 epoch_timestamp|" + self.graphite_post_cmd)
        self.execCmdOS(command)   
        
    def getCRUDCountsDB(self,db):
        
        query = ("SELECT count(update_) ct, 'total_update_count' " +
                "FROM " + db + ".temp2_product_attributes_counts3" + self.test_suffix + " " +
                "WHERE update_ = '1' " +
                "UNION ALL " +
                "SELECT count(insert_) ct, 'total_insert_count' " +
                "FROM " + db + ".temp2_product_attributes_counts3" + self.test_suffix + " " +
                "WHERE insert_ = '1' " +
                "UNION ALL " +
                "SELECT count(delete_) ct, 'total_delete_count' " +
                "FROM " + db + ".temp2_product_attributes_counts3" + self.test_suffix + " " +
                "WHERE delete_ = '1';"   
                )  
        
        query = "SET mapred.job.priority=" + self.priority_level[0] + ";" + query
        results = self.execHiveCheck(query, self.retry_count)  
        print '[crudQuery completed]: results: ', results
        lines = results.split('\n')
        crud_counts_s = ''
        for line in lines:
            line = line.replace('\t', ' ')
            fields = line.split()
            if len(fields) > 1:
                name = fields[1].strip()
                count = fields[0].strip()
                if name != '':
                    crud_counts_s += name + '=' + count + '#'
        crud_counts_s = '"' + crud_counts_s + '"'
        return crud_counts_s           
    
    def postHive(self, topAttributes_dynamic, topAttributes_dynamic_tableau, topAttributes_static, topAttributes_identification, 
                 topAttributes_size, top_sources_s, top_product_types_s, 
                 totalCountAttributes, crud_counts_s, topAttrDictL2,
                 product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready):
        current_date = self.dateid2
        print '[get top attributes static]:', topAttributes_static
        top_attributes_dynamic_map = self.getTopStatsMap(topAttributes_dynamic)
        top_attributes_dynamic_tableau_map = self.getTopStatsMap(topAttributes_dynamic_tableau)
        top_attributes_static_map = self.getTopStatsMap(topAttributes_static)
        top_attributes_identification_map = self.getTopStatsMap(topAttributes_identification)
        top_attributes_size_map = self.getTopStatsMap(topAttributes_size)
        print '[top_attributes_static_map]:', top_attributes_static_map
        top_sources_map = self.getTopStatsMap(top_sources_s)
        print '[top_sources_map]:', top_sources_map
        top_product_types_map = self.getTopStatsMap(top_product_types_s)
        print '[top_product_types_map]:', top_product_types_map
        
        top_attributes_static_map_s = json.dumps(top_attributes_static_map)
        top_sources_map_s = json.dumps(top_sources_map)
        top_product_types_map_s = json.dumps(top_product_types_map)
        
        print '[crud_counts]: ', crud_counts_s
        
        if (((current_date!='') and (top_attributes_static_map_s!='{}') and (top_sources_map_s!='{}') and 
            (top_product_types_map_s!='{}') and (totalCountAttributes!='0')) or (self.is_validation == 'True')):
            
                self.insertTempTableTopProductAttributes(self.db, current_date, 
                        top_attributes_dynamic_map, top_attributes_dynamic_tableau_map, top_attributes_static_map, 
                        top_attributes_identification_map, top_attributes_size_map, 
                        top_sources_map, top_product_types_map, 
                        totalCountAttributes, crud_counts_s, topAttrDictL2,
                        product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready)
        else:
            print 'ERROR: hive data looks bad!'
            
    def postHiveF(self, top_attributes_dynamic_map, top_attributes_dynamic_tableau_map, top_attributes_static_map, 
                 top_attributes_identification_map, top_attributes_size_map, top_sources_map, top_product_types_map, 
                 totalCountAttributes, crud_counts_s, topAttrDictL2,
                 product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready):
        
        current_date = self.dateid2
        
        top_attributes_static_map_s = json.dumps(top_attributes_static_map)
        top_sources_map_s = json.dumps(top_sources_map)
        top_product_types_map_s = json.dumps(top_product_types_map)
        
        print '[crud_counts]: ', crud_counts_s
        
        if (((current_date!='') and (top_attributes_static_map_s!='{}') and (top_sources_map_s!='{}') and 
            (top_product_types_map_s!='{}') and (totalCountAttributes!='0')) or (self.is_validation == 'True')):
            
                self.insertTempTableTopProductAttributes(self.db, current_date, 
                        top_attributes_dynamic_map, top_attributes_dynamic_tableau_map, top_attributes_static_map, 
                        top_attributes_identification_map, top_attributes_size_map, 
                        top_sources_map, top_product_types_map, 
                        totalCountAttributes, crud_counts_s, topAttrDictL2,
                        product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready)
        else:
            print 'ERROR: hive data looks bad!'
     
    def getTopStatsFormatted(self, topStatsMap):
        topStatsOutMap = {}
        style_table = 'style="background-color:#F5CBA7;width:100%"'
        style_cell2 = 'style="width:50%"'
        topStatsOutString = (' <div><table border="1" ' + style_table + '><tr><td ' + style_cell2 + '><b>Attribute</b></td>' + 
                            '<td ' + style_cell2 + '><b>Count</b></td></tr>')
        for topStat in topStatsMap:
            value = int(topStatsMap[topStat])
            topStatsOutMap[topStat] = value
        topStatsOutListSorted = sorted(topStatsOutMap.items(), key=lambda x: x[1], reverse=True)
        for key, value in topStatsOutListSorted:
            topStatsOutString += '<tr><td ' + style_cell2 + '>' + key + "</td><td " + style_cell2 + ">" + "{:,}".format(value) + '</td></tr>'
        topStatsOutString += '</table></div> '
        return topStatsOutString    
    
    def getTopStatsFormattedAll(self):
        topAttributes_dynamic = ''
        topAttributes_static = ''
        topAttributes_identification = ''
        topAttributes_size = ''
        topSources = ''
        topProductTypes = ''
        totalCountAttributes = ''
        
        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        print '[current_dates1] ', current_dates
        day1 = current_dates[0]
        print '[day1] ', day1
        
        try:
            topAttributesDynamicMap = self.top_stats_7days[day1]['top_attributes_dynamic']
            print '[topAttributesDynamicMap] ', topAttributesDynamicMap
            topAttributes_dynamic = self.getTopStatsFormatted(topAttributesDynamicMap)
            
            topAttributesStaticMap = self.top_stats_7days[day1]['top_attributes_static']
            print '[topAttributesStaticMap] ', topAttributesStaticMap
            topAttributes_static = self.getTopStatsFormatted(topAttributesStaticMap)
            
            topAttributesIdentificationMap = self.top_stats_7days[day1]['top_attributes_identification']
            print '[topAttributesIdentificationMap] ', topAttributesIdentificationMap
            topAttributes_identification = self.getTopStatsFormatted(topAttributesIdentificationMap)
            
            topAttributesSizeMap = self.top_stats_7days[day1]['top_attributes_size']
            print '[topAttributesSizeMap] ', topAttributesSizeMap
            topAttributes_size = self.getTopStatsFormatted(topAttributesSizeMap)
            
            topSourcesMap = self.top_stats_7days[day1]['top_sources']
            topSources = self.getTopStatsFormatted(topSourcesMap)
            
            topProductTypesMap = self.top_stats_7days[day1]['top_product_types']
            topProductTypes = self.getTopStatsFormatted(topProductTypesMap)
            
            totalCountAttributes2 = self.top_stats_7days[day1]['total_changed_count']
            print '[totalCountAttributes2]: ', totalCountAttributes2
            totalCountAttributes = "{:,}".format(int(totalCountAttributes2))
        except Exception as e:
            print '[Exception: mailNotification] ', e
            sys.exit(1)
        return (topAttributes_dynamic, topAttributes_static, topAttributes_identification, topAttributes_size, 
                topSources, topProductTypes, totalCountAttributes, day1)
    
    def getUpdateCount(self, day):
            total_crud_counts = str(self.top_stats_7days[day].get('total_crud_counts', '####'))
            total_crud_counts = total_crud_counts.replace('"', '').replace("'", "")
            if total_crud_counts != '####':
                total_count_inserts_dummy , total_count_updates, total_count_deletes_dummy = self.getInsertUpdateDelete(total_crud_counts)  ###<<< new
            else:
                total_count_inserts = '-1'
                total_count_updates = '-1' ##missing value
                total_count_deletes = '-1' ##missing value
            return int(total_count_updates)
        
    def get7DayTotalCounts(self):
        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        dates_count = len(current_dates) if len(current_dates) < 7 else 7
        print '[dates_count]: ', dates_count

        if dates_count == 0:
            day1 = 0
            totalCountAttributesDay1 = 0
            totalCountAttributesDay2 = 0
        elif dates_count == 1:
            day1 = current_dates[0]
            #----------------------------------------------------------------------------------
            #totalCountAttributesDay1 = self.top_stats_7days[day1]['total_changed_count']
            totalCountAttributesDay1 = self.getUpdateCount(day1)
            totalCountAttributesDay2 = totalCountAttributesDay1
            #----------------------------------------------------------------------------------
        elif dates_count == 2:
            day1 = current_dates[0]
            day2 = current_dates[1]
            #totalCountAttributesDay1 = self.top_stats_7days[day1]['total_changed_count']
            #totalCountAttributesDay2 = self.top_stats_7days[day2]['total_changed_count']
            totalCountAttributesDay1 = self.getUpdateCount(day1)
            totalCountAttributesDay2 = self.getUpdateCount(day2)
        else:
            day1 = current_dates[0]
            day2 = current_dates[1]
            #totalCountAttributesDay1 = self.top_stats_7days[day1]['total_changed_count']
            #totalCountAttributesDay2 = self.top_stats_7days[day2]['total_changed_count']
            totalCountAttributesDay1 = self.getUpdateCount(day1)
            totalCountAttributesDay2 = self.getUpdateCount(day2)

        totalCountAttributesSum = 0
        for i in range(dates_count):
            #totalCountAttributesSum += int(self.top_stats_7days[current_dates[i]]['total_changed_count'])
            totalCountAttributesSum += self.getUpdateCount(current_dates[i])
        totalCountAttributesAvg = int(totalCountAttributesSum * 1.0/dates_count)
        return (totalCountAttributesDay1, totalCountAttributesDay2, totalCountAttributesAvg)
    
    def getCrudCounts(self):
        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        day1 = current_dates[0]
        total_crud_counts_map = self.top_stats_7days[day1]['total_crud_counts']
        print '[total_crud_counts]: ', total_crud_counts_map
        
        return total_crud_counts_map
      
    def mailNotificationTopStats3(self, is_prod_db, table_size, table_name, partition):
        sender = "rkumar1@walmartlabs.com"
        #receiver_raj = ["rkumar1@walmartlabs.com", 'venky@walmartlabs.com']
        receiver_raj = ["rkumar1@walmartlabs.com"]
        s = smtplib.SMTP('localhost')
        print 'Sending error message to raj', receiver_raj
        subject_loc = is_prod_db + "- failed delivery- prd attrs daily stats"
        msg = ('Failed stats run- missing data for recent partitions: table name: ' + table_name + ', partition: ' + 
        partition + ', table size: ' + str(table_size) + ' TB')
        message = 'Subject: {}\n\n{}'.format(subject_loc, msg)
        s.sendmail(sender, receiver_raj, message)   
        s.quit()
        
    def mailNotificationTopStats4(self, is_prod_db, msg):
        sender = "rkumar1@walmartlabs.com"
        receiver_raj = ["rkumar1@walmartlabs.com"]
        s = smtplib.SMTP('localhost')
        print 'Sending error message to raj', receiver_raj
        subject_loc = is_prod_db + "- failed delivery- prd attrs daily stats"
        msg = 'Exceptiom thrown: ' + 'is_prod_db: ' + is_prod_db + '. ' + str(msg)
        message = 'Subject: {}\n\n{}'.format(subject_loc, msg)
        s.sendmail(sender, receiver_raj, message)   
        s.quit()
        
    def getHistStats(self, statName):
        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        print '[current_dates__]: ', current_dates, ', statName: ', statName 
        if len(current_dates) > 0:            
            current_date = current_dates[0]
        else:
            print '[No Data for Top Stats! System Exiting...]'
            sys.exit(1)
        previous_date = current_dates[1] if len(current_dates) > 1 else current_date
            
        stat_current_date = self.top_stats_7days[current_date][statName] ##<<s   
        stat_previous_date = self.top_stats_7days[previous_date][statName] ##<<s   
        stat_7days = 0
        for current_date in current_dates:
            stat_7days += int(self.top_stats_7days[current_date][statName])
        stat_7days_avg = stat_7days/len(current_dates)
        
        stat_current_date_f = "{:,}".format(int(stat_current_date))
        stat_previous_date_f = "{:,}".format(int(stat_previous_date))
        stat_7day_avg_f = "{:,}".format(int(stat_7days_avg))
        print '[stat_current_date_f__]: ', stat_current_date_f
        print '[stat_previous_date_f__]: ', stat_previous_date_f
        print '[stat_7day_avg_f__]: ', stat_7day_avg_f
        return (stat_current_date_f, stat_previous_date_f, stat_7day_avg_f)
    
    def getHistAttrs(self, attrType, color):
        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        print '[current_dates__]: ', current_dates, '[attrType]: ', attrType
        if len(current_dates) == 0:            
            print '[No Data for Top Stats! System Exiting...]'
            sys.exit(1)
        
        attrList = (self.top_stats_7days[current_dates[0]][attrType]).keys()
        print '[attrList_1]: ', attrList
        style_table = 'style="background-color:' + color + ';width:100%"'
        style_cell5 = 'style="width:20%"'
        
        attr_map = {}   
        for attr in attrList:
            attr_list = []
            attr_list2 = []
            for date_ in current_dates:
                attrVal = self.top_stats_7days[date_][attrType].get(attr, '0')
                attr_list.append(int(attrVal))
            avg = sum(attr_list)/len(current_dates)
            if len(attr_list) < 1:
                attr_list2 = [0, 0, 0] ##for empty list, assign 0s
            elif len(attr_list) < 2:
                attr_list2 = [attr_list[0], attr_list[0], 0] ##use day1 value for day2 too
                avg = attr_list[0]
                delta = 0
            else:
                delta = -1 #dealing with integers only
                if attr_list[1] != 0:
                    delta = (attr_list[0] - attr_list[1]) * 100.0 / attr_list[1]
                attr_list2 = attr_list[0:2]
                attr_list2.append(delta)   
            attr_list2.append(avg) ## order: today. previous day, delta, average  
            attr_map[attr] = attr_list2
        attr_s = '<table border="1" ' + style_table + '>'
        key_max_size = 0
        for attr in attrList:
            if len(attr) > key_max_size:
                key_max_size = len(attr)
        
        if attrType == 'top_sources':
            topStat = 'Source'
        elif attrType == 'top_product_types':
            topStat = 'Product Type'
        else:
            topStat = 'Attribute'
                    
        attr_s += ('<tr><td ' + style_cell5 + '><b>' + topStat + '</b></td><td ' + style_cell5 + '><b>Today</b></td>' + 
                   '<td ' + style_cell5 + '><b>Previous Day</b></td>' + 
                       '<td ' + style_cell5 + '><b>Last 7 Days Average</b></td><td ' + style_cell5 + '><b>Delta</b></td></tr>')
        
        for key in attr_map:
            today = "{:,}".format(attr_map[key][0])
            previous_day = "{:,}".format(attr_map[key][1])
            delta = 'NA'
            if attr_map[key][2] != -1:
                delta = "{0:.2f}".format(attr_map[key][2]) + '%'
                print '[attr_map[key][2]: ', attr_map[key][2]
            average = "{:,}".format(attr_map[key][3])

            attr_s += ('<tr><td ' + style_cell5 + '>' + key + '</td><td ' + style_cell5 + '>' + 
                       today + '</td><td ' + style_cell5 + '>'+ previous_day +
                       '</td><td ' + style_cell5 + '>' + average + '</td><td ' + style_cell5 + '>' + delta + '</td></tr>')
        attr_s += "</table>"
        return attr_s 

    def mailNotificationTopStats2(self, is_prod_db, post_hive):
        (topAttributes_dynamic, topAttributes_static_deprecated, topAttributes_identification_deprecated, 
         topAttributes_size_deprecated, topSources_deprecated, topProductTypes_deprecated, 
         totalCountAttributes, day1) = self.getTopStatsFormattedAll()  
         
        print "[self.top_stats_7days[day1]]: ", self.top_stats_7days[day1]
        print "[self.top_stats_7days[day1]['top_attributes_dynamic']]: ", self.top_stats_7days[day1]['top_attributes_dynamic']
        topAttributes_dynamic_list = self.top_stats_7days[day1]['top_attributes_dynamic'].keys()
        print '[topAttributes_dynamic_list__]: ', topAttributes_dynamic_list, '[day1]: ', day1
        ##Note: can not post historical data for dynamic attributes, since the attributes change with time
        topAttributes_static = self.getHistAttrs('top_attributes_static', '#AED6F1')
        #topAttributes_static = self.getHistAttrs('top_attributes_static', '#EAFAF8')
        topAttributes_size = self.getHistAttrs('top_attributes_size', '#A3E4D7')
        topAttributes_identification = self.getHistAttrs('top_attributes_identification', '#A2D9CE')
       
        topSources = self.getHistAttrs('top_sources', '#F7DC6F')
        topProductTypes = self.getHistAttrs('top_product_types', '#F0B27A')

        current_dates = sorted(self.top_stats_7days.keys(), reverse=True)
        print '[current_dates__]: ', current_dates               
        day1 = current_dates[0]
        print 'day1__: ', day1
        style_table1 = 'style="background-color:#AED6F1;width:100%"'
        style_table2 = 'style="background-color:#F0B27A;width:100%"'
        style_table3 = 'style="background-color:#AED6F1;width:100%"'
        style_cell2 = 'style="width=50%"'
        style_cell3 = 'style="width=33%"'
        style_cell5 = 'style="width=20%"'
        
        top_attr_l2 = self.top_stats_7days[day1]['top_attr_l2']    
        top_prod_type_l2 = self.top_stats_7days[day1]['top_prod_type_l2']  
          
        product_ids_created_count_current, product_ids_created_count_previous, product_ids_created_count_avg = self.getHistStats('product_ids_created_count')
        product_ids_deleted_count_current, product_ids_deleted_count_previous, product_ids_deleted_count_avg = self.getHistStats('product_ids_deleted_count')
        crud_insert_count_f_CPA = ('<b>Today:</b> ' + product_ids_created_count_current + '<b>  Previous Day:</b> ' + product_ids_created_count_previous + 
                                         '<b>  Last 7 Days Average:</b> ' + product_ids_created_count_avg)  ##CPA: Current, Previous, Average
        crud_delete_count_f_CPA = ('<b>Today:</b> ' + product_ids_deleted_count_current + '<b>  Previous Day:</b> ' + product_ids_deleted_count_previous + 
                                         '<b>  Last 7 Days Average:</b> ' + product_ids_deleted_count_avg)  
         
        data_ready = self.top_stats_7days[day1].get('data_ready', 1)   
        print '[top_attr_l2__]: ', top_attr_l2
        print '[top_prod_type_l2__]: ', top_prod_type_l2
        print '[topAttributes_dynamic]: ', topAttributes_dynamic
        print '[topAttributes_static]: ', topAttributes_static
        print '[topAttributes_identification]: ', topAttributes_identification
        print '[topAttributes_size]: ', topAttributes_size
        
        sender = "rkumar1@walmartlabs.com"
        receiver_raj = ["rkumar1@walmartlabs.com"]
        receiver_dist = self.receiver_dist

        subject = "PRODUCT ATTRIBUTES DAILY STATISTICS"
        s = smtplib.SMTP('localhost')
        
        day1_ymd = day1.replace('00:00:00', '').strip()
        year = day1_ymd[0:4]
        month = day1_ymd[5:7]
        day = day1_ymd[-2:]
        print '[year:month:day]:: ', year, ':', month, ':', day
        msg_break = "<hr>"
        msg_attr_dynamic = '<h3>ATTRIBUTES WITH THE MOST NUMBER OF UPDATES</h3>'
        msg_attr_static = '<h3>MAIN ATTRIBUTES TRACKED DAILY</h3>' 
        msg_attr_identification = '<h3>IDENTIFICATION ATTRIBUTES TRACKED DAILY</h3>'
        msg_attr_size = '<h3>SIZE ATTRIBUTES TRACKED DAILY</h3>'
        msg_src = '<h3>SOURCES WITH THE MOST NUMBER OF UPDATES FOR THE MAIN ATTRIBUTES</h3>'
        msg_prd_type = '<h3>TOP 10 PRODUCT TYPES WITH THE MOST NUMBER OF UPDATES FOR THE MAIN ATTRIBUTES</h3>'
        msg = '<div>'  ##email client already has html/body tags, so in email, the top level tag should be a 'div'
        ##--------------------Global message-----------------------------------------------------------------------   
        if data_ready == 1:
            ##---------------------------------------------------------
            msg += '<h2>PRODUCT ATTRIBUTES DAILY STATISTICS</h2>'
            msg += '<b>TIME PERIOD: </b>' + month + "-" + day + "-" + year + " 6am-6am" + msg_break
            msg += '<h3>SUMMARY STATS</h3>'
            totalCountAttributesDay1i, totalCountAttributesDay2i, totalCountAttributesAvg = self.get7DayTotalCounts()
            print '[totalCountAttributesDay1i]: ', totalCountAttributesDay1i, " : ", '[totalCountAttributesDay2i]: ', totalCountAttributesDay2i
            print '[self.uber_value_attributes_count]: ', self.uber_value_attributes_count
            print '[totalCountAttributesAvg__]: ', totalCountAttributesAvg
            totalCountAttributesDay1 = "{:,}".format(int(totalCountAttributesDay1i))
            totalCountAttributesDay2 = "{:,}".format(int(totalCountAttributesDay2i))
            totalCountAttributesAvg_crud = "{:,}".format(int(totalCountAttributesAvg))
            
            crud_update_count_f_CPA = ('<b>Today: </b>' + totalCountAttributesDay1 + '<b>  Previous Day: </b>' + totalCountAttributesDay2 + 
                                         '<b>  Last 7 Days Average: </b>' + totalCountAttributesAvg_crud) 
            delta_update_f = 'NA'
            delta_update = 'NA'
            if int(totalCountAttributesDay2i) != 0:
                delta_update_f = (int(totalCountAttributesDay1i) - int(totalCountAttributesDay2i)) * 100.0 / int(totalCountAttributesDay2i)
                delta_update = "{0:.2f}".format(delta_update_f) + '%'       
                    
            #######################TopAttrL2###################################################    
            msgTAL2 = '<table border="1" ' + style_table1 + '>'
            msgTAL2 += ('<tr><td ' + style_cell3 + '><b>Top Attribute </b></td><td ' + style_cell3 + 
                        '><b>Top 3 Product Types</b></td>' + '<td ' + style_cell3 + '><b>Top 3 Sources</b></td></tr>') 
            
            for topAttr in top_attr_l2:
                top3ProductTypes = top_attr_l2[topAttr]['top3ProductTypes']
                top3Sources = top_attr_l2[topAttr]['top3Sources']
                top3ProductTypes = top3ProductTypes.replace('#', ', ')
                top3Sources = top3Sources.replace('#', ', ')
                msgTAL2 += ('<tr><td ' + style_cell3 + '>' + topAttr + '</td><td ' + style_cell3 + '>' +
                             top3ProductTypes + '</td><td ' + style_cell3 + '>' + top3Sources + '</td></tr>')
            msgTAL2 += "</table>"
            #print '[msgTAL2__]: ', msgTAL2
            #######################TopProdTypeL2################################################### 
            maxTopProdTypeSizeP, maxTop3OrgIdSizeP, maxTop3SourcesSizeP = self.getMaxSizesTA_P(top_prod_type_l2)
            maxTopProdTypeHeaderSizeP = len('Top Product Type')
            maxTopProdTypeSizeP = maxTopProdTypeSizeP if maxTopProdTypeSizeP > maxTopProdTypeHeaderSizeP else maxTopProdTypeHeaderSizeP
            
            msgTAL2P = '<table border="1" ' + style_table2 + '>'
            msgTAL2P += ('<tr><td ' + style_cell3 + '><b>Top Product Type</b></td><td ' + style_cell3 + 
                         '><b>Top 3 Organizations</b></td><td ' + style_cell3 + '><b>Top 3 Sources</b></td></tr>')
            
            for topProdTypeP in top_prod_type_l2:
                top3OrgIdsP = top_prod_type_l2[topProdTypeP]['top3OrgIds']
                top3SourcesP = top_prod_type_l2[topProdTypeP]['top3Sources']
                top3OrgIdsP = top3OrgIdsP.replace('#', ', ')
                top3SourcesP = top3SourcesP.replace('#', ', ')
                msgTAL2P += ('<tr><td ' + style_cell3 + '>' + topProdTypeP + '</td><td ' + style_cell3 + '>' + 
                             top3OrgIdsP + '</td><td ' + style_cell3 + '>' + top3SourcesP + '</td></tr>')
            msgTAL2P += "</table>"
            #print '[msgTAL2P__]: ', msgTAL2P
            ##########################################################################
            total_crud_counts_map = self.getCrudCounts()
            if total_crud_counts_map != 'NULL':
                product_ids_created_count_current_int = int(product_ids_created_count_current.replace(',', ''))
                product_ids_created_count_previous_int = int(product_ids_created_count_previous.replace(',', ''))
                
                delta_create_f = 'NA'
                delta_create = 'NA'
                if product_ids_created_count_previous_int != 0:
                    delta_create_f = (product_ids_created_count_current_int - product_ids_created_count_previous_int) * 100.0 / product_ids_created_count_previous_int
                    delta_create = "{0:.2f}".format(delta_create_f) + '%'
                        
                crud_counts = ('<b>From PRODUCT CREATE: </b>' + crud_insert_count_f_CPA + " <b>Delta:</b> " + delta_create + "<br>" +
                    '<b>From PRODUCT UPDATE: </b>' + crud_update_count_f_CPA + " <b>Delta:</b> " + delta_update + "<br>" +
                    '<b>From PRODUCT DELETE: </b>' + crud_delete_count_f_CPA + "<br>")
                #---------------------------------------
                top_table1 = ('<table border="1" ' + style_table3 + '><tr>' +
                              '<td ' + style_cell5 + '><b>Stat Name</b></td>' +
                              '<td ' + style_cell5 + '><b>Today</b></td>' + 
                              '<td ' + style_cell5 + '><b>Previous Day</b></td>' +
                              '<td ' + style_cell5 + '><b>Last 7 Days Average</b></td>' + 
                              '<td ' + style_cell5 + '><b>Delta</b></td>' + 
                              '</tr>')
                top_table1 += ('<tr><td ' + style_cell5 + '>TOTAL NUMBER OF UPDATED ATTRIBUTES</td><td ' + style_cell5 + '>' + 
                               str(totalCountAttributesDay1) + '</td><td ' + style_cell5 + '>' + str(totalCountAttributesDay2) + '</td>' + 
                               '<td ' + style_cell5 + '>' + str(totalCountAttributesAvg_crud) + '</td><td ' + 
                               style_cell5 + '>' + delta_update + '</td></tr>')
                
                top_table2 = ('<tr><td ' + style_cell5 + '>From PRODUCT CREATE</td><td ' + style_cell5 + '>' + 
                              product_ids_created_count_current + '</td><td ' + style_cell5 + '>' + product_ids_created_count_previous + 
                              '</td><td ' + style_cell5 + '>' + product_ids_created_count_avg + '</td><td ' + style_cell5 + '>' + 
                              delta_create + '</td></tr>')
                            
                top_table3 = ('<tr><td ' + style_cell5 + '>From PRODUCT UPDATE</td><td ' + style_cell5 + '>' + 
                              totalCountAttributesDay1 + '</td><td ' + style_cell5 + '>' + totalCountAttributesDay2 + 
                              '</td><td ' + style_cell5 + '>' + totalCountAttributesAvg_crud + '</td><td ' + style_cell5 + '>' +
                              delta_update + '</td></tr>')
                        
                top_table4 = ('<tr><td ' + style_cell5 + '>From PRODUCT DELETE</td><td ' + style_cell5 + '>' + 
                              product_ids_deleted_count_current + '</td><td ' + style_cell5 + '>' + product_ids_deleted_count_previous + 
                              '</td><td ' + style_cell5 + '>' + product_ids_deleted_count_avg + '</td><td ' + style_cell5 + '>0%</td></tr>')
                
                top_table5 = ('<tr><td ' + style_cell5 + '>PERCENTAGE OF CATALOG "VALUE" ATTRIBUTES UPDATED</td><td ' + style_cell5 + '>' + 
                              str("{0:.4f}".format(int(totalCountAttributesDay1i) * 100.0/self.uber_value_attributes_count)) + '%' + 
                              '</td><td ' + style_cell5 + '>' + str("{0:.4f}".format(int(totalCountAttributesDay2i) * 100.0/self.uber_value_attributes_count)) + '%' + 
                              '</td><td ' + style_cell5 + '>' +  str("{0:.4f}".format(int(totalCountAttributesAvg) * 100.0/self.uber_value_attributes_count)) + '%' + 
                              '</td><td ' + style_cell5 + '>NA</td></tr>')
                top_table5 += '</table>'
                msg += top_table1 + top_table2 + top_table3 + top_table4 + top_table5
                legend = '<div><b>Legend:</b><BR>Delta = (Today - Previous Day) * 100 % / Previous Day<BR>'
                legend += 'NA: Not Applicable</div>'
                msg += legend
                #---------------------------------------
            msg += msg_break
            if (topAttributes_static != '') or (topSources != '') or (topProductTypes != '') or (totalCountAttributes != ''):
                msg += '<div>' + msg_attr_static + topAttributes_static + '</div><br><div>' + msgTAL2 + '</div>' + msg_break
                msg += '<div>' + msg_attr_identification + topAttributes_identification + '</div>' + msg_break
                msg += '<div>' + msg_attr_size + topAttributes_size + '</div>' + msg_break
                msg += '<div>' + msg_attr_dynamic + topAttributes_dynamic + '</div>' + msg_break
                msg += '<div>' + msg_src + topSources + '</div>' + msg_break
                msg += '<div>' + msg_prd_type + topProductTypes + '</div><br><div>' + msgTAL2P + '</div>' + msg_break
            else:
                print '[NO TOP STATS]'
                return
        ####-----------------End Global------------------------------------------------------------------------------------
        hour = datetime.datetime.now().hour
        week_day = datetime.datetime.now().weekday()
        print '[week_day]:', week_day, ' [current hour]: ', hour, ' [hour_post]: ', self.hour_post
        if is_prod_db == 'True':
            last_report_file = 'last_report.txt'
        else:
            last_report_file = 'last_report_local.txt' 
        print '[last_report_file]: ', last_report_file
##-------------case when data is not ready----------------------            
        with open(last_report_file, "r") as f1:
            last_report_date = f1.read().strip().replace('\\n', '')
            print '[last_report_date]: ', last_report_date, ' [day1_ymd]: ', day1_ymd
        is_new_data = 'True'
        if last_report_date == day1_ymd:
            print '[No new data- analytics report will not be sent to dist list!]'
            msg= '<div><div style="background-color:#f4eb42">NO NEW DATA- analytics report will not be sent to dist list</div>' + msg + '</div>'
            is_new_data = 'False'
##--------------------------------------------------------------- 
        msg += "</div>"
        msg_mime = MIMEMultipart('alternative')    
        html = MIMEText(msg, 'html')
        msg_mime.attach(html)
##--------------------------------------------------------------- 
        #print "[msg___]: ", msg
        with open('logs/cron2/daily_report.html', 'w') as f:
            f.write(msg)
        if (hour == self.hour_post) and (is_prod_db == 'True') and (post_hive == 'False') and (is_new_data == 'True'):
            print 'Sending message to dist list ', receiver_dist
            msg_mime['Subject'] = "Subject: " + subject
            s.sendmail(sender, receiver_dist, msg_mime.as_string())
            #s.sendmail(sender, receiver_raj, msg_mime.as_string())
            with open(last_report_file, "w") as f2:
                f2.write(day1_ymd)
        else:
            print 'Sending message to raj', receiver_raj
            subject_loc = "LOCAL/" + is_prod_db + "- prd attrs daily stats"
            msg_mime['Subject'] = "Subject: " + subject_loc
            s.sendmail(sender, receiver_raj, msg_mime.as_string()) 
               
        if (is_prod_db == 'False'):
            with open(last_report_file, "w") as f2:
                f2.write(day1_ymd)
            
        s.quit()
        return    
        
    def getPaddedStatRight(self, topStat, maxTopStatSize):
        topStatSize = len(topStat)
        margin = 2
        padding = maxTopStatSize + margin - topStatSize
        topStatPadded = ' ' * padding + topStat
        return topStatPadded
    
    def getPaddedStatLeft(self, topStat, maxTopStatSize):
        topStatSize = len(topStat)
        margin = 2
        padding = maxTopStatSize + margin - topStatSize
        topStatPadded = topStat + ' ' * padding
        return topStatPadded
        
    def getMaxSizesTA(self, top_attr_l2):
        maxTopAttrSize = 0
        maxTop3SourcesSize = 0
        maxTop3ProductTypesSize = 0
        for topAttr in top_attr_l2:
            top3ProductTypes = top_attr_l2[topAttr]['top3ProductTypes']
            top3Sources = top_attr_l2[topAttr]['top3Sources']
            top3ProductTypes = top3ProductTypes.replace('#', ', ')
            top3Sources = top3Sources.replace('#', ', ')
            if maxTopAttrSize < len(topAttr):
                maxTopAttrSize = len(topAttr)
            if maxTop3SourcesSize < len(top3Sources):
                maxTop3SourcesSize = len(top3Sources)
            if maxTop3ProductTypesSize < len(top3ProductTypes):
                maxTop3ProductTypesSize = len(top3ProductTypes) 
        return (maxTopAttrSize, maxTop3SourcesSize, maxTop3ProductTypesSize) 
    
    def getMaxSizesTA_P(self, top_prod_type_l2):
        # "topProdTypeDictL2": {"T-Shirts": {"top3OrgIds": "BRAAVOS#Seven_Times_Six#CafePress__Inc.", "top3Sources": "MARKETPLACE_PARTNER#BRAAVOS#CONTENT_STORE"}}
        maxTop3OrgIdSize = 0
        maxTop3SourcesSize = 0
        maxTopProductTypesSize = 0
        for topProdType in top_prod_type_l2:
            top3OrgIds = top_prod_type_l2[topProdType]['top3OrgIds']
            top3Sources = top_prod_type_l2[topProdType]['top3Sources']
            top3OrgIds = top3OrgIds.replace('#', ', ')
            top3Sources = top3Sources.replace('#', ', ')
            if maxTopProductTypesSize < len(topProdType):
                maxTopProductTypesSize = len(topProdType) 
            if maxTop3SourcesSize < len(top3Sources):
                maxTop3SourcesSize = len(top3Sources)
            if maxTop3OrgIdSize < len(top3OrgIds):
                maxTop3OrgIdSize = len(top3OrgIds)
        return (maxTopProductTypesSize, maxTop3OrgIdSize, maxTop3SourcesSize) 
    
    def get_top_stats_hive(self, lmt_count):
        lmt = 'LIMIT ' + str(lmt_count)
        query = ('SELECT current_date, top_json ' +
                 'FROM catint.temp2_top_product_attributes' + self.test_suffix + ' ORDER BY current_date DESC ' + lmt + ';')  
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count) 
        results = results.replace(': ', ':').replace(', ', ',').replace('""', '"')
        #results = re.sub('[0-9][0-9]:[0-9][0-9]:[0-9][0-9]', '', results)
        return results
    
    def generateCSVdata(self, attrType):
        cur_dates = sorted(self.top_stats_all.keys())
        attribute_keys = sorted((self.top_stats_all[cur_dates[0]]).keys())
        attribute_keys_csv = (','.join(attribute_keys)) + '\n'
        attribute_values_csv = ''
        for cur_date in cur_dates:
            attrs_map = self.top_stats_all[cur_date][attrType]
            for key in attribute_keys:
                attribute_values_csv += str(attrs_map.get(key, -1)) + ','
            attribute_values_csv = attribute_values_csv[-1]
            attribute_values_csv += '\n'
        attributes_csv = attribute_keys_csv + attribute_values_csv
        return attributes_csv 
    
    def getAttrKeyValuesPostgres(self, postfix, attr_map):
        keys = []
        values = []
        for key in attr_map:
            if (postfix == '_dy') and (key in self.top_attributes_dynamic_tableau) or (postfix in ['_st', '_iden', '_sz']):
                values.append(str(attr_map.get(key, ' ')))
                keys.append(key + postfix)
        keys_s = ','.join(keys)
        values_s = ','.join(values)
        return (keys_s, values_s)
        
    def generatePostgresQuery(self, postgres_query_file, is_postgres_test):
        curr_dates = sorted(self.top_stats_all.keys())
        print '[curr_dates1]: ', curr_dates
        queries_all = ''
        print '[generatePostgresQuery called]...'
        
        total_count_inserts_previous = None
        total_count_updates_previous = None
        total_count_deletes_previous = None
        product_ids_created_previous = None
        product_ids_deleted_previous = None
        
        for curr_date in curr_dates:
            #---------------------------------------------------------------
            print '[curr_date]::: ', curr_date
            #----------------------------------------------------------------                
            #print '[self.top_stats_all[curr_date]: ', self.top_stats_all[curr_date]
            #print "[self.top_stats_all[curr_date]['top_attributes_static']: ", self.top_stats_all[curr_date]['top_attributes_static']
            ##1. get stattic values
            keys_static, values_static = self.getAttrKeyValuesPostgres('_st', self.top_stats_all[curr_date]['top_attributes_static'])
            keys_static = keys_static.replace('_st', '_main')
            #print '[keys_static, values_static]: ', keys_static, values_static
            ##2. get identification values
            keys_iden, values_iden = self.getAttrKeyValuesPostgres('_iden', self.top_stats_all[curr_date]['top_attributes_identification'])
            #print '[keys_iden, values_iden]: ', keys_iden, values_iden
            ##3. get size values
            keys_size, values_size = self.getAttrKeyValuesPostgres('_sz', self.top_stats_all[curr_date]['top_attributes_size'])
            #print '[keys_size, values_size]: ', keys_size, values_size
            ##4. get dynamic values
            keys_dynamic, values_dynamic = self.getAttrKeyValuesPostgres('_dy', self.top_stats_all[curr_date]['top_attributes_dynamic'])
            #print '[keys_dynamic, values_dynamic]: ', keys_dynamic, values_dynamic
            #
            top_sources = str(self.top_stats_all[curr_date].get('top_sources', '####')) ##separate table
            #top_product_types = str(self.top_stats_all[curr_date]['top_product_types_map'])  ##separate table
            top_product_types = str(self.top_stats_all[curr_date].get('top_product_types_map', '####'))  ##separate table
            top_sources_dummy = '-'
            top_product_types_dummy = '-'
            #print '[top_sources]: ', top_sources
            #print '[top_product_types]: ', top_product_types
            #
            product_ids_created = str(self.top_stats_all[curr_date]['product_ids_created_count'])
            product_ids_deleted = str(self.top_stats_all[curr_date]['product_ids_deleted_count'])
            #print '[product_ids_created]: ', product_ids_created
            #print '[product_ids_deleted]: ', product_ids_deleted
            #
            #total_crud_counts = str(self.top_stats_all[curr_date]['total_crud_counts_map'])
            total_crud_counts = str(self.top_stats_all[curr_date].get('total_crud_counts', '####'))
            total_crud_counts = total_crud_counts.replace('"', '').replace("'", "")
            #total_crud_counts = "u'total_insert_count=16363061#total_update_count=18754011#total_delete_count=2853802#'"
            #[total_crud_counts2]:  "total_update_count=6000208#total_insert_count=12185132#total_delete_count=627887#"
            if total_crud_counts != '####':
                total_count_inserts , total_count_updates, total_count_deletes = self.getInsertUpdateDelete(total_crud_counts)
                #total_count_updates = str(total_crud_counts.split('#')[0].split('=')[1])
                #total_count_deletes = str(total_crud_counts.split('#')[2].split('=')[1])
            else:
                total_count_inserts = '-1'
                total_count_updates = '-1' ##missing value
                total_count_deletes = '-1' ##missing value
            #print '[total_crud_counts2]: ', total_crud_counts
            #print '[total_crud_inserts2]: ', total_count_inserts
            #print '[total_count_updates2]: ', total_count_updates
            #print '[total_count_deletes2]: ', total_count_deletes
            
            total_count_inserts_delta_percent = self.getPercentageDelta(total_count_inserts, total_count_inserts_previous)
            total_count_updates_delta_percent = self.getPercentageDelta(total_count_updates, total_count_updates_previous)
            total_count_deletes_delta_percent = self.getPercentageDelta(total_count_deletes, total_count_deletes_previous)
            product_ids_created_delta_percent = self.getPercentageDelta(product_ids_created, product_ids_created_previous)
            product_ids_deleted_delta_percent = self.getPercentageDelta(product_ids_deleted, product_ids_deleted_previous)
            value_attributes_updated_percent = str("{0:.4f}".format(int(total_count_updates) * 100.0/self.uber_value_attributes_count)) ###
            
            #print ('[deltas]::  ' +  total_count_inserts_delta_percent + ':' + total_count_updates_delta_percent + ':' + 
            #total_count_deletes_delta_percent + ':' + product_ids_created_delta_percent + ':' + product_ids_deleted_delta_percent)
            
            ##----------------------------------------------------------------
            query = ('INSERT INTO TEMP2_QARTH_PRODUCT_DATA_ANALYTICS (curr_date, ' + 
                    keys_static + ',' + keys_iden + ',' + keys_size + ',' + keys_dynamic + ', top_sources, top_product_types' + 
                    ', product_ids_created, product_ids_deleted, total_count_inserts, total_count_updates, total_count_deletes ' +
                    ', product_ids_created_delta_percent, product_ids_deleted_delta_percent, total_count_inserts_delta_percent' + 
                    ',total_count_updates_delta_percent, total_count_deletes_delta_percent, value_attributes_updated_percent' +
                    ') ' + 
                    "VALUES ('" + curr_date + "' ," + 
                    values_static + ',' +  values_iden + ',' + values_size + ',' + values_dynamic + ",'" + top_sources_dummy + "','" + top_product_types_dummy + 
                    "'," + product_ids_created + ',' + product_ids_deleted + ',' + total_count_inserts + ',' + total_count_updates + ',' + total_count_deletes + 
                    "," + product_ids_created_delta_percent + ',' + product_ids_deleted_delta_percent + ',' + total_count_inserts_delta_percent + 
                    ',' + total_count_updates_delta_percent + ',' + total_count_deletes_delta_percent + ',' + value_attributes_updated_percent +
                    ');')
            #print 'query_postgres: ', query
            total_count_inserts_previous = total_count_inserts
            total_count_updates_previous = total_count_updates
            total_count_deletes_previous = total_count_deletes
            product_ids_created_previous = product_ids_created
            product_ids_deleted_previous = product_ids_deleted
            queries_all += query
            self.queries += query
        query_trunc = "TRUNCATE TABLE TEMP2_QARTH_PRODUCT_DATA_ANALYTICS;"
        with open(postgres_query_file, 'w') as f:
            f.write(query_trunc + queries_all)
    
    def getPercentageDelta(self, today, previous):
        if (previous == None) or (float(previous) == 0.0):
            delta = 0.0
        else:
            try:
                delta = (float(today) - float(previous)) * 100.0 / float(previous)
            except Exception as e:
                delta = 0.0
        return str("{0:.2f}".format(delta))
    
    def getInsertUpdateDelete(self, total_crud_counts): 
        total_crud_counts_fields = total_crud_counts.split('#')
        total_crud_counts_dict = {}
        for total_crud_counts_field in total_crud_counts_fields:
            fields = total_crud_counts_field.split('=')
            if len(fields) > 1:
                total_crud_counts_dict[fields[0]] = fields[1]
        return (total_crud_counts_dict.get('total_insert_count', '-1'), 
                total_crud_counts_dict.get('total_update_count', '-1'), 
                total_crud_counts_dict.get('total_delete_count', '-1'))
    
    def writePostgres(self):
        # requires python2.6 - run as a separate cron job
        with open(self.postgres_query_file, 'r') as f:
            query = f.read()
        self.execPostgres(query) 
      
    def read_top_stats2(self, num_days):
        top_stats_results_hive = self.get_top_stats_hive(num_days)
        top_stats_results_hive_array = top_stats_results_hive.split('\n')
        i = 0
        for entry in top_stats_results_hive_array:
            if (entry.strip() == '') or ('date_id' in entry):
                continue
            if (i > num_days):
                break
            fields = entry.split('\t')
            fields[1] = fields[1].replace("'", "")
            #----------------------------------------------------
            try:
                current_date = fields[0].strip() 
                top_json_raw = fields[1].strip()
                if current_date < '2018-04-11 00:00:00':
                    top_json = json.loads(top_json_raw)
                else:  
                    top_json_64 = base64.b64decode(top_json_raw)
                    top_json = eval(top_json_64)
                    #print '[current_date2]: ', current_date, '   [top_json_raw/base64decoded]: ', top_json_raw
                    #print '[current_date2]: ', current_date, '   [top_json_64/base64decoded]: ', top_json_64
                    #print '[current_date2]: ', current_date, '   [top_json/base64decoded]: ', top_json
                    
                if current_date=='NULL' or top_json=='NULL':
                    continue
                top_sources = top_json['top_sources']
                top_product_types = top_json['top_product_types_map']
                total_changed_count = top_json['total_count_attributes']
                total_crud_counts = top_json['total_crud_counts_map']
                top_attr_l2 = top_json['topAttrDictL2']
                top_prod_type_l2 = top_json.get('topProdTypeDictL2', None)
                top_attributes_dynamic = top_json.get('top_attributes_dynamic', None)
                top_attributes_static = top_json.get('top_attributes_static', None)
                top_attributes_identification = top_json.get('top_attributes_identification', None)
                top_attributes_size = top_json.get('top_attributes_size', None)
                data_ready = top_json.get('data_ready', 1)
                product_ids_created_count = top_json.get('product_ids_created_count', None)
                product_ids_deleted_count = top_json.get('product_ids_deleted_count', None)    
                if ((top_attributes_dynamic!={}) and (top_sources!={}) and (top_product_types!={}) and ('NULL' not in current_date)):
                    memberVar = ''
                    if num_days == 7:
                        memberVar = self.top_stats_7days
                    else:
                        memberVar = self.top_stats_all
                    #self.top_stats_7days[current_date] = {
                    memberVar[current_date] = {
                                           'top_attributes_dynamic' : top_attributes_dynamic,
                                           'top_attributes_static' : top_attributes_static,
                                           'top_attributes_identification' : top_attributes_identification,
                                           'top_attributes_size' : top_attributes_size,
                                           'top_sources' : top_sources, 
                                           'top_product_types' : top_product_types,
                                           'total_changed_count' : total_changed_count,
                                           'total_crud_counts' : total_crud_counts,
                                           'top_attr_l2' : top_attr_l2,
                                           'top_prod_type_l2' : top_prod_type_l2,
                                           'product_ids_created_count' : product_ids_created_count,
                                           'product_ids_deleted_count' : product_ids_deleted_count,
                                           'data_ready' : data_ready
                                           }
            except Exception as e:
                print 'read_top_stats2/Exception: ', e
                sys.exit(1)
            i = i + 1

    def get_uber_attributes_count(self, db):
        #set  product_attributes_jsonb = "{}" in the udf, and run the full script.
        table_name = 'temp2_product_attributes_counts3' + self.test_suffix
        query = ("SELECT count(fqkey3) as uberAttributesCount " + 
                 "FROM " + db + "." + table_name + " " +
                 "WHERE " +
                 "(fqkey3 is not null) AND " +
                 "(product_type is not null) AND " +
                 "(source is not null);")
        
    def get_uber_attribute_types_count(self):
        #set  product_attributes_jsonb = "{}" in the udf, and run the full script.
        query = ("SELECT count(S.fqkey) as uberAttributeTypesCount FROM " +
                    "(SELECT DISTINCT fqkey3 as fqkey FROM temp2_product_attributes_counts3_test " +
                    "WHERE " +
                    "(fqkey3 is not null) AND " +
                    "(product_type is not null) AND " +
                    "(source is not null) " +
                    ")S;")
        
    def checktableSizesAll(self):
        command1 = 'hadoop fs -du -s -h /user/catint/temp2_product_attributes' + self.test_suffix
        command2 = 'hadoop fs -du -s -h /user/catint/temp2_product_attributes_counts1' + self.test_suffix
        command3 = 'hadoop fs -du -s -h /user/catint/temp2_product_attributes_counts2' + self.test_suffix
        command4 = 'hadoop fs -du -s -h /user/catint/temp2_product_attributes_counts3' + self.test_suffix
        result1 = self.execCmdOS2(command1)
        result2 = self.execCmdOS2(command2)
        result3 = self.execCmdOS2(command3)
        result4 = self.execCmdOS2(command4)
        print '[temp2_product_attributes' + self.test_suffix + ' table size]: ', result1, ' [expected size is around 2.0TB]'
        print '[temp2_product_attributes_counts1' + self.test_suffix + '  table size]: ', result2, ' [expected size is around 6.0MB]'
        print '[temp2_product_attributes_counts2' + self.test_suffix + '  table size]: ', result3, ' [expected size is around 9.0MB]'
        print '[temp2_product_attributes_counts3' + self.test_suffix + '  table size]: ', result4, ' [expected size is around 12.0MB]'
        
    def checktableSizeAttr(self, table_name):
        command = 'hadoop fs -du -s -h /user/catint/' + table_name + self.test_suffix
        result = self.execCmdOS2(command)
        print '[checktableSizeAttr/result]: ', table_name, " : ", result
        try:
            result = result.replace(' ', '', 1)
            table_size = result.split()[0]
        except Exception as e:
            print 'Exception caught/checktableSize: ', e
            sys.exit(1)
        return table_size
    
    def checktableSize(self, table_name):
        command = 'hadoop fs -du -s -h /user/catint/' + table_name + self.test_suffix
        result = self.execCmdOS2(command)
        print '[checktableSize/result]: ', table_name, " : ", result
        try:
            result = result.replace(' ', '', 1)
            table_size = result.split()[0]
        except Exception as e:
            print 'Exception caught/checktableSize: ', e
            sys.exit(1)
        return table_size
    
    def execCmdOS2(self, command):
        #useful when need to run in the same process due to authorization issues
        #for example, running hadoop commands
        print '[execCmdOS2]: ', command
        try:
            result = os.popen(command).read()   
        except Exception as e:
            print 'Exception thrown: ', e
            sys.exit(1)    
        return result
    
    def execCmdOS3(self, command):
        #useful when need to run in the same process due to authorization issues
        #for example, running hadoop commands
        print '[execCmdOS3]: ', command
        try:
            result = commands.getoutput(command)
        except Exception as e:
            print 'Exception thrown: ', e
            sys.exit(1)       
        return result
    
    def execHiveCheck2(self):
        cmd = "/usr/local/bin/hive"
        option1 = '-e'
        command = [cmd, option1, 'ANALYZE', 'table', 'qarth_product_daily', 'PARTITION(date_id=20171221)', 'COMPUTE', 'STATISTICS', 'noscan']
        print '[COMMAND]:  ', ", ".join(command)
        result = ''
        try:
            result = subprocess.check_output(command)
        except Exception as e:
            print 'ERROR: Exception thrown: Hive query failed', e
            sys.exit(1)
        return result
    
    def getSize(self, result):
        size = 0.0
        lines = result.split("\n")
        for line in lines:
            if 'totalSize' not in line:
                continue
            else:
                size = float(line.split(',')[2].split('=')[1])/2**40
                break
        return size
            
    def checkQarthtableSizes(self, is_prod_db):
        query1 = "/usr/local/bin/hive -e 'ANALYZE table qarth_product_daily PARTITION(date_id=" + self.dateid1 + ") COMPUTE STATISTICS noscan;'"
        query2 = "/usr/local/bin/hive -e 'ANALYZE table qarth_product_daily PARTITION(date_id=" + self.dateid2 + ") COMPUTE STATISTICS noscan;'"
        #Partition catint.qarth_product_daily{date_id=20171221} stats: [numFiles=2145, numRows=-1, totalSize=2008477955636, rawDataSize=-1]
        result1 = self.execCmdOS3(query1)
        result2 = self.execCmdOS3(query2)
        print '[checkQarthtableSizes/result1]: ', result1
        print '[checkQarthtableSizes/result2]: ', result2
        print '[qarth_product_daily partitions]: ', self.dateid1, ' : ', self.dateid2
        sz1 = 0.0
        sz2 = 0.0
        try:
            sz1 = self.getSize(result1)
            print '[qarth_product_daily size1]: ', sz1, ' TB'
            if sz1 < 0.99:
                self.errorMessage(is_prod_db, sz1, 'qarth_product_daily', self.dateid1)
        except Exception as e:
            print '[Exception/checkQarthtableSizes1]: ', self.dateid1, " : ", e
            self.errorMessage(is_prod_db, sz1, 'qarth_product_daily', self.dateid1)
        try:
            sz2 = self.getSize(result2)
            print '[qarth_product_daily size2]: ', sz2, ' TB'
            if sz2 < 0.99:
                self.errorMessage(is_prod_db, sz2, 'qarth_product_daily', self.dateid2)
        except Exception as e:
            print '[Exception/checkQarthtableSizes2]: ', self.dateid2, " : ", e
            self.errorMessage(is_prod_db, sz2, 'qarth_product_daily', self.dateid2)

    def errorMessage(self, is_prod_db, sz, table_name, partition):
        print '    ERROR: QARTH PRODUCT DAILY table PARTITION NOT FULL SIZE, partition: ', partition
        self.mailNotificationTopStats3(is_prod_db, sz, table_name, partition)
        sys.exit(1)   
    
    def getTopAttributesList(self, topAttributes):
        topAttributes = topAttributes.split('\n')
        topAttributeList = []
        for line in topAttributes:
            line = line.replace('\t', ' ').strip()
            if line == '':
                continue
            topAttribute = line.split()[0]
            #print '[getTopAttributesList/topAttribute1]: ', topAttribute
            topAttributeList.append(topAttribute)
        return topAttributeList    
            
    def getTopAttrDictL2Fast(self, topAttributesList, is_prod_db):  ##fast method (new)
        sources_hive = self.getTopSrcsPrdTypesDictL2Fast(self.db, 'source', topAttributesList)
        #print '[sources_hive_]: ', sources_hive
        sources_dict = self.processHiveDataAttrDictL2Fast(sources_hive)
        print '[sources_dict_]: ', sources_dict
        sources_dict_sorted = self.getTop3SourcesPrdTypesAttrDictL2Fast(sources_dict)
        print '[sources_dict_sorted_]: ', sources_dict_sorted
        
        product_types_hive = self.getTopSrcsPrdTypesDictL2Fast(self.db, 'product_type', topAttributesList)
        #print '[product_types_hive_]: ', product_types_hive
        product_types_dict = self.processHiveDataAttrDictL2Fast(product_types_hive)
        ##print '[product_types_dict_]: ', product_types_dict  -- large output
        product_types_dict_sorted = self.getTop3SourcesPrdTypesAttrDictL2Fast(product_types_dict)
        print '[product_types_dict_sorted_]: ', product_types_dict_sorted
        
        for topAttr in topAttributesList:
            topSourcesList1 = sources_dict_sorted.get(topAttr, ['not available'])
            topSourcesList = "#".join(topSourcesList1)
            
            topProductTyesList1 = product_types_dict_sorted.get(topAttr, ['not available'])
            topProductTypesList = "#".join(topProductTyesList1)
            
            self.topAttrDictL2[topAttr] = {'top3Sources': topSourcesList, 'top3ProductTypes': topProductTypesList}
            
    def getTopProdTypeDictL2Fast(self, topProductTypesList, is_prod_db):
        sources_hive = self.getTopSrcsOrgIdsDictL2Fast(self.db, 'source', topProductTypesList)
        sources_dict = self.processHiveDataAttrDictL2Fast(sources_hive)  ##use same method as for Attributes..
        sources_dict_sorted = self.getTop3SourcesPrdTypesAttrDictL2Fast(sources_dict)  ##use same method as for Attributes..
        
        org_ids_hive = self.getTopSrcsOrgIdsDictL2Fast(self.db, 'org_id', topProductTypesList)
        org_ids_dict = self.processHiveDataAttrDictL2Fast(org_ids_hive)  ##use same method as for Attributes..
        org_ids_dict_sorted = self.getTop3SourcesPrdTypesAttrDictL2Fast(org_ids_dict)  ##use same method as for Attributes..
        
        topOrgIdsList_all = []
        for topProdType in topProductTypesList:
            topOrgIdsList_all.extend(org_ids_dict_sorted.get(topProdType, ['not available']))
        
        #--------------------------
        topOrgIdsList_all = list(set(topOrgIdsList_all))
        topOrgIdsList_dict_all = self.mapToSellersName2(topOrgIdsList_all)  ####get all sellers names
        print '[topOrgIdsList_dict_all]: ', topOrgIdsList_dict_all
        #--------------------------
        
        for topProdType in topProductTypesList:
            topSourcesList1 = sources_dict_sorted.get(topProdType, ['not available'])
            topSourcesList = "#".join(topSourcesList1)
            topOrgIdsList1 = org_ids_dict_sorted.get(topProdType, ['not available'])
            #--------------------------
            sellers_names_list = []
            for org_id in topOrgIdsList1:
                sellers_names_list.append(topOrgIdsList_dict_all.get(org_id, 'unknown'))
            topOrgIdsList = "#".join(sellers_names_list)
            topOrgIdsList = topOrgIdsList.replace("'", "")     
            self.topProdTypeDictL2[topProdType] = {'top3Sources': topSourcesList, 'top3OrgIds': topOrgIdsList}
     
    def mapToSellersName2(self, topOrgIdsList):  
        topOrgIdsList_s = "('"
        topOrgIdsList_s += "','".join(topOrgIdsList)
        topOrgIdsList_s += "')"
        query = ("select translate(display_name, ' ', '_') as sellers_name, COALESCE(type_id, 'unknown') as org_id " +
                    "from catint.uber_meta_data_daily " + 
                    "where (date_id='" + self.dateid2 + "') and (type_id in " + topOrgIdsList_s + ");")
        
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count)
        print '[mapToSellersName2/results]: ', results
        lines = results.split("\n")
        topSellersNames = {}
        for line in lines:
            fields = line.replace('\t', ' ').split()
            if len(fields) != 2:
                continue
            topSellersNames[fields[1]] = fields[0]
        return topSellersNames  
    
    def getCounts3AttrsF(self):
        for line in self.temp2_product_attributes_counts3_lines:
            fields = line.strip().split('\t')
            if len(fields) != 8:
                continue
            attr = fields[0]
            product_type = fields[1]
            source = fields[2]
            org_id = fields[3]
            update_ = 0 if fields[4] == 'NULL' else fields[4]
            insert_ = 0 if fields[5] == 'NULL' else fields[5]
            delete_ = 0 if fields[6] == 'NULL' else fields[6]
            marker = fields[7]
            if (attr == 'unknown') or (attr == ''):
                continue
            self.addAttrF(attr)
            self.addSourceF(attr, source)
            self.addProductTypeF(attr, product_type)
            #----------------------------------------------------------------
            #use addOrgId1F() to save memory...
            self.addOrgId1F(attr, org_id)  ## self.attrsMap[attr]['org_id'] = org_id...
            #self.addOrgId2F(attr, org_id)  ## self.attrsMap[attr]['org_id'] = [org_id...]
            #----------------------------------------------------------------
            self.addUpdateF(attr, update_)
            self.addInsertF(attr, insert_)
            self.addDeleteF(attr, delete_)
        i = 0
        for attr in self.top_attributes_static:
            print "[attr, self.attrsMap[attr]['org_id']]:  ", attr, ':', self.attrsMap[attr]['org_id']
            
        for attr in self.attrsMap:
            if i < 3:
                print '[attrsMap/getCounts3AttrsF]: ', attr, self.attrsMap[attr]
                ##[attrsMap/getCounts3AttrsF]:  display_resolution {'product_type': {'default': 3, 
                #'Digital_Cameras': 3, 'Prepaid_Calling_Cards': 1, 'Digital_Audio_Players': 1, 
                #'Laptop_Computers': 1}, 'org_id': '4481eade-7ffb-48b3-ba27-09b25e209894', 
                #'insert_': 0, 'source': {'CONTENT_STORE': 9}, 'delete_': 3, 'update_': 6}
            i = i + 1
     
    def pruneCounts3(self, topAttributes_dynamic):
        temp2_product_attributes_counts3_lines_pruned = []
        print_once = 0
        try:
            for line in self.temp2_product_attributes_counts3_lines:
                fields = line.strip().split('\t')
                if len(fields) != 8:
                    continue
                if print_once < 2:
                    print '[line/pruneCounts3]: ', line, ' : ', type(line)
                    print '[fields/pruneCounts3]: ', fields
                    print_once += 1
                attr = fields[0]
                if attr in (self.top_attributes_static + 
                            self.top_attributes_size +
                            self.top_attributes_identification +
                            self.top_attributes_dynamic_tableau +
                            topAttributes_dynamic.keys()):
                    temp2_product_attributes_counts3_lines_pruned.append(line)
        except Exception as e:
            print '[Exception/pruneCounts3]: ', e, ' : ', temp2_product_attributes_counts3_lines_pruned
        self.temp2_product_attributes_counts3_lines = temp2_product_attributes_counts3_lines_pruned
                
    def postToHiveInMemory(self, post_hive, is_new_data, data_ready, is_prod_db, is_postgres, is_postgres_test):  
      if (post_hive == 'True') and (is_new_data == 'True') and (data_ready == 1):   #cron1 job
        topAttributes_dynamic =  " "
        topAttributes_dynamic_tableau =  " "
        opAttributes_static = " " 
        topAttributes_identification =  " "
        topAttributes_size = 0
        topSources =  " "
        topProductTypes =  " "
        totalCountAttributes =  " "
        crud_counts_s = " " 
        product_ids_created_count =  0
        product_ids_deleted_count =  0
        ###--------------------------------------------------------------
        table_name = 'temp2_product_attributes_counts3' + self.test_suffix
        query = ("select * from catint." + table_name + " where " + 
        "(fqkey3 != '') AND " +
        "(fqkey3 is not null)  AND " +
        "(source is not null)  AND " +
        "(product_type is not null)  AND " +
        "(source != 'unknown')  AND " +
        "(product_type != 'unknown') " +
        ";")
        try:  
            print '[getting data from temp2_product_attributes_counts3 table...]'       
            query = self.setOptions + query
            results = self.execHiveCheck(query, self.retry_count)
            self.temp2_product_attributes_counts3_lines = results.split("\n")
            print '[lines0-10]: \n', self.temp2_product_attributes_counts3_lines[0:10]
            self.getCounts3AttrsF()
            print '[returned from getCounts3AttrsF]'
            topAttributes_dynamic = self.getTopAttrsDynamicF()
            print '[topAttributes_dynamic]: ', topAttributes_dynamic
            #-------------------------------------------------------------
            ##optionally, prune the self.temp2_product_attributes_counts3_lines list for perf, if needed
            self.pruneCounts3(topAttributes_dynamic)
            print '[returned from pruneCounts3]'
            #-------------------------------------------------------------
            topAttributes_dynamic_tableau = self.getTopAttrsStaticF(self.top_attributes_dynamic_tableau)
            print '[topAttributes_dynamic_tableau]: ', topAttributes_dynamic_tableau
            topAttributes_static = self.getTopAttrsStaticF(self.top_attributes_static)
            print '[topAttributes_static]: ', topAttributes_static
            topAttributes_identification = self.getTopAttrsStaticF(self.top_attributes_identification)
            print '[topAttributes_identification]: ', topAttributes_identification
            topAttributes_size = self.getTopAttrsStaticF(self.top_attributes_size)
            print '[topAttributes_size]: ', topAttributes_size
        except Exception as e:
            print "Exception caught/getTopAttributes_: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            sys.exit(1) 
        #------------------------------------------------------------------
        try:
            print '[getting topSources...]'
            topSources = self.getTopStatsF('source')
            print '[topSources]: ', topSources
            topProductTypes = self.getTopStatsF('product_type')
            print '[topProductTypes]: ', topProductTypes
        except Exception as e:
            print "Exception caught/getTopSources: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            #sys.exit(1)  
        #------Level2 attributes-------------------------------------------------------- 
        try:
            print '[getting topAttrDictL2...]'
            topAttrDictL2 = self.getTopAttrDictL2F()
            print '[topAttrDictL2]: ', topAttrDictL2
        except Exception as e:
            print "Exception caught/getTopAttrDictL2Fast: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            #sys.exit(1) 
        ###Level2 product types----------------------------------------------
        try:
            print '[getting topProductTypeDictL2...]'
            topProdTypeDictL2 = self.getTopProdTypeDictL2F(topProductTypes)
            print '[topProdTypeDictL2]: ', topProdTypeDictL2
        except Exception as e:
            print "Exception caught/getTopAttributesList: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            #sys.exit(1) 
        #---------------------------------------------------------------------------
        try:
            print '[getting total counts...]'
            totalCountAttributes = str(self.getTotalCountAttributesF())
            crud_counts_s = str(self.getCrudCountsF())
            print '[crud_counts_s]: ', crud_counts_s
        except Exception as e:
            print "Exception caught/getTotalCountAttributes: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            #sys.exit(1) 
        #----------------------------------------------------------------------
        try:
            print '[getting product ids created/deleted...]'
            product_ids_created_count = str(self.getProductsCreatedDeletedCount(self.db, 'created'))  ##use existing method
            print '[product_ids_created_count]: ', product_ids_created_count
            product_ids_deleted_count = str(self.getProductsCreatedDeletedCount(self.db, 'deleted'))
            print '[product_ids_deleted_count]: ', product_ids_deleted_count
        except Exception as e:
            print "Exception caught/getProductsCreatedDeletedCount: ", e
            self.mailNotificationTopStats4(is_prod_db, e)
            #sys.exit(1) 
        #------------------------------------------------------------------
        try:
            print '[posting to Hive...]'
            self.postHiveF(topAttributes_dynamic, topAttributes_dynamic_tableau, topAttributes_static, topAttributes_identification, topAttributes_size, 
                            topSources, topProductTypes, totalCountAttributes, crud_counts_s, topAttrDictL2, 
                            product_ids_created_count, product_ids_deleted_count, topProdTypeDictL2, data_ready)
        except Exception as e:
            self.mailNotificationTopStats4(is_prod_db, e)
            print "Exception caught/postHive1: ", e
            #sys.exit(1)  
        try:  ##generate query for postgres db
            print '[posting to Postgres...]'
            if is_postgres == 'True':
                self.read_top_stats2(self.postgres_number_of_days) ##last 30 days
                self.generatePostgresQuery(self.postgres_query_file, is_postgres_test)  
        except Exception as e:
            print "Exception caught/postgres1: ", e
            #sys.exit(1)

    def getProductTypeStatsF(self, product_type_tgt, attr_tgt, orgIdSellerNameMap):
        stats = {}
        top3stats = []
        for line in self.temp2_product_attributes_counts3_lines:
            try:
                fields = line.strip().split('\t')
                if len(fields) != 8:
                    continue
                attr = fields[0]
                product_type = fields[1]
                source = fields[2]
                org_id = fields[3]
                update_ = fields[4]
                insert_ = fields[5]
                delete_ = fields[6]
                marker = fields[7]
                if ((product_type != product_type_tgt) or (attr not in self.top_attributes_static) or 
                    (source == 'unknown') or (org_id == 'unknown')):
                        continue
                
                if attr_tgt == 'source':
                    stats[source] = stats.get(source, 0) + 1
                elif (attr_tgt == 'org_id') and (orgIdSellerNameMap.get(org_id, 'unknown') != 'unknown'):
                    stats[org_id] = stats.get(org_id, 0) + 1
            except Exception as e:
                print '[Exception/getProductTypeStatsF]: ', e
                print '[line2]: ', line
                print '[fields2]: ', fields
        ##--------------------------------------------------        
        try:
            stats_sorted = []
            #print '[stats/getProductTypeStatsF1]: ', stats
            stats_sorted = sorted(stats.items(), key=lambda x:x[1], reverse=True)[0:3]
            print '[stats__sorted/getProductTypeStatsF]: ', stats_sorted
            for tp in stats_sorted:
                top3stats.append(tp[0])
            print '[top3stats/getProductTypeStatsF2]: ', top3stats
            if (attr_tgt == 'org_id') and ('unknown' in top3stats):
                print '[top3stats with unknown]: ', top3stats
        except Exception as e:
            print '[Exception/getProductTypeStatsF2]: ', e
            print '[stats_sorted]: ', stats_sorted
            print '[top3stats]: ', top3stats
            
        return top3stats      

    def getTopProdTypeDictL2F(self, topProductTypes):
        #self.topProdTypeDictL2[topProdType] = {'top3Sources': topSourcesList, 'top3OrgIds': topOrgIdsList}
        topProductTypeDictL2 = {}
        topOrgIdsAll = []
        ##1. get org_id to seller_name mapping
        ##----------------------------------------------------------------
        table_name = 'catint.temp2_org_ids' + self.test_suffix
        #query = "select org_id, seller_name from " + table_name + " where org_id in " + org_ids_string + ";"
        query = "select org_id, seller_name from " + table_name + ";"
        query = self.setOptionsPriority + query
        print '[query/orgIds]: ', query[0:400]    
        results = self.execHiveCheck(query, self.retry_count)  
        lines = results.splitlines()
        orgIdSellerNameMap = {}
        print_once = 0
        for line in lines:
            try:
                fields = line.split('\t')
                if len(fields) < 2:
                    continue
                org_id = fields[0]
                seller_name = fields[1]
                seller_name = seller_name.replace(' ', '_').replace(',', '_')
                orgIdSellerNameMap[org_id] = seller_name
            except Exception as e:
                print 'Exception thrown10: ', e
                print '[line4]: ', line
                print '[fields4]: ', fields
         
        print '[created orgId:seller_name map]'   
         
        for product_type in topProductTypes:
            try:
                top3Sources = self.getProductTypeStatsF(product_type, 'source', orgIdSellerNameMap)
                top3Sources = "#".join(top3Sources)
                top3OrgIds = self.getProductTypeStatsF(product_type, 'org_id', orgIdSellerNameMap)
                top3OrgIdsToSellers = []
            except Exception as e:
                print 'Exception thrown11: ', e
                print '[top3Sources: ]', top3Sources
            for org_id in top3OrgIds:
                seller_name = orgIdSellerNameMap.get(org_id, 'unknown')
                top3OrgIdsToSellers.append(seller_name)
            top3OrgIdsToSellers = "#".join(top3OrgIdsToSellers)
            ##2. update top3OrgIds
            topProductTypeDictL2[product_type] = {'top3Sources' : top3Sources, 'top3OrgIds' : top3OrgIdsToSellers}
        return topProductTypeDictL2
        
    def getTopAttrDictL2F(self):
        #self.topAttrDictL2[topAttr] = {'top3Sources': topSourcesList, 'top3ProductTypes': topProductTypesList}
        topAttrDictL2 = {}
        for key in self.top_attributes_static:
            #print '[getTopAttrDictL2F/1]', key
            source_map = self.attrsMap[key]['source']
            #print '[getTopAttrDictL2F/2]', source_map
            #print '[source_map_]: ', source_map
            product_type_map = self.attrsMap[key]['product_type']
            #print '[getTopAttrDictL2F/3]', product_type_map
            #print '[product_type_map_]: ', product_type_map
            topSourcesList1 = self.getTop3SourcesProductTypesF(source_map)
            #print '[getTopAttrDictL2F/4]', topSourcesList1
            topSourcesList = "#".join(topSourcesList1)
            #print '[topSourcesList]: ', topSourcesList
            topProductTypesList1 = self.getTop3SourcesProductTypesF(product_type_map)
            #print '[getTopAttrDictL2F/5]', topProductTypesList1
            topProductTypesList = "#".join(topProductTypesList1)
            #print '[getTopAttrDictL2F/6]', topProductTypesList
            #print '[topProductTypesList]: ', topProductTypesList
            topAttrDictL2[key] = {'top3Sources': topSourcesList, 'top3ProductTypes': topProductTypesList}
        return topAttrDictL2
    
    def getTop3SourcesProductTypesF(self, source_product_type_map):    
        #print '[source_product_type_map]: ', source_product_type_map
        source_product_type_sorted_list = sorted(source_product_type_map.items(), key=lambda x:x[1], reverse=True)
        print '[source_product_type_sorted_list]: ', source_product_type_sorted_list[0]
        source_product_types = []
        try:
            source_product_types.append(source_product_type_sorted_list[0][0])
            source_product_types.append(source_product_type_sorted_list[1][0])
            source_product_types.append(source_product_type_sorted_list[2][0])
        except Exception as e:
            print '[Exception/getTop3SourcesProductTypesF1]: ', e
            print '[Exception/getTop3SourcesProductTypesF2]: ', source_product_type_sorted_list
        
        return source_product_types
                    
    def getTopStatsF(self, attr):
        top_stats_map = {}
        for key_ in self.attrsMap:
            if key_ in self.top_attributes_static:
                stats_map = self.attrsMap[key_][attr]
                for key_ in stats_map:
                    count = top_stats_map.get(key_, 0)
                    top_stats_map[key_] = count + stats_map[key_]
        top_stats_reverse_sorted = sorted(top_stats_map.items(), key=lambda x:x[1], reverse=True)[0:10]
        top_stats_map10 = dict(top_stats_reverse_sorted)
        return top_stats_map10                  
   
    def getTotalCountAttributesF(self):
        update_count = 0
        for key in self.attrsMap:
            update_count += int(self.attrsMap[key]['update_'])
        return update_count
    
    def getCrudCountsF(self):
        update_count = 0
        insert_count = 0
        delete_count = 0
        for key in self.attrsMap:
            update_count += int(self.attrsMap[key]['update_'])
            insert_count += int(self.attrsMap[key]['insert_'])
            delete_count += int(self.attrsMap[key]['delete_'])
        crud_counts = ("total_update_count=" + str(update_count) + "#total_insert_count=" + 
                       str(insert_count) + "#total_delete_count=" + str(delete_count))
        return '"' + crud_counts + '"'
    
    def getTopAttrsStaticF(self, static_list):
        attrs_static_map = {}
        for attr in static_list:
            if attr in self.attrsMap:
                attrs_static_map[attr] = int(self.attrsMap[attr]['update_'])
        return attrs_static_map
    
    def getTopAttrsDynamicF(self):
        attrs_dynamic_map = {}
        for attr in self.attrsMap:
                attrs_dynamic_map[attr] = int(self.attrsMap[attr]['update_'])
        attrs_dynamic_map_top = {}
        i = 0       
        for key, value in sorted(attrs_dynamic_map.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            attrs_dynamic_map_top[key] = value
            i += 1
            if i > 10:
                break
        return attrs_dynamic_map_top

                     
    def addAttrF(self, attr):
        if self.attrsMap.get(attr, 0) == 0:
            self.attrsMap[attr] = {}
            self.attrsMap[attr]['source'] = {}
            self.attrsMap[attr]['product_type'] = {}
            self.attrsMap[attr]['org_id'] = None
            self.attrsMap[attr]['update_'] = None
            self.attrsMap[attr]['insert_'] = None
            self.attrsMap[attr]['delete_'] = None
            
    def addOrgId1F(self, attr, org_id):
        self.attrsMap[attr]['org_id'] = org_id ##not saving full list due to mem limitation
        
    def addOrgId2F(self, attr, org_id):
        ##this method may lead to memory overflow..., but this org_id info is not used anyway
        if attr not in self.top_attributes_static:
            return
        try:
            if type(self.attrsMap[attr].get('org_id', 0)) != list:
                self.attrsMap[attr]['org_id'] = [org_id]
            else:   #existing org_id - update
                current_org_id_list = self.attrsMap[attr].get('org_id', [])
                self.attrsMap[attr]['org_id'] = current_org_id_list.append(org_id)
        except Exception as e:
            print '[Exception/addOrgId2F]: ', e, ' : ', attr, ' : ', org_id, ' : ', type(current_org_id_list)
      
    def getCurrentCountF(self, count):
        if count is None:
            current_count = 0
        else: 
            current_count = int(count)  
        return current_count
    
    def addUpdateF(self, attr, update_):
        current_count = self.getCurrentCountF(self.attrsMap[attr].get('update_', 0))
        self.attrsMap[attr]['update_'] = current_count + int(update_)
        
    def addInsertF(self, attr, insert_):
        current_count = self.getCurrentCountF(self.attrsMap[attr].get('insert_', 0))
        self.attrsMap[attr]['insert_'] = current_count + int(insert_)
        
    def addDeleteF(self, attr, delete_):
        current_count = self.getCurrentCountF(self.attrsMap[attr].get('delete_', 0))
        self.attrsMap[attr]['delete_'] = current_count + int(delete_)
    
    def addSourceF(self, attr, source):
        if self.attrsMap[attr]['source'].get(source, None) == None:  #new source - insert
            self.attrsMap[attr]['source'][source] = 1
        else:   #existing source - update
            current_count = self.getCurrentCountF(self.attrsMap[attr]['source'].get(source, 0))
            self.attrsMap[attr]['source'][source] = current_count + 1
        
    def addProductTypeF(self, attr, product_type):
        if self.attrsMap[attr]['product_type'].get(product_type, 0) == None:  #new product_type - insert
            self.attrsMap[attr]['product_type'][product_type] = 1
        else:   #existing product_type - update
            current_count = self.getCurrentCountF(self.attrsMap[attr]['product_type'].get(product_type, 0))
            self.attrsMap[attr]['product_type'][product_type] = current_count + 1
                     
    def getTopSrcsOrgIdsDictL2Fast(self, db, attribute_for, topAttributesList):
        table_name3 = 'temp2_product_attributes_counts3' + self.test_suffix
        attr_list = '('
        for attr in topAttributesList:
            attr_list += "'" + attr + "',"
        attr_list = attr_list[0:-1] + ")"
    
        topAttrClause = "AND (product_type IN " + attr_list + ") "
            
        query = ("SELECT regexp_replace(regexp_replace(product_type,' ','_'), '&', 'and'), " + attribute_for + 
                 ", count(product_type) ct " + 
                 "FROM " + db + "." + table_name3 + " " +
                 "WHERE (fqkey3 is not null) AND (source is not null) AND (product_type is not null) " +
                 "AND (source != 'unknown') AND (product_type != 'unknown') " +
                 topAttrClause +
                 "GROUP BY product_type, " + attribute_for + ";")
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count)  
        return results
        
    def getTopSrcsPrdTypesDictL2Fast(self, db, attribute_for, topAttributesList):
        table_name3 = 'temp2_product_attributes_counts3' + self.test_suffix
        attr_list = '('
        for attr in topAttributesList:
            attr_list += "'" + attr + "',"
        attr_list = attr_list[0:-1] + ")"
    
        #topAttrClause = "AND (fqkey3 IN " + attr_list + ") "
        topAttrClause = "AND (LOWER(fqkey3) IN " + attr_list + ") "
            
        query = ("SELECT regexp_replace(regexp_replace(fqkey3,' ','_'), '&', 'and'), " + attribute_for + 
                 ", count(fqkey3) ct " + 
                 "FROM " + db + "." + table_name3 + " " +
                 "WHERE (fqkey3 is not null) AND (source is not null) AND (product_type is not null) " +
                 "AND (source != 'unknown') AND (product_type != 'unknown') " +
                 topAttrClause +
                 "GROUP BY fqkey3, " + attribute_for + ";")
        query = self.setOptions + query
        results = self.execHiveCheck(query, self.retry_count)  
        return results
    
    def processHiveDataAttrDictL2Fast(self, results):
        results_list = results.split("\n")
        dict_attr = {}
        for line in results_list:
            line = line.replace('\t', ' ')
            fields = line.split()
            if len(fields) != 3:
                continue
            entry = {fields[1] : int(fields[2])}
    
            if fields[0] not in dict_attr:
                dict_attr[fields[0]] = entry
            else:
                current_entry = dict_attr[fields[0]]
                current_entry.update(entry)
                entry.update(current_entry)
                dict_attr[fields[0]] = entry
                
        return dict_attr
    
    def getTop3SourcesPrdTypesAttrDictL2Fast(self, d):
        results = {}
        for key in d:
            stats_dict = d[key]
            ds = sorted(stats_dict.items(), key=lambda value: value[1], reverse=True)
            if len(ds) > 3:
                ds = ds[0:3]
            results[key] = [tpl[0] for tpl in ds]
        return results
        
    def get_last_report_date(self, last_report):
        last_report = last_report.strip().replace('\\t', ' ')
        fields = last_report.split()
        if len(fields) > 1:
            last_report_date = fields[0].replace('-', '')
        else:
            print '[ERROR]: hive table temp2_top_product_attributes data corrupted!'
            sys.exit(1)
        print '[last_report_date__]: ', last_report_date
        return last_report_date
    
    def getCommandLineArguments(self):
        write_new_attr = sys.argv[1] # use a new partition or use old saved tables
        write_new_count = sys.argv[2] # use a new partition or use old saved tables
        post_hive = sys.argv[3] # use a new partition or use old saved tables
        mail_distn = sys.argv[4]  # if True mail to distribution, otherwise, mail to local
        is_prod_db = sys.argv[5]  # use prod tables or test tables
        is_scaled = sys.argv[6]  # used to get full or scaled data into temp2_product_attributes
        is_new_ids = sys.argv[7]  # create new ids tables for New Created PCFs
        is_postgres = sys.argv[8]  # generate csv data
        return (write_new_attr, write_new_count, post_hive, mail_distn, is_prod_db, is_scaled, is_new_ids, is_postgres)
    
    def setTestSuffix(self, is_prod_db):
        if is_prod_db=='True':
            self.test_suffix = ''
        else:
            self.test_suffix = '_test'
            
    def getLastReportDate(self, is_prod_db, dateid2):
        last_report_file = 'last_report.txt' if is_prod_db == 'True' else 'last_report_local.txt'
        print '[last_report_file]: ', last_report_file
        with open(last_report_file, 'r') as f1:
            last_report_date = f1.read().replace('-', '').replace('\\n', '').strip()
        print '[Checking for new data...] ', ' last_report_date: ', last_report_date, ' dateid2: ', dateid2
        return last_report_date
    
    def insertOrgIds(self, is_new_ids, is_new_data, is_prod_db):
        if (is_new_ids == 'True') and (is_new_data == 'True'):
            try:
                self.droptable(self.db, "temp2_org_ids")
                self.createTempTableOrgId(self.db, "temp2_org_ids")
                self.insertIntoTempTableOrgId(self.db, "uber_meta_data_daily", 
                            "temp2_org_ids", self.dateid2) #use either dateid1 or dateid2
            except Exception as e:
                print "Exception caught0: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1)      
    
    def writeNewAttributes(self, write_new_attr, is_new_data, is_prod_db, is_scaled, data_ready):
        if (write_new_attr == 'True') and (is_new_data == 'True'):  ###cron1 job
            try:
                self.checkQarthtableSizes(is_prod_db)
                self.droptable(self.db, self.temp2_product_attributes)
                self.createTempTableProdAttr(self.db, self.temp2_product_attributes)
                
                self.insertTempTableProdAttr(self.db, self.dateid2, self.temp2_product_attributes, 
                                             self.qarth_product_daily, 'today', is_scaled)
                self.insertTempTableProdAttr(self.db, self.dateid1, self.temp2_product_attributes, 
                                             self.qarth_product_daily, 'yesterday', is_scaled)
                    
                table_size2 = self.checktableSizeAttr('temp2_product_attributes')
                print '[temp2_product_attributes table_size2A]: ', table_size2
                table_size2 = float(table_size2[0:-1])
                print '[temp2_product_attributes table_size2B]: ', table_size2
                self.checktableSizesAll()
                if (is_scaled == 'True') or (table_size2 >= 2.0):
                    print '[writing into temp2_product_attributes]: '
                    data_ready = 1
                else:
                    print '[ERROR]: temp2_product_attributes table size is less than 2TB! ', ' : ', table_size2
                    self.mailNotificationTopStats3(is_prod_db, table_size2, 'temp2_product_attributes', ' ')
                    data_ready = 0
                print '[data_ready_]: ', data_ready
            except Exception as e:
                print "Exception caught1: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1)
        return data_ready
        
    def writeNewCounts(self, write_new_count, data_ready, is_new_data, is_prod_db, mail_distn):   
        print '[writeNewCounts]: write_new_count: ', write_new_count, ', data_ready: ', data_ready, ', is_new_data: ' , is_new_data, ', is_prod_db: ', is_prod_db        
        if ((write_new_count == 'True') and (data_ready == 1) and (is_new_data == 'True')):   ###cron1 job
            try:
                print '[executing writeNewCounts]'
                self.droptable(self.db, self.temp2_product_attributes_counts1)
                self.createTempTableCounts1(self.db, self.temp2_product_attributes_counts1)  
                self.droptable(self.db, self.temp2_product_attributes_counts2)
                self.createTempTableCounts2(self.db, self.temp2_product_attributes_counts2)  
                self.droptable(self.db, self.temp2_product_attributes_counts3)
                self.createTempTableCounts3(self.db, self.temp2_product_attributes_counts3)  
                print '[executing insertTempTableCounts1]'        
                self.insertTempTableCounts1(self.db, self.temp2_product_attributes_counts1, 
                                            self.temp2_product_attributesX) ##with/without seller_name info..
                print '[executing insertTempTableCounts2]'        
                self.insertTempTableCounts2(self.db)
                print '[executing insertTempTableCounts3]'        
                self.insertTempTableCounts3(self.db)
                table_size_counts3 = self.checktableSize('temp2_product_attributes_counts3')
                print '[table size temp2_product_attributes_counts3]: ', table_size_counts3
                
                ##...This section is optional (used only for email report)................  
                if mail_distn == 'True':       
                    self.droptable(self.db, "temp2_product_ids1")
                    self.droptable(self.db, "temp2_product_ids2")
                    self.createTempTableProductIds(self.db, "temp2_product_ids1")
                    self.createTempTableProductIds(self.db, "temp2_product_ids2")
                    print '[executing insertIntoTempProductIds]'       
                    self.insertIntoTempProductIds(self.db, "temp2_product_ids2", self.dateid2)
                    self.insertIntoTempProductIds(self.db, "temp2_product_ids1", self.dateid1)
                ##...........................................................................
                print '[finished executing writeNewCounts]'
            except Exception as e:
                print "Exception caught/writeNewCounts: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1)
                
    def postToHiveDisk(self, post_hive, is_new_data, data_ready, is_prod_db, is_postgres, is_postgres_test):          
        if (post_hive == 'True') and (is_new_data == 'True') and (data_ready == 1):   #cron1 job
            topAttributes_dynamic =  " "
            topAttributes_static = " " 
            topAttributes_identification =  " "
            topAttributes_size = 0
            topSources =  " "
            topProductTypes =  " "
            totalCountAttributes =  " "
            crud_counts_s = " " 
            product_ids_created_count =  0
            product_ids_deleted_count =  0
            ###--------------------------------------------------------------
            try:
                topAttributes_dynamic = self.getTopStats(self.db, "fqkey3", "")
                print "\n[topAttributes_dynamic]: ", topAttributes_dynamic
                topAttributes_dynamic_tableau = self.getTopStats(self.db, "fqkey3", self.top_attributes_dynamic_tableau)
                print "\n[topAttributes_dynamic_tableau]: ", topAttributes_dynamic_tableau
                print '[post_hive/getTopStats- top_attributes_static]:'
                topAttributes_static = self.getTopStats(self.db, "fqkey3", self.top_attributes_static)
                print '[topAttributes_static]: ', topAttributes_static
                topAttributes_identification = self.getTopStats(self.db, "fqkey3", self.top_attributes_identification)
                print '[topAttributes_identification]: ', topAttributes_identification
                topAttributes_size = self.getTopStats(self.db, "fqkey3", self.top_attributes_size)
                print '[topAttributes_size]: ', topAttributes_size
            except Exception as e:
                print "Exception caught/getTopAttributes: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                ###-----------------------------------------------------------------
            try:
                topSources = self.getTopStats(self.db, "source", topAttributes_static)
                print "\n[topSources]: ", topSources
                topProductTypes = self.getTopStats(self.db, "product_type", topAttributes_static) ##1
                print "\n[topProductTypes]: ", topProductTypes  ##2
            except Exception as e:
                print "Exception caught/getTopSources: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                ###Level2 attributes--------------------------------------------
            try:
                self.getTopAttrDictL2Fast(self.top_attributes_static, is_prod_db)
                print '[topAttrDictL2]: ', self.topAttrDictL2
            except Exception as e:
                print "Exception caught/getTopAttrDictL2Fast: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                ###Level2 product types----------------------------------------------
            try:
                topProductTypesList = self.getTopAttributesList(topProductTypes)  ##3
                print '[topProductTypesList]: ', topProductTypesList  ##4
                print '[len(topProductTypesList)]: ', len(topProductTypesList)  ##4
                self.getTopProdTypeDictL2Fast(topProductTypesList, is_prod_db)
                print '[topProdTypeDictL2FAST__]: ', self.topProdTypeDictL2  ##6
            except Exception as e:
                print "Exception caught/getTopAttributesList: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                    ###--------------------------------------------------------------
            try:
                totalCountAttributes = self.getTotalCountAttributes(self.db)
                print "\n[totalCountAttributes]: ", totalCountAttributes
                crud_counts_s = self.getCRUDCountsDB(self.db)
                print '[counts_all]: ', crud_counts_s
            except Exception as e:
                print "Exception caught/getTotalCountAttributes: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                    #--------------------------------------------------------------
            try:
                product_ids_created_count = self.getProductsCreatedDeletedCount(self.db, 'created')
                print '[product_ids_created_count]: ', product_ids_created_count
                product_ids_deleted_count = self.getProductsCreatedDeletedCount(self.db, 'deleted')
                print '[product_ids_deleted_count]: ', product_ids_deleted_count
            except Exception as e:
                print "Exception caught/getProductsCreatedDeletedCount: ", e
                self.mailNotificationTopStats4(is_prod_db, e)
                sys.exit(1) 
                ###-----------------------------------------------------------------------
            try:
                self.postHive(topAttributes_dynamic, topAttributes_dynamic_tableau, topAttributes_static, topAttributes_identification, topAttributes_size, 
                                topSources, topProductTypes, totalCountAttributes, crud_counts_s, self.topAttrDictL2, 
                                product_ids_created_count, product_ids_deleted_count, self.topProdTypeDictL2, data_ready)
            except Exception as e:
                self.mailNotificationTopStats4(is_prod_db, e)
                print "Exception caught/postHive: ", e
                sys.exit(1)  
            try:  ##generate query for postgres db
                if is_postgres == 'True':
                    self.read_top_stats2(self.postgres_number_of_days) ##last 30 days
                    self.generatePostgresQuery(self.postgres_query_file, is_postgres_test)  
            except Exception as e:
                print "Exception caught/postgres: ", e
                sys.exit(1)  
                
    def cleanup(self):
        path = '/home/catint/qarth_product_top_attributes/logs/cron1/'
        today = datetime.datetime.now()
        previous = today - datetime.timedelta(days=40)
        month = previous.month
        year = previous.year
        if month < 10:
            month = '0' + str(month)
        cmd = '/bin/rm ' + path + 'product_top_attributes_cron1_*-' + str(month) + '-' + str(year) + '.out'
        self.execCmdOS(cmd)
        
    def saveHiveTable(self):
        top_stats_results_hive = self.get_top_stats_hive(365)
        #print '[Hive temp2_top_attributes data]: ', top_stats_results_hive
        
        
    ##--------------------------------
    ##--------------------------------
        
    def getRankedAttributeRawTableau(self, db, table_name_counts3):
        query = ('SELECT ' + self.dateid2 + ', fqkey3, count(*) as ct '
                'FROM ' + db + '.' + table_name_counts3 + ' '
                'WHERE (trim(fqkey3) != "") AND '
                "(fqkey3 is not null) AND (source is not null) AND (product_type is not null) AND " 
                "(source != 'unknown') AND (product_type != 'unknown') AND " 
                "(update_ == '1') " 
                'GROUP BY fqkey3 '
                'ORDER BY ct desc;')
        
        query = self.setOptions  + query
        results = self.execHiveCheck(query, self.retry_count)  
        return results     
     
    def getRankedAttributeRawTableauFast(self, db, table_name_counts3):
        query = ('SELECT ' + self.dateid2 + ', fqkey3, source, product_type '
                'FROM ' + db + '.' + table_name_counts3 + ' '
                'WHERE (trim(fqkey3) != "") AND '
                "(fqkey3 is not null) AND (source is not null) AND (product_type is not null) AND " 
                "(source != 'unknown') AND (product_type != 'unknown') AND " 
                "(update_ == '1') ;")
        
        query = self.setOptions  + query
        results = self.execHiveCheck(query, self.retry_count)  
        return results      
        
    def getRankedAttributesListTableau(self, attrs_raw):
        lines = attrs_raw.split('\n')
        attrs_dict = {}
        attrs_list = []
        for line in lines:
            entry = {}
            fields = line.split('\t')
            if len(fields) >= 3:
                entry['date_id'] = fields[0]
                entry['attr_name'] = fields[1]
                entry['attr_count'] = fields[2]
                attrs_dict[fields[1]] = entry
                attrs_list.append(fields[1])
        return attrs_dict, attrs_list
    
    def getRankedAttributesListTableauFast(self, attrs_raw):
        lines = attrs_raw.split('\n')  ##dateid2, fqkey3, source, product_type
        attrs_dict = {}
        i = 0
        for line in lines:
            i += 1
            entry = {'date_id': None, 'attr_name': None, 'attr_count': 0, 'source': {}}
            fields = line.split('\t')
            source_value = {}
            if len(fields) >= 4:
                entry['date_id'] = fields[0]
                entry['attr_name'] = fields[1]
                if attrs_dict.get(fields[1], 0) == 0:
                    attrs_dict[fields[1]] = {}

                if attrs_dict[fields[1]].get('attr_count', 0) == 0:
                    attr_count = 0
                else:
                    attr_count = attrs_dict[fields[1]]['attr_count']
                entry['attr_count'] = int(attr_count) + 1
                    
                if attrs_dict[fields[1]].get('source', 0) == 0:
                    source_value = {}
                else:
                    source_value = attrs_dict[fields[1]]['source']
                    source_count = source_value.get(fields[2], 0)
                    #print 'source_value: ', source_value
                    #print 'source_count: ', source_count
                    if source_count == '':
                        source_count = 0
                    source_value[fields[2]] = int(source_count) + 1
                entry['source'] = source_value
                    
                #if i % 1000000 == 0:
                #print '[entry]: ', entry
                attrs_dict[fields[1]] = entry
                #print '[attrs_dict]: ', attrs_dict
        return attrs_dict
            
    def getTop3SourceCountsTableau(self, db, table_name_counts3, attrs_list_ranked):
        results = []
        sources_top_n_attributes = self.sources_top_n_attributes
        attrs_list_top_n = attrs_list_ranked[0:sources_top_n_attributes]
        for attr_name in attrs_list_top_n:
            query = ('select fqkey3, source, count(*) as ct '
                    'from ' + db + '.' + table_name_counts3 + ' '
                    'where trim(fqkey3) != "" AND '
                    'fqkey3 = "' + attr_name + '" AND '
                    'source != "unknown" '
                    'group by fqkey3, source '
                    'order by ct desc limit 3;')
            query = self.setOptions  + query
            #print '[query]: ', query
            result = self.execHiveCheck(query, self.retry_count) 
            #print '[result]: ', result
            results.append(result) 
        #print '[results]: ', results
        return results  
    
    def writeFile(self, filename, data):
        try:
            with open(filename, 'w') as f:
                f.write(data)
        except Exception as e:
            print '[Exception/write]: ', e
            
    def readFile(self, filename):
        data = ''
        try:
            with open(filename, 'r') as f:
                data = f.read()
        except Exception as e:
            print '[Exception/write]: ', e
        return data 
    
    def generatePostGresQueryTableuAttrs(self, attrs_sources_dict):
        queries = []
        for key in attrs_sources_dict:
            curr_date = attrs_sources_dict[key]['date_id']
            attr_name = attrs_sources_dict[key]['attr_name']
            attr_count = attrs_sources_dict[key]['attr_count']
            source1_name = attrs_sources_dict[key]['source1_name']
            source1_count = attrs_sources_dict[key]['source1_count']
            source2_name = attrs_sources_dict[key]['source2_name']
            source2_count = attrs_sources_dict[key]['source2_count']
            source3_name = attrs_sources_dict[key]['source3_name']
            source3_count = attrs_sources_dict[key]['source3_count']
            values = (curr_date + ',' + attr_name + ',' + attr_count + ',' + 
                      source1_name + ',' + source1_count + ',' +
                      source2_name + ',' + source2_count + ',' +
                      source3_name + ',' + source3_count)

            query = ('insert into temp2_qarth_dd_attrs (curr_date, attr_name, attr_count, '
                     'source1_name, source1_count, source2_name, source2_count, source3_name, '
                     'source3_count) values ( ' +
                      values + ');'
                    )
            queries.append(query)
        queries_s = '\n'.join(queries)
        self.writefile('drilldown_attrs_query.txt', queries_s)
        
    def getAttrsSourcesDictTableau(self, attrs_dict, sources_count_raw_list):
        print '[sources_count_raw_listx2]: ', sources_count_raw_list
        #sources_count_raw_list = sources_count_raw_list.split('\n')
        for sources_count_raw in sources_count_raw_list:
            print '[sources_count_rawx2]: ', sources_count_raw
            lines = sources_count_raw.split('\n')
            print '[linesx2]: ', lines
            for line in lines:
                fields = line.split('\t')
                print '[fieldsx2]: ', fields
                if len(fields) >=3:
                    attr_name = fields[0]
                    source_name = fields[1]
                    source_count = fields[2]
                    print '[attrs_dict[attr_name]: ', attrs_dict[attr_name]
                    if attrs_dict[attr_name].get('source_name1', None) == None:
                        attrs_dict[attr_name]['source_name1'] = source_name
                        attrs_dict[attr_name]['source_count1'] = source_count
                    elif attrs_dict[attr_name].get('source_name2', None) == None:
                        attrs_dict[attr_name]['source_name2'] = source_name
                        attrs_dict[attr_name]['source_count2'] = source_count
                    else:
                        attrs_dict[attr_name]['source_name3'] = source_name
                        attrs_dict[attr_name]['source_count3'] = source_count
                        
        return attrs_dict 
    #'wmt_category': {'source_count3': '31873', 'attr_name': 'wmt_category', 
    #'source_name3': 'MARKETPLACE_PARTNER', 'source_name1': 'MERCHANT_MANUAL', 
    #'source_count1': '90599', 'source_count2': '39623', 'source_name2': 'CONTENT_STORE', 
    #'attr_count': '232800', 'date_id': '20180719'}
    #curr_date, attr_name, attr_count, source1_name, source1_count, source2_name, source2_count, 
    #source3_name, source3_count,
    def generatePostGresQueryTableau(self, attrs_sources_dict):
        attr_names = ('curr_date, attr_name, attr_count, source_name1, source_count1, '
                      'source_name2, source_count2, source_name3, source_count3')
        queries = []
        for key_ in attrs_sources_dict:
            print '[key_]: ', key_
            attr_values = '-'
            curr_date = '-'
            attr_name = "'-'"
            attr_count = '-1'
            source_name1 = "'-'"
            source_count1 = '-1'
            source_name2 = "'-'"
            source_count2 = '-1'
            source_name3 = "'-'"
            source_count3 = '-1'
            fields_dict = attrs_sources_dict[key_]
            print '[fields_dictx]: ', fields_dict
            if fields_dict == {}:
                continue
            for attr in fields_dict:
                if attr == 'date_id':
                    curr_date = "'" + fields_dict[attr] + "'"
                elif attr == 'attr_name':
                    attr_name = "'" + fields_dict[attr] + "'"
                elif attr == 'attr_count':
                    attr_count = fields_dict[attr] 
                elif attr == 'source_name1':
                    source_name1 = "'" + fields_dict[attr] + "'"
                elif attr == 'source_count1':
                    source_count1 = fields_dict[attr] 
                elif attr == 'source_name2':
                    source_name2 = "'" + fields_dict[attr] + "'"
                elif attr == 'source_count2':
                    source_count2 = fields_dict[attr]
                elif attr == 'source_name3':
                    source_name3 = "'" + fields_dict[attr] + "'"
                elif attr == 'source_count3':
                    source_count3 = fields_dict[attr]
            attr_values = (curr_date + ',' + attr_name + ',' + attr_count + ',' + source_name1 + ',' +
                      source_count1 + ',' + source_name2 + ',' + source_count2 + ',' +
                      source_name3 + ',' + source_count3)
            
            #this query is for postgres db, schema: postgres
            query = 'insert into ' + self.temp2_qarth_analytics_drilldown1 + ' (' + attr_names + ') values (' + attr_values + ');'
            print '[queryx]: ', query
            queries.append(query) 
        return queries   
            
    
    def processTableauDrillDownDeprecated(self, is_postgres):
        is_attrs_count_raw = 'False' ##default: 'False'
        is_sources_count_raw_list = 'False'  ##default: 'False'
        attrs_count_raw = ''
        sources_count_raw_list = []
        if is_postgres == 'True':
            if is_attrs_count_raw == 'False':
                attrs_count_raw = self.getRankedAttributeRawTableau(self.db, self.temp2_product_attributes_counts3)
                self.writeFile('attrs_count_raw.txt', attrs_count_raw)
            else:
                attrs_count_raw = self.readFile('attrs_count_raw.txt')
            attrs_dict, attrs_list_ranked = self.getRankedAttributesListTableau(attrs_count_raw)
            if is_sources_count_raw_list == 'False':
                sources_count_raw_list = self.getTop3SourceCountsTableau(self.db, self.temp2_product_attributes_counts3, attrs_list_ranked)
                self.writeFile('sources_count_raw_list.txt', '\n'.join(sources_count_raw_list))
            else:
                sources_count_raw_list = (self.readFile('sources_count_raw_list.txt')).split('\n')
            attrs_sources_dict = self.getAttrsSourcesDictTableau(attrs_dict, sources_count_raw_list)
            queries = self.generatePostGresQueryTableau(attrs_sources_dict) 
            queries_s = '\n'.join(queries)
            self.writeFile('postgres/postgres_drilldown_queries.sql', queries_s)    
            self.writeFile('postgres/postgres_drilldown_queries_' + self.dateid2 + '.sql', queries_s) ##backup copy   
            
    def generatePostGresQueryTableauFast(self, attrs_raw):
        attr_names = ('curr_date, attribute, source')
        queries = []
        attrs_raw_lines = attrs_raw.split('\n')
        for line in attrs_raw_lines:
            fields = line.split('\t')
            if len(fields) < 3:
                continue
            curr_date = fields[0]
            attribute = fields[1]
            source = fields[2]
            attr_values = ("'" + curr_date + "'" + ",'" + attribute + "','" + source + "'")
            #this query is for postgres db, schema: postgres
            query = 'insert into ' + self.temp2_qarth_analytics_drilldown1 + ' (' + attr_names + ') values (' + attr_values + ');'
            #print '[queryx]: ', query
            queries.append(query) 
        return queries
                    
    def processTableauDrillDownFast(self, is_postgres, is_attrs_count_readfile):
        attrs_count_raw = ''
        queries = []
        queries_s = ''
        if is_postgres == 'True':
            if is_attrs_count_readfile == 'False':
                attrs_count_raw = self.getRankedAttributeRawTableauFast(self.db, self.temp2_product_attributes_counts3)
                self.writeFile('attrs_count_raw_fast.txt', attrs_count_raw)
            else:
                attrs_count_raw = self.readFile('attrs_count_raw_fast.txt')
            #attrs_dict = self.getRankedAttributesListTableauFast(attrs_count_raw)
            #print '[attrs_dictx]: ', attrs_dict
            queries = self.generatePostGresQueryTableauFast(attrs_count_raw)
        queries_s = '\n'.join(queries)
        print '[queries_s]: ', queries_s
        self.writeFile('postgres/postgres_drilldown_queries.sql', queries_s)    
        self.writeFile('postgres/postgres_drilldown_queries_' + self.dateid2 + '.sql', queries_s) ##backup copy  
        
    def writeNewAttributesAndOrgIds(self, write_new_attr, is_new_ids, is_new_data, is_prod_db, is_scaled, data_ready, mail_distn):
        if write_new_attr == 'True':
            if mail_distn == 'True':  #needed only for email report
                self.insertOrgIds(is_new_ids, is_new_data, is_prod_db) ###This needs to be before inserts into temp2_product_attributes2
            print '[writing new attributes]...'
            data_ready = self.writeNewAttributes(write_new_attr, is_new_data, is_prod_db, is_scaled, data_ready)
            
        return data_ready
            
################################################################################ 
def run():
    print "[---------------------Job Started---------------------------]"
    pADD = ProductTopAttributesCron()
    start_time = time.time()
    
    write_new_attr, write_new_count, post_hive, mail_distn, is_prod_db, is_scaled, is_new_ids, is_postgres = pADD.getCommandLineArguments()
    print ('[mail_distn]: ', mail_distn, ' [is_prod_db]: ', is_prod_db, ' [write_new_attr]: ', write_new_attr, 
           ' [write_new_count]: ', write_new_count, ' [post_hive]: ', post_hive)
    #####--------------------------------------------------------------------#######
    data_ready = 1
    is_postgres_test = 'False'  # 'False' for production use
    #####--------------------------------------------------------------------#######    
    pADD.setTestSuffix(is_prod_db)
    #--------------------------------------------------------------------
    last_report_date = pADD.getLastReportDate(is_prod_db, pADD.dateid2)
    if last_report_date == pADD.dateid2:
        print '[No New Data- quitting...]'
        sys.exit(1)
    if pADD.is_data_validation == 'True':
        is_new_data = 'False' if last_report_date == pADD.dateid2 else 'True'
    else:
        is_new_data = 'True'
    print '[is_new_data]: ', is_new_data
    #============================For Testing Only===============================================
    #
    #=====get new attributes======================================================================================
    data_ready = pADD.writeNewAttributesAndOrgIds(write_new_attr, is_new_ids, is_new_data, is_prod_db, is_scaled, data_ready, mail_distn)
    #------get new counts--------------------------------------------------------------------
    print '[starting writeNewCounts]...'
    pADD.writeNewCounts(write_new_count, data_ready, is_new_data, is_prod_db, mail_distn)            
    #------Tableau drilldown--------------------------------------------------------
    print '[starting processTableauDrillDownFast]...'
    #pADD.processTableauDrillDownDeprecated(is_postgres)
    is_attrs_count_readfile = 'True'
    pADD.processTableauDrillDownFast(is_postgres, is_attrs_count_readfile)
    #--------------------------------------------------------------------------
    #pADD.postToHiveDisk(post_hive, is_new_data, data_ready, is_prod_db, is_postgres, is_postgres_test)          
    #pADD.postToHiveInMemory(post_hive, is_new_data, data_ready, is_prod_db, is_postgres, is_postgres_test)          
    #--------------------------------------------------------------------------
    #if mail_distn == 'True':  ##cron2 job
        #pADD.read_top_stats2(7) #specify number of days
        #pADD.mailNotificationTopStats2(is_prod_db, post_hive)
    #-------------------------------------------------------------- 
    end_time = time.time()
    print '[Queries executed]: ', pADD.queries
    if write_new_attr == 'True':
        qpath = 'cron1'
    else:
        qpath = 'cron2'
    #-------------------------------------------------------------- 
    with open('./logs/' + qpath + '/queries_executed.sql', 'w') as f:
        pADD.queries = (pADD.queries.replace('SET', '\nSET').replace('SELECT', '\nSELECT').replace('FROM', '\nFROM').
                        replace('WHERE', '\nWHERE').replace('GROUP BY', '\nGROUP BY').replace('AND', '\nAND').replace('IN', '\nIN').
                        replace('HAVING', '\nHAVING').replace('STORED', '\nSTORED').replace('LOCATION', '\nLOCATION').replace('split', '\nsplit').
                        replace('INSERT', '\nINSERT').replace('COALESCE', '\nCOALESCE').replace('CONCAT', '\nCONCAT').replace('UNION', '\nUNION').
                        replace('get_json_object', '\nget_json_object').replace('ON (', '\nON (').replace('LEFT', '\nLEFT').
                        replace('delete', '\ndelete').replace('add', '\nadd').replace('USING', '\nUSING').replace('ORDER', '\nORDER'))
        f.write(pADD.queries)
    #-------------------------------------------------------------- 
    #cleanup
    pADD.saveHiveTable()
    pADD.cleanup()
    #-------------------------------------------------------------- 
    run_time = (end_time - start_time)/3600
    print "[Run Time in hours]: ", run_time
    print "[--------------------Job Completed--------------------]"
##################################################################################
if __name__ == "__main__":
    run()
###################################################################################
###TOTO:
##1.
###################################################################################
