import json
import gc
import pyspark
import os
import sys
import random
import uuid
import time

import imagehash  
import requests
from io import BytesIO
import urllib, cStringIO

from PIL import Image

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.conf import SparkConf

#Commands:  nohup  spark-submit --master yarn --deploy-mode client 
# --conf spark.pyspark.virtualenv.enabled=true  
# --conf spark.pyspark.virtualenv.type=native 
# --conf spark.pyspark.virtualenv.requirements=/home/catint/raj/ingest_5m_images/test/requirements.txt 
# --conf spark.pyspark.virtualenv.bin.path=/home/catint/raj/ingest_5m_images/raj_image/bin 
# --conf spark.pyspark.python=/home/catint/raj/ingest_5m_images/raj_image/bin/python2.7 
# ingest_spark.py 0  10000  > ingest_spark.out &
#Setup the virtualenv, and generate requirements.txt file first:
#   pip freeze > requirements.txt
# https://community.hortonworks.com/articles/104947/using-virtualenv-with-pyspark.html
#Note:  After the job is run, the leftover images in the local folders need to be manually copied to hdfs.

def getAssetUrlsListFile(rawfile):
    assetUrlsList = []
    with open(rawfile, 'r') as f:
        for line in f:
            assetUrlsList.append(line)
    return assetUrlsList
    
def getFormat(url):
    if 'jpeg' in url:
        fmt = 'jpeg'
    elif 'jpg' in url:
        fmt = 'jpg'
    elif 'png' in url:
        fmt = 'png'
    elif 'gif' in url:
        fmt = 'gif'
    elif 'tiff' in url:
        fmt = 'tiff'
    elif 'bmp' in url:
        fmt = 'bmp'
    elif 'exif' in url:
        fmt = 'exif'
    else:
        fmt = 'jpeg'
    return fmt
  
def getSuffix(i, item_id):
    if i < 10:
        suffix = "00" + str(i)
    elif i < 100:
        suffix = "0" + str(i)
    elif i < 1000:
        suffix = str(i)
    else:
        print("Error: More than 999 images for this item_id"), item_id
        suffix = "ERROR" 
    return suffix

def mkdirLocal(localdir):
    try:
        if os.path.isdir(localdir) == False:
            os.mkdir(localdir)
    except Exception as e:
            print "Exception in fetchImagelist/mkdir(localdir): ", e
       
def fetchImagelist(record, start_end, hdfs_path, number_threads):
    flds = start_end.split(":")
    start = flds[0]
    end = flds[1]
    fields = record.split(",")
    item_id = fields[0]
    asset_urls = fields[1:]
    bucket = int(item_id) % int(number_threads)
    image_dir = 'images' + str(bucket)
    hdfs_dir = hdfs_path + image_dir
    
    s = requests.Session()
    adapter1 = requests.adapters.HTTPAdapter(max_retries=3)
    adapter2 = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount('http://', adapter1)
    s.mount('https://', adapter2)

    error_images_over_999 = []
    batch_size = 1000
    localdir = './' + image_dir
    mkdirLocal(localdir)
    
    try:
        i = 1
        for assetUrl in asset_urls:
            url = assetUrl.lower()
            fmt = getFormat(url)
            try:
              response = s.get(assetUrl, timeout=3)
            except Exception as e:
                print 'Error: Image load failed', assetUrl
                continue
            im = Image.open(BytesIO(response.content))
            suffix = getSuffix(i, item_id)
            if suffix == "ERROR":
                error_images_over_999.append(item_id)
                continue
            outfile = localdir + "/" + str(item_id) + suffix + '.' + fmt
            mkdirLocal(localdir)
            im.save(outfile, format=fmt)  #save to local storage
            i = i + 1

        number_of_files = len(os.listdir(localdir))
        if number_of_files > batch_size:
            #get hdfs folder
            localdir_h = localdir + uuid.uuid4().hex
            command = 'mv ' + localdir + ' ' + localdir_h
            execShellCmd(command)
            #save to hdfs
            command = 'hadoop fs -put ' + localdir_h + ' ' + hdfs_dir + '/'
            execShellCmd(command)
            #delete localdir files
            command = 'rm -rf ' + localdir_h
            execShellCmd(command)
    except Exception as e:
        print "Exception in fetchImagelist/assetUrl: ", e, ";  ", record
    s.close()
    return None

def execShellCmd(command):
    try:
        os.system(command)     
    except Exception as e:
        print 'Exception thrown: ', e
    
def setup_proxy(x):
    http_proxy = "http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
    https_proxy = "http://gec-proxy-svr.homeoffice.wal-mart.com:8080"
    os.environ['http_proxy'] = http_proxy
    os.environ['HTTP_PROXY'] = http_proxy
    os.environ['https_proxy'] = https_proxy
    os.environ['HTTPS_PROXY'] = https_proxy
    return x    

def main():
    number_threads = "16"
    conf = SparkConf()
    conf.setMaster("local[" + number_threads + "]")
    conf.setAppName("image_ingest_application")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.connection.timeout", "10000s")
    conf.set("spark.network.timeout", "600s")
    conf.set("spark.files.fetchTimeout", "500s")
    #conf.set("spark.ui.port", "4045") ###
    #conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ###
    #conf.set("spark.cores.max", 200)
    
    sc = SparkContext(conf = conf)
    print "[Starting Spark Job]"
        
    start = (sys.argv[1]).strip()
    end = (sys.argv[2]).strip()
    print 'start:end::', start, "::", end
    start_time = time.time()
    print 'start_time: ', start_time
    start_end = start + ":" + end
    
    image_dir = start + 'images' + end
    hdfs_path = '/hive/catint.db/temp2_ds_images_spark3/'
    hdfs_dir = hdfs_path + image_dir
    command = 'hadoop fs -mkdir ' +  hdfs_dir
    execShellCmd(command)
        
    rawfile = 'asset_urls_5m.txt'
    assetUrlsList = getAssetUrlsListFile(rawfile)
    start_index = int(start)
    end_index = int(end)
    assetUrlsList_chunk = assetUrlsList[start_index : end_index]
    print 'len(assetUrlsList_chunk): ', len(assetUrlsList_chunk)
    rdd1 = sc.parallelize(assetUrlsList_chunk)

    rdd2 = (rdd1.
                map(lambda x: setup_proxy(x)).
                map(lambda x: fetchImagelist(x, start_end, hdfs_path, number_threads))
           )
    
    rdd2.collect()
 
    end_time = time.time()
    print 'end_time: ', end_time
    total_execution_time = (end_time - start_time)/60
    print 'total execution time/mins: ', total_execution_time
    print "[Spark Job Completed]"
    
main()
###############################################################################################

## https://www.alibabacloud.com/help/doc-detail/28124.htm