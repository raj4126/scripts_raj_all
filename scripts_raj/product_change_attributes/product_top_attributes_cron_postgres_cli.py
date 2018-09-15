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
import psycopg2

def execPostgres(query):
        print '[postgres query]: ', query
        conn = psycopg2.connect("dbname='postgres' user='postgres' host='10.65.167.217' password='postgres'")
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()

query = ''        
with open('postgres_query.sql', 'r') as f:
    query = f.read()
    
execPostgres(query)