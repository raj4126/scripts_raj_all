#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2
conn = psycopg2.connect("dbname='postgres' user='postgres' host='10.65.167.217' password='postgres'")
#conn = psycopg2.connect("dbname='vn0aisn' user='postgres' host='10.65.167.217' password='postgres'")
cur = conn.cursor()
cur.execute("""SELECT * from vktest2""")
rows = cur.fetchall()
for row in rows:
    print "   ", row[0]
 
cur.execute("INSERT INTO temp2_junk VALUES(10, 'SK')")
#cur.execute("INSERT INTO vktest2 VALUES(10, 'SK')")
#cur.execute("DELETE FROM vktest2 WHERE col1=3")
conn.commit()
cur.close()