#! /usr/bin/bash
path=qarth_product_top_attributes
#path=qarth_product_top_attributes_dev
cd /home/catint/$path
#commandline args: write_new_attr  write_new_count  post_hive  mail_distn  is_prod_db  is_scaled  is_new_ids is_postgres
#[/usr/bin/python2.7 -u /home/catint/$path/product_top_attributes_cron.py True True True False True False True True]
##std run:
/usr/bin/python2.7 -u /home/catint/$path/product_top_attributes_cron.py True True True False True False True True

##skipping first part...
#/usr/bin/python2.7 -u /home/catint/$path/product_top_attributes_cron.py False False True False True False True True

/usr/bin/python2.7 /home/catint/$path/product_top_attributes_cron.py True True True False True False True True
##---------------------------------------------------------------------------------------------------------------------
##Postgres:
#  /usr/bin/python2.7 /home/catint/$path/product_top_attributes_cron.py False False False False True False False True
##---------------------------------------------------------------------------------------------------------------------