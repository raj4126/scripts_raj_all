CREATE  TABLE `temp2_product_attributes2_validation_2018031110_3k`(
  `product_id` string,
  `source` string,
  `updated_at` string,
  `product_attributes` string,
  `org_id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'maprfs:/user/catint/temp2_product_attributes2_validation_2018031110_3k';
  
insert into table temp2_product_attributes2_validation_2018031110_3k select product_id, source, updated_at, product_attributes, org_id from temp2_product_attributes2;

select count(*) from temp2_product_attributes2;
8371  (should have been 3810 x 2 = 7620, so 8371-7620 = 751 duplicates for day1 or day2)
select count(*) from temp2_product_attributes2_validation_2018031110_3k;
8371

truncate table temp2_product_attributes2;
# insert into table temp2_product_attributes2 select product_id, source, updated_at, product_attributes, org_id from temp2_product_attributes2_validation_2018031110_3k 
# where product_id='1QSWQFKAAJ71';
insert into table temp2_product_attributes2 select * from temp2_product_attributes2_validation_2018031110_3k where product_id='1QSWQFKAAJ71';
-------------------------------------------------------------------------------
1.  Exceptions:  '1AI9L3LG1TOE' has three records!   check_dups.hql
2.  json is malformed in some cases:  lists have extra enclosing quotes ("")

3.  Looks like same product_id can have different org_id on day1 vs day2?, removed from group by clause for now...
-------------------------------------------------------------------------------

INSERT INTO table catint.temp2_product_attributes_counts1 
SELECT TRANSFORM(S.pas[0], S.pas[1], S.source, S.product_id) 
USING '/usr/bin/python ./product_top_attributes_udf.py' AS (fqkey string) 
FROM (SELECT product_id, source, org_id, COLLECT_SET(product_attributes) pas 
      FROM catint.temp2_product_attributes2 
      GROUP BY product_id, source, org_id HAVING size(pas) = 2)S
      
      
org_id: Active Apparel and Sportswear
        Canvas Jack's

source:  MARKETPLACE_PARTNER
         MARKETPLACE_PARTNER

hive> desc temp2_product_attributes2;
OK
product_id              string
source                  string
updated_at              string
product_attributes      string
org_id                  string