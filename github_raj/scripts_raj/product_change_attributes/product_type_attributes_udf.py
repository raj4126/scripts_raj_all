import json
import sys
import string
###############################################################################
# Raj Kumar
#   steps for creating UDF:
#    1.  delete file /home/vn0aisn/modified_product_attributes/product_udf_jsondiff2.py
#    2.  add file /home/vn0aisn/modified_product_attributes/product_udf_jsondiff2.py
#    3.  SELECT TRANSFORM (A.product_id, A.updated_at, B.updated_at, A.product_attributes, B.product_attributes) 
#          USING 'python product_udf_jsondiff2.py' AS (product_id string, fqkey string) 
#          FROM catint.temp2_product_attributes  A 
#          INNER JOIN catint.temp2_product_attributes  B 
#          ON (A.product_id = B.product_id) 
#          WHERE (A.product_attributes != B.product_attributes);
################################################################################
class ProductTypeAttributesUdf:
    
    def __init__(self):
        self.results = ''
        self.prod_run = True
        self.product_type_attrs_test_file = 'product_type_attrs_test.txt';
        
  
    def dumpResult(self, product_type_attributes_keys_s):   
      if (product_type_attributes_keys_s == None) or (product_type_attributes_keys_s == ''):
        return 
      self.results = product_type_attributes_keys_s

    def parseJson(self, product_type_attributes_s ):
      try:
        product_type_attributes = json.loads(product_type_attributes_s)
        keys = []
        for key_ in product_type_attributes:
            #print '[key_]: ', key_
            keys.append(key_)
        
        #print '[keys]: ', keys    
        keys_s = '\n'.join(keys)
        self.dumpResult(keys_s)
      except Exception as e:
        pass
      return None      
  
    def getInputJson(self):
      if self.prod_run == True:
        product_type_attributes = sys.stdin.readline()  
      else:
        product_type_attributes = ''
        try:
            with open(self.product_type_attrs_test_file, 'r') as f:
                product_type_attributes = f.read()
        except Exception as e:
            print 'Exception1: ', e
      return product_type_attributes
  
    def processInputData(self):
        while True:
          try:
              product_type_attributes = self.getInputJson()
              #print '[product_type_attributes]: ', product_type_attributes
              if len(product_type_attributes) == 0:
                  continue
              product_type_attributes = string.strip(product_type_attributes, "\n") 
              product_type_attributes = product_type_attributes.decode("utf-8")
              product_type_attributes = product_type_attributes.encode("ascii", "ignore")
          except Exception as e:
              pass
          try:          
              ##------------------------------------------------ 
            self.parseJson(product_type_attributes)
            print self.results
            self.results = ''
            ###-----------------------------------------------------------
          except Exception as e:
              self.results = ''
              #print 'Exception2: ', e
###################################################################        
def run(): 
    ptaUdf = ProductTypeAttributesUdf() 
    ptaUdf.processInputData() 
###################################################################        
run()
#######################################################################


