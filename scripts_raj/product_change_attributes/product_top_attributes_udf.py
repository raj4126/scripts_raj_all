import json
import sys
import string
import codecs
import base64
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
class ProductTopAttributesUdf:
    
    def __init__(self):
        self.tenant_id = "0"  # used in product_attributes_daily_counts_cron
        self.filter_process_only = 'values.value'
        self.filter_required_key = "locale"
        self.filter_required_value = "en_US"
        self.line_separator = "\n"
        self.field_separator = ","
        self.results = []
        self.debug = 0
        self.is_data_validation = False
        self.skip_cmp = False
        
    def dumpResult(self, fqkey):   
      if (fqkey == None) or (fqkey == ''):
        return 
      self.results.append(fqkey) 
           
    def processList(self, src_list, tgt_list, key):
        value_s = ""
        value_t = ""
        size_s = len(src_list)
        size_t = len(tgt_list)

        for i in range(size_s):
          values_s = src_list[i]
          if values_s.get(self.filter_required_key, "") != self.filter_required_value:  #filter out non-US locales
              continue
          for key_ in values_s:
            value_s = values_s[key_]
            if i < size_t:
              values_t = tgt_list[i]
              value_t = values_t.get(key_, None)
            else:
              value_t = "Missing Value"
            if value_s != value_t:
              if value_s == None:
                 value_s = " "
              if value_t == None:
                 value_t = " "
              fqkey = key + "." + key_
              self.dumpResult(fqkey)         

    def compareJson(self, product_attributes_jsona, product_attributes_jsonb, product_id):
      try:
        json1 = json.loads(product_attributes_jsona)
        json2 = json.loads(product_attributes_jsonb)
        ##-----------------------------------------
        pcf1 = json1['pcf']
        day1 = json1['day']
        pcf2 = json2['pcf']
        day2 = json2['day']
        days = day1 + day2
        
        if ('today' not in days) or ('yesterday' not in days):
            raise Exception('day1 or day2 in error!')
            
        if day1 == 'today':
            src_json = pcf1
            tgt_json = pcf2
        else:
            src_json = pcf2
            tgt_json = pcf1
            
      except Exception as e: 
        if self.is_data_validation == True:
            #self.results.append('Exception3: ' + org_id)  ## this goes into the output table
            #print 'Exception3: ', e
            pass
        return None
      try:
        self.processJsonListUpdatesInsertsDeletes(src_json, tgt_json,"", product_id) 
      except:
          #print 'Exception4: ', e
          pass
      return None
      
    def getJsonValues(self, jsonlist):
        if (jsonlist == None):
            return []
        jsonvals = []
        for entry in jsonlist:
            if entry == None:
                continue
            jsonval = entry.get('value', '')
            jsonval = jsonval.replace(' ', '').lower()  ###new change
            jsonvals.append(jsonval)
        return jsonvals
    
    def notes_matching_algorithm(self):
        pass
        # compare src and tgt jsons:
        #     1. if len(srcjson) == len(tgtjson):
        #              look for updates only = # of mismatches
        #     2. if len(srcjson) > len(tgtjson):
        #              # of inserts = len(srcjson) - len(tgtsjon)
        #              # of updates = len(tgtjson) - # of matches
        #     3. if len(srcjson) < len(tgtjson):
        #              # of deletes = len(tgtjson) - len(srcjson)
        #              # of updates = len(srcjson) - # of matches 
                         
    def processJsonListUpdatesInsertsDeletes(self, src_json, tgt_json, fqkey, product_id):
        for key in src_json.keys():
            ##----------------------------------------------------------------------------------------
            try:
                if(key == None) or (key == ''):
                    continue   
                ##----------------------------------------------------------------------------------------
                if (self.is_data_validation == True) and (key.lower() != 'all_rhids'):   ###For testing only...
                    continue
                src_jsonc = src_json[key]
                if type(tgt_json) is dict:
                    tgt_jsonc = tgt_json.get(key, {})
                else:
                    tgt_jsonc = {}          
                
                if src_jsonc is None:
                    src_jsonc = {}
                    
                if tgt_jsonc is None:
                    tgt_jsonc = {}
                
                try:
                    source = tgt_jsonc['properties']['qid'].split('#')[0]  ##get the most recent source
                except:
                    source = 'unknown'
                    
                try:    
                    org_id = tgt_jsonc['properties']['qid'].split('#')[1]  ##get the most recent org_id
                except:
                    org_id = 'unknown'
                              
                srclist = src_jsonc.get('values', [])
                tgtlist = tgt_jsonc.get('values', [])
                  
                if type(srclist) is not list:
                    srclist = []
                srcvals = self.getJsonValues(srclist) 
                
                if type(tgtlist) is list:
                    tgtvals = self.getJsonValues(tgtlist) 
                elif type(tgtlist) is dict:   ###New addition
                    tgtval = tgtlist.get('value', None)  ###
                    if tgtval is not None:   ###
                        tgtvals = [tgtval]  ###
                    else:                   ###
                        tgtvals = []        ###
                else:
                    tgtvals = []  
                    
                ###print org_id, ' @srcvals: ', srcvals, ' @tgtvals: ', tgtvals, 'key: ', key
            except Exception as e:
                #print 'Exception5: ', e
                pass
            ##--------------------------------------
            try:
                insert_count, update_count, delete_count = self.getCrudCounts(srcvals, tgtvals)
#                 if len(srcvals) == len(tgtvals):
#                     update_count = len(set(srcvals) - set(tgtvals))
#                     insert_count = 0
#                     delete_count = 0
#                 elif len(srcvals) > len(tgtvals):
#                     insert_count = len(srcvals) - len(tgtvals)
#                     delete_count = 0
#                     matches = srcvals & tgtvals
#                     update_count = len(tgtvals) - len(matches)
#                 else:
#                     delete_count = len(tgtvals) - len(srcvals)
#                     insert_count = 0
#                     matches = srcvals & tgtvals
#                     update_count = len(srcvals) - len(matches)
            except Exception as e:
                #print 'Exception6: ', e
                pass
            ##---------------------------------------------       
            try:
                product_type = src_json['product_type']['values'][0]['value']
            except:
                product_type = "unknown"
             
            try:   
                if source == None or source == "":
                    source = "unknown"
                if org_id == None or org_id == "":
                    org_id = "unknown"
                    
                key_encoded = key
                product_type = product_type.replace(',', '_').replace('@', '_at_').replace(' ', '_').replace('&', '_and_').replace('#', '_').replace('\t', '') # ',' and '@' are reserved for script use
            except Exception as e:
                #print 'Exception7: ', e
                pass
            #####--------------------------------------------------------------
            try:
                if (self.is_data_validation == False):  ##Normal case
                    key_encoded = key_encoded + "@" + product_type + "@" + source + "@" + org_id
                else:  ##For testing only
                    key_encoded = product_id + " @ " + str(srcvals) + " @ " + str(tgtvals) + " @ " + key
                    #update_coount = update_count
                    #insert_count = 0
                    #delete_count = 0
            except Exception as e:
                    #print 'Exception8: ', e
                    pass
            #####--------------------------------------------------------------
            #convention: @update@insert@delete
            try:
                for i in range(update_count):
                    self.dumpResult(key_encoded + '@1@0@0@5')
                for i in range(insert_count):
                    self.dumpResult(key_encoded + '@0@1@0@5')
                for i in range(delete_count):
                    self.dumpResult(key_encoded + '@0@0@1@5')
            except Exception as e:
                #print 'Exception9: ', e
                pass

    def getCrudCounts(self, srcvals, tgtvals):
        srcvals2 = list(srcvals)
        tgtvals2 = list(tgtvals)
        for item in srcvals:
            try:
                if (item in srcvals2) and (item in tgtvals2):
                    srcvals2.remove(item)
                    tgtvals2.remove(item)  ##so that same item is not compared twice
            except:
                pass
        create_count = 0
        update_count = 0
        delete_count = 0
        if (len(srcvals2) > 0) and (len(srcvals) >= len(tgtvals)):
            create_count = len(srcvals) - len(tgtvals)
            if len(srcvals2) > create_count:
                update_count = len(srcvals2) - create_count
        if (len(tgtvals2) > 0) and (len(srcvals) < len(tgtvals)):
            delete_count = len(tgtvals) - len(srcvals)
        return (create_count, update_count, delete_count)
            
    def getPaddedKey(self, key):
        key_length = len(key)
        padded_key_length = 50
        if key_length > padded_key_length:
            return key[0 : padded_key_length - 1] + "####"
        else:
            padding = padded_key_length - key_length
            return key + '#' * padding                     
  
    def processJsonGeneric(self, src_json, tgt_json, fqkey):
        for key in src_json.keys():
            if(key == None):
                continue
            srcjson = src_json[key]
            if tgt_json != None:
                tgtjson = tgt_json.get(key, '')
            else:
                tgtjson = ''
            if key not in tgt_json:
                #self.dumpResult(fqkey + "." + key)   
                pass                  
            elif (type(srcjson) in [str, unicode, int, long, float]) and (srcjson != tgtjson):
                #self.dumpResult(fqkey + "." + key)   
                pass 
            elif type(srcjson) is list:
                self.processList(srcjson, tgtjson, fqkey + "." + key)                  
            elif type(srcjson) is dict:
                self.processJsonGeneric(srcjson, tgtjson, fqkey + "." + key)  
                
    def preCleanJson(self, product_attributes_json):   
        return (product_attributes_json.strip().replace("\n", " ").replace("\r", " ").replace("\t", " ")
                .replace('"[', '[').replace(']"', ']').replace("'", ""))        
             
    def processInputData(self):
        while True:
          try:
              line = sys.stdin.readline() 
              if len(line) == 0:
                  break
              line = string.strip(line, "\n") 
              line = line.decode("utf-8")
              line = line.encode("ascii", "ignore")
          except Exception as e:
              pass
          try:
              product_attributes_jsona, product_attributes_jsonb, product_id = string.split(line, "\t")              
              ##------------------------------------------------ 
              if self.skip_cmp == True:   ###For testing only...                       
                  print "This test counts total number of records"  ##For testing only..
              else:
                  self.compareJson(product_attributes_jsona, product_attributes_jsonb, product_id)
                  results_s = ",".join(self.results).strip()
                  self.results = []
                  print results_s
              ###-----------------------------------------------------------
          except Exception as e:
              self.results = []
              #print 'Exception2: ', e
              
    def processInputDataUTF(self):
        #with open('record_1QSWQFKAAJ71_ascii.txt', 'r') as f:
        with open('record_6PGGSU271IXT_non_ascii.txt', 'r') as f:
          line = f.read()
          try:

              line = string.strip(line, "\n ") 
              line = line.decode("utf-8")
              line = line.encode("ascii", "ignore")
          except Exception as e:
              #print 'Exception1: ', e
              pass
          try:
              product_attributes_jsona, product_attributes_jsonb, source, org_id = string.split(line, "\t")
              source = source.replace(',', '_').replace('@', '_at_').replace(' ', '_').replace('&', '_and_').replace('#', '_').replace('\t', '') # ',' and '@' are reserved for script use
              org_id = org_id.replace(',', '_').replace('@', '_at_').replace(' ', '_').replace('&', '_and_').replace('#', '_').replace('\t', '') # ',' and '@' are reserved for script use
          
              #self.results.append('Raw json: ' + product_attributes_jsona + '\n')
              #product_attributes_jsona = self.preCleanJson(product_attributes_jsona)
              #product_attributes_jsonb = self.preCleanJson(product_attributes_jsonb)  
                                         
              self.compareJson(product_attributes_jsona, product_attributes_jsonb, source, org_id)
              results_s = ",".join(self.results).strip()
              self.results = []
              print results_s
              ###-----------------------------------------------------------
          except Exception as e:
              self.results = []
              #print ''
              #print 'Exception2: ', e
    
    def processTestData2(self):
        
          try:
              product_attributes_jsona = '{ "pcf" : {"all_rhids":{"properties":{"attributeId":"rhid","taxonomy_version":"~draft","attributeName":"Site Taxonomy","taxonomy":"rhid-dotcom","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"path_id":"["40000","45000","45004","45121","45365"]","id":"45365","locale":"en_US","value":"OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L5","tag_source":"rule","path_str":"["EVERYDAY LIVING","OFFICE PRODUCTS AND SCHOOL SUPPLY","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L2","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L3","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4"]"}]},"primary_shelf":{"properties":{"attributeId":"shelf","added":"Wed Nov 08 22:10:29 PST 2017","attributeName":"Primary Shelf","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"all_paths_str":"[["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]]","path_id":"["0","4044","133012","1045881","922699"]","all_paths_id":"[["0","4044","133012","1045881","922699"]]","taxonomy_version":"~draft","id":"922699","taxonomy":"site-dotcom","locale":"en_US","value":"Wall Decor","tag_source":"normalization","path_str":"["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]"}]},"alternate_shelves":{"properties":{"attributeId":"alternate_shelves","attributeName":"Alternate Shelves","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"922699"}]},"item_class_id":{"properties":{"attribute_type":"STRING","attributeName":"Item Class ID","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"locale":"en_US","value":"1","tag_source":"rule"}]},"primary_category_path":{"properties":{"attributeId":"primary_category_path","attributeName":"Primary Category Path","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"0:4044:133012:1045881:922699"}]},"product_url_text":{"properties":{"fromPcp":"false","added":"Thu Nov 09 06:10:29 UTC 2017","isMultiValued":"false","taxonomy_version":"urn:taxonomy:pcs2.0","attribute_type":"STRING","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","attributeId":"86412","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","isMarketAttribute":"true","attributeName":"Product URL Text","updated":"Thu Nov 09 06:10:29 UTC 2017"},"values":[{"isPrimary":"true","display_attr_name":"url_text_key","locale":"en_US","value":"/ip/NMC-Signs-Dl76R-Hot-Dot-Placard-Sign-10-75-X-10-75-Rigid-Plastic-050/765417231","source_key":"url_text_key","source_value":"/ip/NMC-Signs-Dl76R-Hot-Dot-Placard-Sign-10-75-X-10-75-Rigid-Plastic-050/765417231"}]},"primary_shelf_id":{"properties":{"attributeId":"primary_shelf_id","attributeName":"Primary Shelf ID","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"isPrimary":"true","locale":"en_US","value":"922699"}]},"char_primary_category_path":{"properties":{"attributeId":"char_primary_category_path","attributeName":"Character Primary Category Path","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"Home Page/Home/Decor/Art & Wall Decor/Wall Decor"}]},"product_long_description":{"properties":{"attributeId":"84122","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Product Long Description","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Long Description","locale":"en_US","value":"<ul><li>MADE IN THE USA</li><li>Name:HOT DOT PLACARD SIGN</li><li>Height (In): 10.75</li><li>Width (In): 10.75</li><li>Material: RIGID PLASTIC .050</li><li>Legend: HOT</li></ul>","source_key":"long_description","source_value":"<ul><li>MADE IN THE USA</li><li>Name:HOT DOT PLACARD SIGN</li><li>Height (In): 10.75</li><li>Width (In): 10.75</li><li>Material: RIGID PLASTIC .050</li><li>Legend: HOT</li></ul>"}]},"wmt_category":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"WMT Category","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","isFacet":"true"},"values":[{"isFacetValue":"true","locale":"en_US","value":"Wall_Decor"}]},"facet_product_type":{"properties":{"taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Facet Product Type","source":"MARKETPLACE_PARTNER","isFacet":"true"},"values":[{"isFacetValue":"true","facet_version":"1.1","locale":"en_US","value":"Plaques & Signs"}]},"brand":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"10848","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeName":"Brand","updated":"Fri Sep 22 05:28:44 UTC 2017","isFacet":"true","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Brand","locale":"en_US","value":"NMC","source_key":"brand","source_value":"NMC"},{"isFacetValue":"true","facet_version":"1.1","locale":"en_US","value":"NMC"}]},"display_status":{"properties":{"attributeId":"94989","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","isMarketAttribute":"true","attributeName":"Display Status","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Display Status","locale":"en_US","value":"STAGING","source_key":"display_status","source_value":"STAGING"}]},"gtin":{"properties":{"multiselect":"N","attributeId":"-1","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"GTIN","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"locale":"en_US","source_key":"gtin","value":"00887481042145","display_attr_name":"GTIN","source_value":"00887481042145","isPrimary":"true"}]},"rh_path":{"properties":{"attributeId":"94969","isMultiValued":"false","taxonomy_version":"version","isMarketAttribute":"true","attribute_type":"STRING","attributeName":"RH Path","taxonomy":"pcs","display_attr_name":"rh_path","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"isPrimary":"true","display_attr_name":"rh_path","locale":"en_US","value":"40000:45000:45004:45121:45365"}]},"item_id":{"properties":{"attributeId":"-1","fromPcp":"false","generated_by":"Qarth","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"ITEM_ID","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"isPrimary":"true","display_attr_name":"ITEM_ID","locale":"en_US","value":"765417231","source_key":"item_id","source_value":"765417231"}]},"upc":{"properties":{"multiselect":"N","attributeId":"-1","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"UPC","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"locale":"en_US","source_key":"upc","value":"887481042145","display_attr_name":"UPC","source_value":"887481042145","isPrimary":"true"}]},"product_pt_family":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"product_pt_family","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product PT Family","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"id":"home_decor_pcs","locale":"en_US","value":"Home Decor","tag_source":"model_guess"}]},"product_name":{"properties":{"multiselect":"N","attributeId":"84123","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Product Name","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Name","locale":"en_US","value":"NMC Signs Dl76R, Hot Dot Placard Sign, 10.75 X 10.75, Rigid Plastic .050","source_key":"product_name","source_value":"NMC Signs Dl76R, Hot Dot Placard Sign, 10.75 X 10.75, Rigid Plastic .050"}]},"shelf_description":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"-2126304598","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","isMarketAttribute":"true","attributeName":"Shelf Description","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Shelf Description","locale":"en_US","value":"<ul><li>Items ship from manufacturer in 2 days</li><li>Made in the USA</li><li>Hot Dot Placard Sign</li></ul>","source_key":"shelf_description","source_value":"<ul><li>Items ship from manufacturer in 2 days</li><li>Made in the USA</li><li>Hot Dot Placard Sign</li></ul>"}]},"product_tax_code":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"84126","isOfferAttribute":"true","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeName":"Product Tax Code","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Tax Code","locale":"en_US","value":"2038711","source_key":"product_tax_code","source_value":"2038711"}]},"condition":{"properties":{"multiselect":"N","attributeId":"10314","fromPcp":"false","added":"Fri Sep 22 17:06:12 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","taxonomy_version":"urn:taxonomy:pcs2.0","backfill":"true","attributeName":"Condition","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017"},"values":[{"locale":"en_US","source_key":"condition","value":"New","display_attr_name":"Condition","source_value":"New","tag_source":"model_predictions:ae2","isPrimary":"true"}]},"all_shelves":{"properties":{"attributeId":"shelf","added":"Wed Nov 08 22:10:29 PST 2017","rule_changed":"N","attributeName":"All Shelves","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"all_paths_str":"[["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]]","path_id":"["0","4044","133012","1045881","922699"]","all_paths_id":"[["0","4044","133012","1045881","922699"]]","taxonomy_version":"~draft","id":"922699","taxonomy":"site-dotcom","locale":"en_US","value":"Wall Decor","tag_source":"normalization","path_str":"["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]"}]},"product_type":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"product_type","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product Type","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"idPath":"[["home_and_garden","home_decor_pcs","1916"]]","charPath":"[["Home & Garden","Home Decor","Plaques & Signs"]]","id":"1916","locale":"en_US","value":"Plaques & Signs","tag_source":"model_guess"}]},"material":{"properties":{"attributeId":"10312","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Material","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"materialValue":"RIGID PLASTIC .050","isPrimary":"true","display_attr_name":"Material","locale":"en_US","value":"RIGID PLASTIC .050","source_key":"material","source_value":"RIGID PLASTIC .050"}]},"manufacturer_part_number":{"properties":{"multiselect":"N","attributeId":"10336","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Manufacturer Part Number","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Manufacturer Part Number","locale":"en_US","value":"DL76R","source_key":"manufacturerpartnumber","source_value":"DL76R"}]},"offer_lifecycle_status":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017"},"values":[{"display_attr_name":"offer_lifecycle_status","locale":"en_US","value":"ACTIVE"}]},"product_short_description":{"properties":{"multiselect":"N","attributeId":"84128","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Short Description","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Short Description","locale":"en_US","value":"Hot substances can cause serious burns when they come in contact with ones skin. Prevent an accident by labeling designated vehicles carrying hot substances with this placard sign.","source_key":"short_description","source_value":"Hot substances can cause serious burns when they come in contact with ones skin. Prevent an accident by labeling designated vehicles carrying hot substances with this placard sign."}]},"reporting_hierarchy":{"properties":{"attributeId":"rhid","taxonomy_version":"~draft","attributeName":"Site Taxonomy","taxonomy":"rhid-dotcom","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"is_updated":"false","path_id":"["40000","45000","45004","45121","45365"]","id":"45365","locale":"en_US","value":"OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4","tag_source":"rule","path_str":"["EVERYDAY LIVING","OFFICE PRODUCTS AND SCHOOL SUPPLY","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L2","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L3","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4"]"}]},"product_category":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"segment","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product Category","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"id":"home_and_garden","locale":"en_US","value":"Home & Garden","tag_source":"model_guess"}]},"ironbank_category":{"properties":{"attributeId":"-19436162","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"IronBank Category","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"idPath":"["office","office_other"]","charPath":"["Office","Office"]","isPrimary":"true","display_attr_name":"IronBank Category","locale":"en_US","value":"Office","source_key":"ironbank_category","source_value":"Office"}]}}, "day":"yesterday"}'
              product_attributes_jsonb = '{ "pcf" : {"all_rhids":{"properties":{"attributeId":"rhid","taxonomy_version":"~draft","attributeName":"Site Taxonomy","taxonomy":"rhid-dotcom","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"path_id":"["40000","45000","45004","45121","45365"]","id":"45365","locale":"en_US","value":"OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L5","tag_source":"rule","path_str":"["EVERYDAY LIVING","OFFICE PRODUCTS AND SCHOOL SUPPLY","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L2","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L3","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4"]"}]},"primary_shelf":{"properties":{"attributeId":"shelf","added":"Wed Nov 08 22:10:29 PST 2017","attributeName":"Primary Shelf","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"all_paths_str":"[["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]]","path_id":"["0","4044","133012","1045881","922699"]","all_paths_id":"[["0","4044","133012","1045881","922699"]]","taxonomy_version":"~draft","id":"922699","taxonomy":"site-dotcom","locale":"en_US","value":"Wall Decor","tag_source":"normalization","path_str":"["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]"}]},"alternate_shelves":{"properties":{"attributeId":"alternate_shelves","attributeName":"Alternate Shelves","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"922699"}]},"item_class_id":{"properties":{"attribute_type":"STRING","attributeName":"Item Class ID","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"locale":"en_US","value":"1","tag_source":"rule"}]},"primary_category_path":{"properties":{"attributeId":"primary_category_path","attributeName":"Primary Category Path","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"0:4044:133012:1045881:922699"}]},"product_url_text":{"properties":{"fromPcp":"false","added":"Thu Nov 09 06:10:29 UTC 2017","isMultiValued":"false","taxonomy_version":"urn:taxonomy:pcs2.0","attribute_type":"STRING","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","attributeId":"86412","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","isMarketAttribute":"true","attributeName":"Product URL Text","updated":"Thu Nov 09 06:10:29 UTC 2017"},"values":[{"isPrimary":"true","display_attr_name":"url_text_key","locale":"en_US","value":"/ip/NMC-Signs-Dl76R-Hot-Dot-Placard-Sign-10-75-X-10-75-Rigid-Plastic-050/765417231","source_key":"url_text_key","source_value":"/ip/NMC-Signs-Dl76R-Hot-Dot-Placard-Sign-10-75-X-10-75-Rigid-Plastic-050/765417231"}]},"primary_shelf_id":{"properties":{"attributeId":"primary_shelf_id","attributeName":"Primary Shelf ID","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"isPrimary":"true","locale":"en_US","value":"922699"}]},"char_primary_category_path":{"properties":{"attributeId":"char_primary_category_path","attributeName":"Character Primary Category Path","source":"MARKETPLACE_PARTNER","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R"},"values":[{"isPrimary":"true","locale":"en_US","value":"Home Page/Home/Decor/Art & Wall Decor/Wall Decor"}]},"product_long_description":{"properties":{"attributeId":"84122","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Product Long Description","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Long Description","locale":"en_US","value":"<ul><li>MADE IN THE USA</li><li>Name:HOT DOT PLACARD SIGN</li><li>Height (In): 10.75</li><li>Width (In): 10.75</li><li>Material: RIGID PLASTIC .050</li><li>Legend: HOT</li></ul>","source_key":"long_description","source_value":"<ul><li>MADE IN THE USA</li><li>Name:HOT DOT PLACARD SIGN</li><li>Height (In): 10.75</li><li>Width (In): 10.75</li><li>Material: RIGID PLASTIC .050</li><li>Legend: HOT</li></ul>"}]},"wmt_category":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"WMT Category","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","isFacet":"true"},"values":[{"isFacetValue":"true","locale":"en_US","value":"Wall_Decor"}]},"facet_product_type":{"properties":{"taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Facet Product Type","source":"MARKETPLACE_PARTNER","isFacet":"true"},"values":[{"isFacetValue":"true","facet_version":"1.1","locale":"en_US","value":"Plaques & Signs"}]},"brand":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"10848","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeName":"Brand","updated":"Fri Sep 22 05:28:44 UTC 2017","isFacet":"true","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Brand","locale":"en_US","value":"NMC","source_key":"brand","source_value":"NMC"},{"isFacetValue":"true","facet_version":"1.1","locale":"en_US","value":"NMC"}]},"display_status":{"properties":{"attributeId":"94989","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","isMarketAttribute":"true","attributeName":"Display Status","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Display Status","locale":"en_US","value":"STAGING","source_key":"display_status","source_value":"STAGING"}]},"gtin":{"properties":{"multiselect":"N","attributeId":"-1","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"GTIN","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"locale":"en_US","source_key":"gtin","value":"00887481042145","display_attr_name":"GTIN","source_value":"00887481042145","isPrimary":"true"}]},"rh_path":{"properties":{"attributeId":"94969","isMultiValued":"false","taxonomy_version":"version","isMarketAttribute":"true","attribute_type":"STRING","attributeName":"RH Path","taxonomy":"pcs","display_attr_name":"rh_path","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9"},"values":[{"isPrimary":"true","display_attr_name":"rh_path","locale":"en_US","value":"40000:45000:45004:45121:45365"}]},"item_id":{"properties":{"attributeId":"-1","fromPcp":"false","generated_by":"Qarth","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"ITEM_ID","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"isPrimary":"true","display_attr_name":"ITEM_ID","locale":"en_US","value":"765417231","source_key":"item_id","source_value":"765417231"}]},"upc":{"properties":{"multiselect":"N","attributeId":"-1","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"UPC","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017","content_lifecycle_status":"ACTIVE","generated_from_matching":"true"},"values":[{"locale":"en_US","source_key":"upc","value":"887481042145","display_attr_name":"UPC","source_value":"887481042145","isPrimary":"true"}]},"product_pt_family":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"product_pt_family","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product PT Family","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"id":"home_decor_pcs","locale":"en_US","value":"Home Decor","tag_source":"model_guess"}]},"product_name":{"properties":{"multiselect":"N","attributeId":"84123","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Product Name","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Name","locale":"en_US","value":"NMC Signs Dl76R, Hot Dot Placard Sign, 10.75 X 10.75, Rigid Plastic .050","source_key":"product_name","source_value":"NMC Signs Dl76R, Hot Dot Placard Sign, 10.75 X 10.75, Rigid Plastic .050"}]},"shelf_description":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"-2126304598","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","isMarketAttribute":"true","attributeName":"Shelf Description","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Shelf Description","locale":"en_US","value":"<ul><li>Items ship from manufacturer in 2 days</li><li>Made in the USA</li><li>Hot Dot Placard Sign</li></ul>","source_key":"shelf_description","source_value":"<ul><li>Items ship from manufacturer in 2 days</li><li>Made in the USA</li><li>Hot Dot Placard Sign</li></ul>"}]},"product_tax_code":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"urn:taxonomy:pcs2.0","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","multiselect":"N","attributeId":"84126","isOfferAttribute":"true","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeName":"Product Tax Code","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Product Tax Code","locale":"en_US","value":"2038711","source_key":"product_tax_code","source_value":"2038711"}]},"condition":{"properties":{"multiselect":"N","attributeId":"10314","fromPcp":"false","added":"Fri Sep 22 17:06:12 UTC 2017","requestId":"reingest1-dab1aa83-efd4-48ef-a282-7b7e88f4dbdf","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","taxonomy_version":"urn:taxonomy:pcs2.0","backfill":"true","attributeName":"Condition","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","updated":"Thu Nov 09 06:10:29 UTC 2017"},"values":[{"locale":"en_US","source_key":"condition","value":"New","display_attr_name":"Condition","source_value":"New","tag_source":"model_predictions:ae2","isPrimary":"true"}]},"all_shelves":{"properties":{"attributeId":"shelf","added":"Wed Nov 08 22:10:29 PST 2017","rule_changed":"N","attributeName":"All Shelves","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"all_paths_str":"[["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]]","path_id":"["0","4044","133012","1045881","922699"]","all_paths_id":"[["0","4044","133012","1045881","922699"]]","taxonomy_version":"~draft","id":"922699","taxonomy":"site-dotcom","locale":"en_US","value":"Wall Decor","tag_source":"normalization","path_str":"["Home Page","Home","Decor","Art & Wall Decor","Wall Decor"]"}]},"product_type":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"product_type","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product Type","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"idPath":"[["home_and_garden","home_decor_pcs","1916"]]","charPath":"[["Home & Garden","Home Decor","Plaques & Signs"]]","id":"1916","locale":"en_US","value":"Plaques & Signs","tag_source":"model_guess"}]},"material":{"properties":{"attributeId":"10312","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Material","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"materialValue":"RIGID PLASTIC .050","isPrimary":"true","display_attr_name":"Material","locale":"en_US","value":"RIGID PLASTIC .050","source_key":"material","source_value":"RIGID PLASTIC .050"}]},"manufacturer_part_number":{"properties":{"multiselect":"N","attributeId":"10336","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Manufacturer Part Number","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Manufacturer Part Number","locale":"en_US","value":"DL76R","source_key":"manufacturerpartnumber","source_value":"DL76R"}]},"offer_lifecycle_status":{"properties":{"fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017"},"values":[{"display_attr_name":"offer_lifecycle_status","locale":"en_US","value":"ACTIVE"}]},"product_short_description":{"properties":{"multiselect":"N","attributeId":"84128","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"Short Description","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"isPrimary":"true","display_attr_name":"Short Description","locale":"en_US","value":"Hot substances can cause serious burns when they come in contact with ones skin. Prevent an accident by labeling designated vehicles carrying hot substances with this placard sign.","source_key":"short_description","source_value":"Hot substances can cause serious burns when they come in contact with ones skin. Prevent an accident by labeling designated vehicles carrying hot substances with this placard sign."}]},"reporting_hierarchy":{"properties":{"attributeId":"rhid","taxonomy_version":"~draft","attributeName":"Site Taxonomy","taxonomy":"rhid-dotcom","source":"BRAAVOS","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","status":"VERIFIED"},"values":[{"is_updated":"false","path_id":"["40000","45000","45004","45121","45365"]","id":"45365","locale":"en_US","value":"OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4","tag_source":"rule","path_str":"["EVERYDAY LIVING","OFFICE PRODUCTS AND SCHOOL SUPPLY","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L2","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L3","OFFICE PRODUCTS AND SCHOOL SUPPLY MISC L4"]"}]},"product_category":{"properties":{"added":"Thu Mar 16 11:59:24 UTC 2017","taxonomy_version":"~draft","taxonomy":"pcs","source":"braavos","qid":"BRAAVOS#0ceec947-5172-46e4-8dc4-8e37b4ffde80#6HSK-TDWA-JEW9","orgId":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","attributeId":"segment","org_id":"0ceec947-5172-46e4-8dc4-8e37b4ffde80","requestId":"PIG-BF16B8FEBD1B4DE393CCEB1079FF51BB@ARABAQA/765417231/14","attributeName":"Product Category","classification_type":"PRODUCT_TYPE","updated":"Mon Oct 16 06:56:05 UTC 2017","status":"VERIFIED"},"values":[{"id":"home_and_garden","locale":"en_US","value":"Home & Garden","tag_source":"model_guess"}]},"ironbank_category":{"properties":{"attributeId":"-19436162","fromPcp":"false","added":"Thu Mar 16 11:59:24 UTC 2017","requestId":"reingest1-689475a0-ccfa-4b36-8af2-8ff6c89ba6f4","org_id":"9dd773f1-f59f-4c51-b6e2-08a313ed43b7","taxonomy_version":"urn:taxonomy:pcs2.0","attributeName":"IronBank Category","source":"mp","qid":"MARKETPLACE_PARTNER#9dd773f1-f59f-4c51-b6e2-08a313ed43b7#DL76R","updated":"Fri Sep 22 05:28:44 UTC 2017","content_lifecycle_status":"ACTIVE"},"values":[{"idPath":"["office","office_other"]","charPath":"["Office","Office"]","isPrimary":"true","display_attr_name":"IronBank Category","locale":"en_US","value":"Office","source_key":"ironbank_category","source_value":"Office"}]}}, "day":"today"}'

              org_id = 'BRAAVOS'
              source = 'BRAAVOS' 
              
              source = source.replace(',', '_').replace('@', '_at_').replace(' ', '_').replace('&', '_and_').replace('#', '_').replace('\t', '') # ',' and '@' are reserved for script use
              org_id = org_id.replace(',', '_').replace('@', '_at_').replace(' ', '_').replace('&', '_and_').replace('#', '_').replace('\t', '') # ',' and '@' are reserved for script use
          
              product_attributes_jsona = product_attributes_jsona.strip().replace("\n", " ").replace("\r", " ").replace("\t", " ").replace('"[', '[').replace(']"', ']')
              product_attributes_jsonb = product_attributes_jsonb.strip().replace("\n", " ").replace("\r", " ").replace("\t", " ").replace('"[', '[').replace(']"', ']')
              self.compareJson(product_attributes_jsona, product_attributes_jsonb, source, org_id)
              results_s = ",".join(self.results).strip()
              self.results = []
              print results_s
          except Exception as e:
              self.results = []
              #print ''

    def processTestData(self):
        product_attributes_jsona = '{"pcf" : {"actual_color" : {"values" : [{"value" : "Womens_T-Shirts"}]}}, "day":"today"}'
        product_attributes_jsonb = '{"pcf" : {"actual_color" : {"values" : [{"value" : "Womens_T-Shirts2"}]}}, "day":"yesterday"}'
        source = "ABCD"
        org_id = "id123" 
        self.compareJson(product_attributes_jsona, product_attributes_jsonb, source, org_id)
        results_s = ",".join(self.results).strip()
        self.results = []
        
        print results_s
###################################################################        
def run(): 
    ptaUdf = ProductTopAttributesUdf() 
    ptaUdf.processInputData() 
    #ptaUdf.processInputDataSh() 
    #ptaUdf.processInputDataUTF() 
    #ptaUdf.processTestData()  # For testing
    #ptaUdf.processTestData2()  # For testing
###################################################################        
run()
#######################################################################


