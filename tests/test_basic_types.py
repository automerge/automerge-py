import automerge
import unittest


# WIP : once the frontend is finished, add the proper asserts

class BasicTypesTestCase(unittest.TestCase):

    def test_init_basic_types(self):

        doc1 = automerge.Doc()
        with doc1 as d:
            d["key_none"]  =  None 
            d["key_bool"]  =  True 
            d["key_int"]  =  99 
            d["key_float"]  =  0.123 
            d["key_str"]  =  "string value" 
                        
            # the following requires a fix in Automerge-rs : https://github.com/automerge/automerge-rs/issues/44
            #d["key_utf8"] =   "🌍🌎🌏"
            #d["key_list_utf8"] =   : [ "السلام عليكم", "Dobrý den", "שָׁלוֹם", "नमस्ते", "こんにちは", "안녕하세요", "你好", "Olá", "Здравствуйте"]

            d["key_list_bools"] = [False, True]
            d["key_list_int"] = [1,2,3]
            d["key_list_str"] = ["a","bc","def", "GHIJ"]
            d["key_list_int_str"] = [1, "abcdefgh", 2, "ijklmnop", 3, "qrstuvwx"]
            d["key_dict_str_str"] = {"subkey1":"val1","subkey2":"val2"}   
            d["key_mixed"] = [ {"k1":"v1", "k2":2, "k3":[True, None, False, {"k31":"v31"}]} ]
            
