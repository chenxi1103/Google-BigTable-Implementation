import requests, unittest, json
import time
from MySupport import MySupport

class OpTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80

    def suite():
        suite = unittest.TestSuite()

        suite.addTest(OpTests('test_setup'))
        suite.addTest(OpTests('test_basic'))
        suite.addTest(OpTests('test_basic_error'))
        suite.addTest(OpTests('test_garbage_collection'))
        suite.addTest(OpTests('test_teardown'))
        suite.addTest(OpTests('test_metadata'))
        
        return suite

    def test_setup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        table_dict = {
                "name": "table_basic",
                "column_families": [
                    {
                        "column_family_key": "fam1",
                        "columns": ["key1", "key2"]
                    },
                    {
                        "column_family_key": "fam2",
                        "columns": ["key3", "key4"]
                    }
                ]
            }

        # create - success
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # create - success
        table_dict["name"] = "table_gc"
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # create - success
        table_dict["name"] = "table_metadata"
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def test_teardown(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        url_delete =  url + "/table_basic"
        url_gc =  url + "/table_gc"

        # remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # remove - success
        response = requests.delete(url_gc)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)



    def test_basic(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_basic/cell")
        url_range = MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_basic/cells")
        row_a_time = time.time()
        data = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": row_a_time
                }
            ]
        }
        retrieve_single = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
        }
        retrieve_range = {
            "column_family": "fam1",
            "column": "key1",
            "row_from": "sample_a",
            "row_to": "sample_d",
        }

        # insert single
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # insert single
        data["row"] = "sample_c"
        data["data"][0]["value"] = "data_c"
        row_c_time = time.time()
        data["data"][0]["time"] = row_c_time
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # insert single
        data["row"] = "sample_f"
        data["data"][0]["value"] = "data_f"
        row_f_time = time.time()
        data["data"][0]["time"] = row_f_time
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # retrieve single
        response = requests.get(url, json=retrieve_single)
        expected = {
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": row_a_time
                }
            ]
        }
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)

        # retrieve range
        response = requests.get(url_range, json=retrieve_range)
        expected = {
            "rows": [
                {
                    "row": "sample_a",
                    "data": [{
                        "value": "data_a",
                        "time": row_a_time
                    }]
                },
                {
                    "row": "sample_c",
                    "data": [{
                        "value": "data_c",
                        "time": row_c_time
                    }]
                }
            ]
        }

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)

    def test_basic_error(self): 
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_basic/cell")
        url_nope =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_nope/cell")
        data = {
            "column_family": "famX",
            "column": "key1",
            "row": "sample_z",
            "data": [{
                "value": "data_a",
                "time": time.time()
            }]
        }
        retrieve_single = {
            "column_family": "fam1",
            "column": "keyX",
            "row": "sample_z"
        }

        # insert - table does not exist 
        response = requests.post(url_nope)
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.content)

        # retrieve - table does not exist 
        response = requests.get(url_nope)
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.content)

        # insert - colfam not exist
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.content)

        # retrive - col not exist
        response = requests.get(url, json=retrieve_single)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.content)

    def test_garbage_collection(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_gc/cell")
        
        data = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": time.time()
                }
            ]
        }
        retrieve_single = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a"
        }

        response = requests.post(url, json=data)

        data["data"][0]["value"] = "data_b"
        data["data"][0]["time"] = time.time()
        response = requests.post(url, json=data)

        data["data"][0]["value"] = "data_c"
        data["data"][0]["time"] = time.time()
        response = requests.post(url, json=data)

        data["data"][0]["value"] = "data_d"
        data["data"][0]["time"] = time.time()
        response = requests.post(url, json=data)

        data["data"][0]["value"] = "data_e"
        data["data"][0]["time"] = time.time()
        response = requests.post(url, json=data)

        data["data"][0]["value"] = "data_f"
        data["data"][0]["time"] = time.time()
        response = requests.post(url, json=data)

        response = requests.get(url, json=retrieve_single)
        self.assertEqual(response.status_code, 200)

        received = response.json()
        self.assertEqual(received["row"], "sample_a")
        self.assertTrue(next((item for item in received["data"] if item["value"] == "data_b"), False))
        self.assertTrue(next((item for item in received["data"] if item["value"] == "data_c"), False))
        self.assertTrue(next((item for item in received["data"] if item["value"] == "data_d"), False))
        self.assertTrue(next((item for item in received["data"] if item["value"] == "data_e"), False))
        self.assertTrue(next((item for item in received["data"] if item["value"] == "data_f"), False))
        self.assertFalse(next((item for item in received["data"] if item["value"] == "data_a"), False))

 
    def test_metadata(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_metadata/cell")
        url_range = MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_metadata/cells")
        url_memtable =  MySupport.url(self.HOSTNAME, self.PORT, "/api/memtable")
        url_delete = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/table_metadata")
        
        data = {
            "column_family": "fam1",
            "column": "key1",
            "row": "",
            "data": []
        }
        memtable = {
            "memtable_max": 20
        }

        # set memtable max to 20
        response = requests.post(url_memtable, json=memtable)
        self.assertEqual(response.status_code, 200)

        # insert one entry
        data["row"] = "row_0"
        row_0_time = time.time()
        data["data"] = [{"value": "0", "time": row_0_time}]
        response = requests.post(url, json=data)

        # insert twenty entries
        for i in range(1,21):
            data["row"] = "row_" + str(i)
            data["data"] = [{"value": str(i), "time": time.time()}]
            response = requests.post(url, json=data)

        # insert another ten entries
        for i in range(10):
            data["row"] = "row_" + str(21+i)
            if 21+i == 25:
                row_25_time = time.time()
                data["data"] = [{"value": str(21+i), "time": row_25_time}]
            elif 21+i == 26:
                row_26_time = time.time()            
                data["data"] = [{"value": str(21+i), "time": row_26_time}]
            else:
                data["data"] = [{"value": str(21+i), "time": time.time()}]
            response = requests.post(url, json=data)

        # set memtable max to 5
        memtable["memtable_max"] = 5
        response = requests.post(url_memtable, json=memtable)
        self.assertEqual(response.status_code, 200)

        # search a row which has been spilled on disk
        retrieve_single = {
            "column_family": "fam1",
            "column": "key1",
            "row": "row_0",
        }
        response = requests.get(url, json=retrieve_single)
        expected = {
            "row": "row_0",
            "data": [
                {
                    "value": "0",
                    "time": row_0_time
                }
            ]
        }
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)

        # search range of entries - starting at row which is spilled, ending at
        # row which is in memtable
        retrieve_range = {
            "column_family": "fam1",
            "column": "key1",
            "row_from": "row_25",
            "row_to": "row_26",
        }
        response = requests.get(url_range, json=retrieve_range)
        expected = {
            "rows": [
                {
                    "row": "row_25",
                    "data": [{
                        "value": "25",
                        "time": row_25_time
                    }]
                },
                {
                    "row": "row_26",
                    "data": [{
                        "value": "26",
                        "time": row_26_time
                    }]
                }
            ]
        }
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(len(response.json().get("rows")), len(expected.get("rows")))

        # table remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # set memtable max to default 100
        memtable["memtable_max"] = 100
        response = requests.post(url_memtable, json=memtable)
        self.assertEqual(response.status_code, 200)
