import requests, unittest, json
from MySupport import MySupport

from cp1_TableTests import TableTests
from cp1_OpTests import OpTests

class MasterTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80

    def suite():
        suite = unittest.TestSuite()
        suite.addTest(MasterTests('test_setup'))
        suite.addTest(MasterTests('test_open_close'))
        suite.addTest(MasterTests('test_use'))
        suite.addTest(MasterTests('test_cleanup'))

        return suite

    def test_use(self):
        url_master =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/table1/")

        # getinfo - tablet hostname and port
        response = requests.get(url_master)
        self.assertEqual(response.status_code, 200)

        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "table1")
        self.assertEqual(len(tablet_info["tablets"]), 1)

        tablet_hostname = tablet_info["tablets"][0]["hostname"]
        tablet_port = tablet_info["tablets"][0]["port"]

        print("HOST: " + tablet_hostname)
        print("PORT: " + str(tablet_port))

        # insert one
        url_tablet = MySupport.url(tablet_hostname, tablet_port, "/api/table/table1/cell")
        insert = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
            "data": [{
                        "value" : "data_a",
                        "time" : 1

            }]
        }

        response = requests.post(url_tablet, json=insert)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # retrieve one
        retrieve_single = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a"
        }
        response = requests.get(url_tablet, json=retrieve_single)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)

        json = response.json()
        self.assertEqual(json["row"], "sample_a")
        self.assertEqual(len(json["data"]), 1)
        self.assertEqual(json["data"][0]["value"], "data_a")
        self.assertEqual(json["data"][0]["time"], 1)

    def test_setup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)

        # list empty
        json = response.json()
        expected = {"tables": []}
        self.assertEqual(json, expected)

        table_dict = {
                "name": "table1",
                "column_families": [
                    {
                        "column_family_key": "fam1",
                        "columns": ["key1", "key2"]
                    }, 
                    {
                        "column_family_key": "fam2",
                        "columns": ["key3", "key3"]           
                    }
                ]
            }

        # create - success
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)

        # create - already exist
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.content)

        # create - not JSON
        response = requests.post(url, data="omgwtfbbq")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.content)

        # list with one
        response = requests.get(url)
        expected = {"tables": ["table1"]}
        self.assertEqual(response.json(), expected)

        # get table info
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/table1")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "table1")
        self.assertEqual(len(tablet_info["tablets"]), 1)

    def test_open_close(self): 
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/lock/")
        url_nope = url + "tablenope"
        url_table = url + "table1"

        data = {
            "client_id": "client1"
        }

        # open - does not exist
        response = requests.post(url_nope, json=data)
        self.assertEqual(response.status_code, 404)

        # open - success
        response = requests.post(url_table, json=data)
        self.assertEqual(response.status_code, 200)

        # open - already opened
        response = requests.post(url_table, json=data)
        self.assertEqual(response.status_code, 400)

        # open - second client
        data["client_id"] = "client2"
        response = requests.post(url_table, json=data)
        self.assertEqual(response.status_code, 200)

        # close - success
        response = requests.delete(url_table, json=data)
        self.assertEqual(response.status_code, 200)

        # close - not holding table
        response = requests.delete(url_table, json=data)
        self.assertEqual(response.status_code, 400)

        # close - not exist
        response = requests.delete(url_nope, json=data)
        self.assertEqual(response.status_code, 404)
        
    def test_cleanup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/")
        url_delete =  url + "table1"
        url_delete_nope =  url + "tablenope"
        url_unlock =  MySupport.url(self.HOSTNAME, self.PORT, "/api/lock/table1")

        # remove - table in use
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.content)

        # close - success
        response = requests.delete(url_unlock, json={"client_id": "client1"})
        self.assertEqual(response.status_code, 200)

        # remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # remove - not exist
        response = requests.delete(url_delete_nope)
        self.assertEqual(response.status_code, 404)
