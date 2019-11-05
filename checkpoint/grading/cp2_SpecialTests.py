import requests, unittest, json, time
from MySupport import MySupport

class SpecialTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80
    MAX_UNIQUE_ROWS = 1000
    MEM_TABLE_LIMIT = 100
    EXTRA_ROWS = 200

    def suite():
        suite = unittest.TestSuite()
        suite.addTest(SpecialTests('test_recovery'))
        suite.addTest(SpecialTests('test_shard'))

        return suite

    def test_shard(self):
        print ("\nBoot up a new tablet server in case you have only one of them actively running.")
        print ("Hit enter when done.")
        input()

        url_master =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        # create - success
        table_dict = {
                "name": "table_shard",
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

        response = requests.post(url_master, json=table_dict)
        self.assertEqual(response.status_code, 200)

        # getinfo - tablet hostname and port
        response = requests.get(url_master + "/table_shard")
        self.assertEqual(response.status_code, 200)

        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "table_shard")
        self.assertEqual(len(tablet_info["tablets"]), 1)

        tablet_hostname = tablet_info["tablets"][0]["hostname"]
        tablet_port = tablet_info["tablets"][0]["port"]

        # overflow the number of unique rows in one tablet server &
        # since we continue to insert in the increemental key order
        # we shouldn't be redirected. That is also the reason row (below)
        # is an integer so that there is no ambiguity about the key order.
        url_tablet = MySupport.url(tablet_hostname, tablet_port, "/api/table/table_shard/cell")
        for i in range(self.MAX_UNIQUE_ROWS + self.EXTRA_ROWS):
            response = requests.post(url_tablet, 
                    json={
                        "column_family": "fam1", "column": "key1",
                        "row": i, "data": [{
                            "value" : str(i),
                            "time" : 0
                        }]
                    })
            self.assertEqual(response.status_code, 200)
            self.assertFalse(response.content)

        # getinfo - tablet hostname and port
        response = requests.get(url_master + "/table_shard")
        self.assertEqual(response.status_code, 200)

        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "table_shard")
        self.assertEqual(len(tablet_info["tablets"]), 2)

        tablet1 = {
            'hostname' : tablet_info["tablets"][0]["hostname"],
            'port' : tablet_info["tablets"][0]["port"]
        }

        tablet2 = {
            'hostname' : tablet_info["tablets"][1]["hostname"],
            'port' : tablet_info["tablets"][1]["port"]
        }
        
        # The two tablets should be different
        self.assertNotEqual(tablet1, tablet2)

        tablet1['row_from'] = str(tablet_info["tablets"][0]["row_from"])
        tablet1['row_to'] = str(tablet_info["tablets"][0]["row_to"])

        tablet2['row_from'] = str(tablet_info["tablets"][1]["row_from"])
        tablet2['row_to'] = str(tablet_info["tablets"][1]["row_to"])
            
        # Check whether the division was equal or not
        match_found = False
        tablet_left = None
        tablet_right = None
        divider_row = str(self.MAX_UNIQUE_ROWS // 2)

        if tablet1['row_from'] == divider_row:
            match_found = True
            tablet_left = tablet2
            tablet_right = tablet1
            self.assertEqual(tablet2['row_to'], divider_row)

        elif tablet2['row_from'] == divider_row:
            match_found = True
            tablet_left = tablet1
            tablet_right = tablet2
            self.assertEqual(tablet1['row_to'], divider_row)

        self.assertTrue(match_found)

        # Read row id = 0 from the left shard (lower order keys) - success
        url_tablet = MySupport.url(tablet_left['hostname'], tablet_left['port'], 
                                   "/api/table/table_shard/cell")

        request = {
            "column_family": "fam1",
            "column": "key1",
            "row": 0,
        }
                
        expected = {
            "row": 0,
            "data": [{
                "value": "0",
                "time": 0
            }]
        }

        response = requests.get(url_tablet, json = request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)

        # Read row id = divider from the right shard (higher order keys) - success
        url_tablet = MySupport.url(tablet_right['hostname'], tablet_right['port'],
                                   "/api/table/table_shard/cell")
        
        request["row"] = int(divider_row)
        expected["row"] = int(divider_row)
        expected["data"][0]["value"] = divider_row

        response = requests.get(url_tablet, json = request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)

        # delete the shard table
        response = requests.delete(url_master + "/table_shard")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def test_recovery(self):
        url_master =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        # create - success
        table_dict = {
                "name": "table_rcvr",
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

        response = requests.post(url_master, json=table_dict)
        self.assertEqual(response.status_code, 200)

        # getinfo - tablet hostname and port
        response = requests.get(url_master + "/table_rcvr")
        self.assertEqual(response.status_code, 200)

        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "table_rcvr")
        self.assertEqual(len(tablet_info["tablets"]), 1)

        tablet_hostname = tablet_info["tablets"][0]["hostname"]
        tablet_port = tablet_info["tablets"][0]["port"]

        # overflow the maximum number of rows in a memtable
        # to make recovery occur over spills
        url_tablet = MySupport.url(tablet_hostname, tablet_port, "/api/table/table_rcvr/cell")
        for i in range(self.MEM_TABLE_LIMIT + self.EXTRA_ROWS):
            response = requests.post(url_tablet, 
                    json={
                        "column_family": "fam1", "column": "key1",
                        "row": "row_" + str(i), "data": [{
                            "value" : str(i),
                            "time" : 0
                        }]
                    })
            self.assertEqual(response.status_code, 200)
            self.assertFalse(response.content)

        print ("\nNow kill the tablet server hosted at {}:{}.".format(tablet_hostname, tablet_port))
        print("Hit enter when you done.")
        input()

        print("Going to sleep for 1 minute for recovery to complete.")
        time.sleep(60)

        print ("Woken up from sleep. Continuing to read data.")

        # getinfo - tablet hostname and port
        response = requests.get(url_master + "/table_rcvr")
        self.assertEqual(response.status_code, 200)

        new_tablet_info = response.json()
        self.assertEqual(new_tablet_info["name"], "table_rcvr")
        self.assertEqual(len(new_tablet_info["tablets"]), 1)

        new_tablet_hostname = new_tablet_info["tablets"][0]["hostname"]
        new_tablet_port = new_tablet_info["tablets"][0]["port"]

        self.assertNotEqual(tablet_hostname, new_tablet_hostname)
        url_tablet = MySupport.url(new_tablet_hostname, new_tablet_port, "/api/table/table_rcvr/cell")

        for i in range(self.MEM_TABLE_LIMIT + self.EXTRA_ROWS):
            request = {
                "column_family": "fam1",
                "column": "key1",
                "row": "row_" + str(i),
            }
                
            expected = {
                "row": "row_" + str(i),
                "data": [{
                    "value": str(i),
                    "time": 0
                }]
            }

            response = requests.get(url_tablet, json = request)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), expected)

        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        # delete the recovery table
        response = requests.delete(url_master + "/table_rcvr")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        print ("Done with testing recovery.")

