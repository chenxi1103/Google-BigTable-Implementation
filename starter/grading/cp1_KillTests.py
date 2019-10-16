import requests, unittest, json
from MySupport import MySupport
import time

class KillTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80

    def suite():
        suite = unittest.TestSuite()

        suite.addTest(KillTests('test_setup'))
        suite.addTest(KillTests('test_kill'))
        suite.addTest(KillTests('test_teardown'))

        return suite

    def test_setup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        table_dict = {
                "name": "table_kill",
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

    def test_teardown(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/table_kill")

        # remove - success
        response = requests.delete(url)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def test_kill(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/table_kill/cell")
        ts = time.time()

        data = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
            "data": [{
                "value": "1",
                "time": ts
            }]
        }

        retrieve_single = {
            "column_family": "fam1",
            "column": "key1",
            "row": "sample_a",
        }

        # insert single
        response = requests.post(url, json=data)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # request KILL
        print("\033[1;31;40m \nKILL AND RESTART THE SERVER  \n")
        print("\033[1;31;40m hit enter when you done...  \n \033[0m")
        input()

        # retrieve single
        response = requests.get(url, json=retrieve_single)
        expected = {
            "row": "sample_a",
            "data": [
                {
                    "value": "1",
                    "time": ts
                }
            ]
        }
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)

