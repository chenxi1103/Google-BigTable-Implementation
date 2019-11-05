import requests, unittest, json
from MySupport import MySupport
import os

class StressTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80

    schema = []
    csv_data = []

    def suite():
        suite = unittest.TestSuite()

        suite.addTest(StressTests('test_populate_data'))

        suite.addTest(StressTests('test_setup'))
        suite.addTest(StressTests('test_rows'))
        suite.addTest(StressTests('test_teardown'))

        suite.addTest(StressTests('test_setup'))
        suite.addTest(StressTests('test_cols'))
        suite.addTest(StressTests('test_teardown'))

        return suite

    def test_populate_data(self):
        csv_file = os.path.join(os.getcwd(), "../dataset/movies.csv")
        self.assertTrue(os.path.exists(csv_file))

        lines = open(csv_file).read().splitlines()
        for field in lines[0].split(','):
            self.schema.append(field)

        lines.pop(0)
        for line in lines:
            fields = line.split(',')
            # considering only the complete lines
            if len(fields) == len(self.schema):
                self.csv_data.append(fields)

    def test_setup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        table_dict = {
            "name": "my_csv",
            "column_families": []
        }

        # using the same name for the column family and the column
        for col in self.schema:
            table_dict['column_families'].append({
                'column_family_key' : col,
                'columns' : [col]
            })

        # create - success
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def test_teardown(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        url_delete =  url + "/my_csv"

        # remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def test_rows(self):
        # Insert entries into the table in a row-major fashion
        # and re-read all the entries

        url_insert =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/my_csv/cell")
        url_retrieve =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/my_csv/cell")

        row_id = 0
        for data in self.csv_data:
            for i in range(len(data)):
                request = {
                    "column_family": self.schema[i],
                    "column": self.schema[i],
                    "row": row_id,
                    "data": [{
                        "value": data[i],
                        "time": row_id
                    }]
                }
                response = requests.post(url_insert, json=request)
                self.assertEqual(response.status_code, 200)

            row_id = row_id + 1
        
        row_id = 0
        for data in self.csv_data:
            for i in range(len(data)):
                request = {
                    "column_family": self.schema[i],
                    "column": self.schema[i],
                    "row": row_id,
                }
                
                expected = {
                    "row": row_id,
                    "data": [{
                        "value": data[i],
                        "time": row_id
                    }]
                }

                response = requests.get(url_retrieve, json=request)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)

            row_id = row_id + 1

    
    def test_cols(self):
        # Insert entries into the table in a column-major fashion
        # and re-read all the entries

        url_insert =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/my_csv/cell")
        url_retrieve =  MySupport.url(self.HOSTNAME, self.PORT, "/api/table/my_csv/cell")

        for i in range(len(self.schema)):
            row_id = 0
            for data in self.csv_data:
                request = {
                    "column_family": self.schema[i],
                    "column": self.schema[i],
                    "row": row_id,
                    "data": [{
                        "value": data[i],
                        "time": row_id
                    }]
                }
                response = requests.post(url_insert, json=request)
                self.assertEqual(response.status_code, 200)
                row_id = row_id + 1
        
        for i in range(len(self.schema)):
            row_id = 0
            for data in self.csv_data:
                request = {
                    "column_family": self.schema[i],
                    "column": self.schema[i],
                    "row": row_id,
                }
                
                expected = {
                    "row": row_id,
                    "data": [{
                        "value": data[i],
                        "time": row_id
                    }]
                }

                response = requests.get(url_retrieve, json=request)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)
                row_id = row_id + 1

