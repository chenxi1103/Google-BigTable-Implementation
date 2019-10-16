import requests, unittest, json, os
from MySupport import MySupport

from cp1_TableTests import TableTests
from cp1_OpTests import OpTests

class BalancerTests(unittest.TestCase):
    HOSTNAME = "host"
    PORT = 80

    movie_schema = []
    movie_data = []
    movie_server = {}

    camera_schema = []
    camera_data = []
    camera_server = {}

    def suite():
        suite = unittest.TestSuite()

        suite.addTest(BalancerTests('test_populate_data'))
        suite.addTest(BalancerTests('test_setup'))
        suite.addTest(BalancerTests('test_rows'))
        suite.addTest(BalancerTests('test_teardown'))

        return suite

    def test_populate_data(self):
        movie_file = os.path.join(os.getcwd(), "../dataset/movies.csv")
        self.assertTrue(os.path.exists(movie_file))

        camera_file = os.path.join(os.getcwd(), "../dataset/camera.csv")
        self.assertTrue(os.path.exists(camera_file))

        # Read in the movies data
        lines = open(movie_file).read().splitlines()
        for field in lines[0].split(','):
            self.movie_schema.append(field)

        lines.pop(0)
        for line in lines:
            fields = line.split(',')
            if len(fields) == len(self.movie_schema):
                self.movie_data.append(fields)

        # Read in the camera data
        lines = open(movie_file).read().splitlines()
        for field in lines[0].split(','):
            self.camera_schema.append(field)

        lines.pop(0)
        for line in lines:
            fields = line.split(',')
            if len(fields) == len(self.camera_schema):
                self.camera_data.append(fields)

    def test_setup(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        # Create the movies table - success
        table_dict = {
            "name": "movies",
            "column_families": []
        }

        for col in self.movie_schema:
            table_dict['column_families'].append({
                'column_family_key' : col,
                'columns' : [col]
            })

        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)

        # Create the camera table - success
        table_dict = {
            "name": "cameras",
            "column_families": []
        }

        for col in self.camera_schema:
            table_dict['column_families'].append({
                'column_family_key' : col,
                'columns' : [col]
            })

        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        

        # get table info for the movies table
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/movies/")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "movies")
        self.assertEqual(len(tablet_info["tablets"]), 1)
        
        self.movie_server['hostname'] = tablet_info["tablets"][0]['hostname'];
        self.movie_server['port'] = tablet_info["tablets"][0]['port'];

        # get table info for the cameras table
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables/cameras/")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        tablet_info = response.json()
        self.assertEqual(tablet_info["name"], "cameras")
        self.assertEqual(len(tablet_info["tablets"]), 1)
        
        self.camera_server['hostname'] = tablet_info["tablets"][0]['hostname'];
        self.camera_server['port'] = tablet_info["tablets"][0]['port'];
        
        # Both tables shouldn't have been assigned to the same tablet due to load balancing.
        # Need to run atleast 2 tablet servers to pass this test.

        self.assertNotEqual(self.movie_server, self.camera_server)

    def test_rows(self):
        self.full_scan("movies", self.movie_schema, self.movie_data, self.movie_server)
        self.full_scan("cameras", self.camera_schema, self.camera_data, self.camera_server)

    def test_teardown(self):
        url =  MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        # remove the movies table
        url_delete =  url + "/movies"
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # remove the cameras table
        url_delete =  url + "/cameras"
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

    def full_scan(self, name, schema, csv_data, tablet):
        url_insert =  MySupport.url(tablet['hostname'], tablet['port'], "/api/table/" + name + "/cell")
        url_retrieve =  MySupport.url(tablet['hostname'], tablet['port'], "/api/table/" + name + "/cell")

        row_id = 0
        for data in csv_data:
            for i in range(len(data)):
                request = {
                    "column_family": schema[i],
                    "column": schema[i],
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
        for data in csv_data:
            for i in range(len(data)):
                request = {
                    "column_family": schema[i],
                    "column": schema[i],
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
