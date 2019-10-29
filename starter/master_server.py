#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Chenxi Li on 2019-10-28
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import json
import sys

tablet_dict = {}
tablet_list = []
table_list = {"tables": []}
tables_info = {}
global tablet_index
tablet_index = 0

def print_info():
    print("================ tablet_dict =================")
    print(tablet_dict)
    print("================ table_list =================")
    print(table_list)
    print("================ table_info =================")
    print(tables_info)


def check_json(input):
    try:
        json.loads(input)
        return True
    except:
        return False

class MyHandler(BaseHTTPRequestHandler):
    def create_table(self, input):
        table_name = input.get("name")
        if table_name in table_list.get("tables"):
            return False
        else:
            # update table_list
            table_list["tables"].append(table_name)
            global tablet_index
            current_tablet = tablet_list[tablet_index % len(table_list)]
            tablet_index += 1
            url = "http://" + current_tablet + "/api/tables"
            response = requests.post(url, json=input)
            if response.status_code == 200:
                hostname = current_tablet.split(":")[0]
                port = current_tablet.split(":")[1]
                # update table_dict
                tablet_dict[current_tablet].append(table_name)
                return_json = {"hostname": hostname, "port": port}

                # update table_info
                tables_info[table_name] = {}
                tables_info[table_name]["name"] = table_name
                tables_info[table_name]["tablets"] = []
                tables_info[table_name]["tablets"].append({"hostname": hostname, "port": port, "row_from": "", "row_to": ""})

                self._set_response(200)
                self.wfile.write(return_json.encode("utf8"))
                return True


    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        # example: this is how you get path and command
        print(self.path)
        print(self.command)

        # example: returning an object as JSON
        data = {
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": "1234"
                }
            ]
        }
        data_json = json.dumps(data)

        self._set_response(200)
        self.wfile.write(data_json.encode("utf8"))

    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']

        if content_length != None:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            request_path = self.path
            if len(request_path.split("/")) >= 3:
                path_1 = request_path.split("/")[2]
                print(path_1)
                # register a tablet server
                if path_1 == 'join':
                    json_value = json.loads(data)
                    tablet_host = json_value.get("host_name")
                    tablet_port = json_value.get("host_port")
                    tablet_path = tablet_host + ":" + str(tablet_port)
                    tablet_dict[tablet_path] = []
                    tablet_list.append(tablet_path)
                    print_info()
                # create a table
                elif path_1 == 'tables':
                    if not check_json(data):
                        self._set_response(400)
                        return

                    flag = self.create_table(json.loads(data))
                    if not flag:
                        self._set_response(409)
                    else:
                        self._set_response(200)
                    print_info()

            # print the content, just for you to see it =)
            print(data)

        self._set_response(200)

    def do_DELETE(self):
        # example: send just a 200
        self._set_response(200)


if __name__ == "__main__":
    host_name = sys.argv[1]
    host_port = int(sys.argv[2])

    server_address = (host_name, host_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()

