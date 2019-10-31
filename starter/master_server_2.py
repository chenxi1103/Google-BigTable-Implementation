#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Chenxi Li on 2019-10-28
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import json
import sys
import time
from threading import Thread
from threading import Timer

tablet_dict = {}
tablet_list = []
table_list = {"tables": []}
tables_info = {}
global tablet_index
tablet_index = 0
lock_tables = {}


def print_info():
    print("================ tablet_dict =================")
    print(tablet_dict)
    print("================ table_list =================")
    print(table_list)
    print("================ table_info =================")
    print(tables_info)
    print("================ locked_table =================")
    print(lock_tables)


def check_json(input):
    try:
        json.loads(input)
        return True
    except:
        return False


def update_table_info(jsonvalue):
    tablet = jsonvalue["tablet"]
    data = jsonvalue["data"]
    for table_name in data:
        tablets = tables_info[table_name]['tablets']
        for tablet_item in tablets:
            if tablet_item["hostname"] == tablet.split(":")[0] and str(tablet_item["port"]) == str(
                    tablet.split(":")[1]):
                if len(data[table_name]) != 0:
                    tablet_item["row_from"] = data[table_name][0]
                    tablet_item["row_to"] = data[table_name][len(data[table_name]) - 1]
                    break


def transfer_table(dead_tablet, old_tables):
    for table in old_tables:
        if table in tables_info:
            tablet_lst = tables_info[table]['tablets']
            for server in tablet_lst:
                host = server['hostname']
                port = server['port']
                host_port_old = host + ":" + port
                if host_port_old == dead_tablet:
                    tablet_lst.remove(server)


def recover(dead_tablet):
    print("in recover func")
    for tablet in tablet_dict:
        print(tablet_dict)
        print("to recover")
        if tablet != dead_tablet:
            print("get recover to old list")
            recover_url = "http://" + tablet + "api/read/" + dead_tablet
            t1 = Thread(target=requests.post, args=recover_url)
            t1.start()
            # response = requests.post(recover_url)
            if dead_tablet in tablet_dict:
                tables = tablet_dict.get(dead_tablet)
                tablet_dict[tablet].extend(tables)
                del tablet_dict[dead_tablet]
                transfer_table(dead_tablet, tables)


class MyHandler(BaseHTTPRequestHandler):
    def lock_table(self, table_name, client):
        if table_name not in table_list['tables']:
            self._set_response(404)
            return
        if client not in lock_tables:
            lock_tables[client] = []
        if table_name in lock_tables[client]:
            self._set_response(400)
            return
        else:
            lock_tables[client].append(table_name)
            self._set_response(200)
            return

    def release_lock(self, table_name, client):
        if table_name not in table_list['tables']:
            self._set_response(404)
            return

        if client not in lock_tables:
            self._set_response(400)
            return
        else:
            if table_name not in lock_tables[client]:
                self._set_response(400)
                return
            else:
                lock_tables[client].remove(table_name)
                self._set_response(200)
                return

    def create_table(self, input):
        table_name = input.get("name")
        if table_name in table_list["tables"]:
            self._set_response(409)
            return
        else:
            # update table_list
            table_list["tables"].append(table_name)
            global tablet_index
            current_tablet = tablet_list[tablet_index % len(tablet_list)]
            tablet_index += 1
            url = "http://" + current_tablet + "/api/tables"
            response = requests.post(url, json=input)
            if response.status_code == 200:
                hostname = current_tablet.split(":")[0]
                port = current_tablet.split(":")[1]
                # update table_dict
                tablet_dict[current_tablet].append(table_name)
                return_json = json.dumps({"hostname": hostname, "port": port})

                # update table_info
                tables_info[table_name] = {}
                tables_info[table_name]["name"] = table_name
                tables_info[table_name]["tablets"] = []
                tables_info[table_name]["tablets"].append(
                    {"hostname": hostname, "port": port, "row_from": "", "row_to": ""})
                self._set_response(200)
                self.wfile.write(return_json.encode("utf8"))
                print_info()
                return

    def retrieve_cell(self, table_name):
        content_length = self.headers['content-length']
        if content_length:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            data_json = json.loads(data)
            row_value = data_json.get("row")
            table_dic = tables_info.get(table_name)
            for tablet in table_dic['tablets']:
                if tablet['row_from'] <= row_value <= tablet['row_to']:
                    host = tablet['hostname']
                    port = tablet['port']
                    url = "http://" + str(host) + ":" + str(port) + "/api/table/" + table_name + "/cell"
                    response = requests.get(url)
                    if response.status_code != 200:
                        self._set_response(response.status_code)
                        return
            self._set_response(200)
            return

    def check_heartbeat(self):
        while True:
            for tablet in tablet_dict.keys():
                try:
                    # while True:
                    url = "http://" + tablet + "/api/heart"
                    print("get heart beat")
                    requests.get(url)
                    time.sleep(10)
                except requests.ConnectionError:
                    self.recover(tablet)

    def delete_table(self, table_name):
        for client in lock_tables:
            if table_name in lock_tables[client]:
                self._set_response(409)
                return

        # update table list
        table_list["tables"].remove(table_name)
        # update table info
        del tables_info[table_name]
        for tablet in tablet_dict:
            if table_name in tablet_dict[tablet]:
                tablet_dict[tablet].remove(table_name)

                url = "http://" + tablet + "/api/tables/" + table_name
                response = requests.delete(url)
                if response.status_code != 200:
                    self._set_response(response.status_code)
                    return
        self._set_response(200)
        return

    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        if self.command == 'GET':
            url = self.path.split('/')[1:]
            if url[0] == 'api' and url[1] == 'tables' and len(url) <= 4:
                # List Tables
                if len(url) == 2:
                    data = table_list
                    data_json = json.dumps(data)
                    self._set_response(200)
                    self.wfile.write(data_json.encode("utf8"))
                    return
                # Get Table info
                elif len(url) > 2:
                    table_name = url[2]
                    if table_name in tables_info:
                        data = tables_info.get(table_name)
                        data_json = json.dumps(data)
                        self._set_response(200)
                        self.wfile.write(data_json.encode("utf8"))
                        return
                    else:
                        self._set_response(404)
                        return
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'cell':
                # Retrieve a cell
                table_name = url[2]
                if table_name not in table_list["tables"]:
                    self._set_response(404)
                    return
                self.retrieve_cell(table_name)
        print_info()

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
                # create a table
                elif path_1 == 'tables':
                    if not check_json(data):
                        self._set_response(400)
                        return
                    else:
                        self.create_table(json.loads(data))
                        return

                # update row key
                elif path_1 == 'update_rowkey':
                    json_value = json.loads(data)
                    update_table_info(json_value)
                    self._set_response(200)

                # lock a table
                elif path_1 == 'lock':
                    table_name = request_path.split("/")[3]
                    json_value = json.loads(data)
                    client = json_value["client_id"]
                    self.lock_table(table_name, client)
                print_info()

        self._set_response(200)

    def do_DELETE(self):
        content_length = self.headers['content-length']
        if content_length:
            content_length = int(content_length)
            data = self.rfile.read(content_length)
            url = self.path.split('/')[1:]
            print(url)
            if len(url) >= 2:
                # delete a table
                if url[1] == "tables":
                    table_name = url[2]
                    if table_name not in table_list["tables"]:
                        # table not exist in table list
                        self._set_response(404)
                        return
                    else:
                        self.delete_table(table_name)

                # release the lock
                if url[1] == "lock":
                    table_name = url[2]
                    json_value = json.loads(data)
                    client = json_value["client_id"]
                    self.release_lock(table_name, client)
            print_info()

def run():
    while True:
        print("in heart")
        for tablet in tablet_dict:
            print("try not in")
            try:
                url = "http://" + tablet + "/api/heart"
                print("get heart beat")
                requests.get(url)
            except requests.ConnectionError:
                recover(tablet)
        time.sleep(10)

if __name__ == "__main__":
    host_name = sys.argv[1]
    host_port = int(sys.argv[2])

    server_address = (host_name, host_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    try:
        recover("localhost:8081")
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
