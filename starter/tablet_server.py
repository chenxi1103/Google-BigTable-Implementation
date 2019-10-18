#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Chenxi Li on 2019-10-17
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import collections

memtables = {}
tables_index = {}
tables_info = {}
table_list = {"tables": []}
global tables_max_size
tables_max_size = 100
global num_row_key
num_row_key = 0
DISK_PATH = "disk/"


def check_tables():
    print("============= Table Index ===============")
    print(tables_index)
    print("============= Table Info ===============")
    print(tables_info)
    print("============= Table list ===============")
    print(table_list)
    print("============= Memtables ===============")
    print(memtables)

def spill_to_the_disk():
    if num_row_key >= tables_max_size:
        for table in memtables:
            try:
                with open(DISK_PATH + str(table) + ".json", "r") as db:
                    load_table = json.load(db)
                    print("============= disk table ===============")
                    print(load_table)

                    with open(DISK_PATH + table + ".json", "w") as new_db:
                        for row_key in memtables[table]:
                            if row_key in load_table:
                                for column in memtables[table][row_key]:
                                    for data in memtables[table][row_key][column]:
                                        load_table[row_key][column][data] = memtables[table][row_key][column][data]
                                        if len(load_table[row_key][column]) > 5:
                                            load_table[row_key][column].popitem(last=False)
                            else:
                                load_table[row_key] = memtables[table][row_key]
                                print("modified load table")
                                print(load_table)
                        load_table = dict(sorted(load_table.items(), key = lambda x: x[0]))
                        json.dump(load_table, new_db)
            except:
                with open(DISK_PATH + table + ".json", "w") as db:
                    sorted_table = dict(sorted(memtables[table].items(), key = lambda x : x[0]))
                    json.dump(sorted_table, db)
        # clean memtables
        clean_memtables()

def clean_memtables():
    memtables.clear()
    global num_row_key
    num_row_key = 0

def create_table(input):
    table_name = input["name"]
    if table_name in table_list["tables"]:
        return False
    else:
        table_list["tables"].append(table_name)
        # For show table info
        tables_info[table_name] = input
        tables_index[table_name] = {}
        for column_family in input["column_families"]:
            key = column_family["column_family_key"]
            columns = column_family["columns"]
            tables_index[table_name][key] = columns
        return True


def insert_cell(input, table_name):
    if table_name not in table_list["tables"]:
        return False
    else:
        dict = json.loads(input)
        column_family = dict.get("column_family")
        column = dict.get("column")
        row = dict.get("row")
        data = dict.get("data")
        if column_family in tables_index[table_name]:
            if column in tables_index[table_name][column_family]:
                if table_name not in memtables:
                    memtables[table_name] = {}
                if row not in memtables[table_name]:
                    memtables[table_name][row] = {}
                    global num_row_key
                    num_row_key += 1
                col_index = column_family + ":" + column
                if col_index not in memtables[table_name][row]:
                    memtables[table_name][row][col_index] = collections.OrderedDict()
                    memtables[table_name][row][col_index][data[0]["time"]] = data[0]["value"]
                else:
                    memtables[table_name][row][col_index][data[0]["time"]] = data[0]["value"]
                    if len(memtables[table_name][row][col_index]) > 5:
                        memtables[table_name][row][col_index].popitem(last=False)
                return True


def check_json(input):
    try:
        json.loads(input)
        return True
    except:
        return False


class MyHandler(BaseHTTPRequestHandler):
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
            if not check_json(data):
                self._set_response(400)

            request_path = self.path
            if len(request_path.split("/")) >= 3:
                path_1 = request_path.split("/")[2]
                # Create a table
                if path_1 == 'tables' and len(request_path.split("/")) == 3:
                    flag = create_table(json.loads(data))
                    if not flag:
                        self._set_response(409)
                    else:
                        self._set_response(200)

                # Insert a cell
                elif path_1 == 'tables' and len(request_path.split("/")) > 3:
                    table_name = request_path.split("/")[3]
                    if insert_cell(data, table_name):
                        spill_to_the_disk()
                        self._set_response(200)
                    else:
                        self._set_response(409)
                # Reset memtable size
                elif path_1 == 'memtable':
                    json_value = json.loads(data)
                    try:
                        global tables_max_size
                        new_size = int(json_value["memtable_max"])
                        tables_max_size = new_size
                        self._set_response(200)
                    except:
                        self._set_response(400)
            check_tables()
            print(tables_max_size)
            print(num_row_key)

    def do_DELETE(self):
        # example: send just a 200
        self._set_response(200)


if __name__ == "__main__":
    server_address = ("localhost", 8080)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
