#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Chenxi Li on 2019-10-17
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import collections

# In-memory Memtable
memtables = {}

# In-memory index
tables_columns = {}
tables_rows = {}

# Table info
tables_info = {}
table_list = {"tables": []}

# Memtable Max size
global tables_max_size
tables_max_size = 100

# In-memory number of row key
global num_row_key
num_row_key = 0

# In-disk database location
DISK_PATH = "disk/"
# Metadata & WAL log location
META_WAL_PATH = "META_WAL/"

def metadata_for_row_index(table_name, row_key):
    with open(META_WAL_PATH + "row.meta", "a") as meta:
        meta.write(table_name + "*" + row_key + "\n")

def metadata_for_col_index(json_value):
    with open(META_WAL_PATH + "col.meta", "a") as meta:
        print(json_value)
        meta.write(json.dumps(json_value) + "\n")

def metadata_for_max_size(size):
    f = open(META_WAL_PATH + "memtable_max_size.meta", 'r+')
    f.truncate()
    f.write(size)

def recover_from_max_size_meta():
    try:
        with open(META_WAL_PATH + "memtable_max_size.meta", 'r') as max_size_meta:
            line = max_size_meta.readline()
            if len(line) > 0:
                global tables_max_size
                new_size = int(line)
                tables_max_size = new_size
                spill_to_the_disk()
    except IOError:
        open(META_WAL_PATH + "memtable_max_size.meta", 'w').close()


def recover_from_row_meta():
    # Recover tables_rows
    try:
        with open(META_WAL_PATH + "row.meta", "r") as row_meta:
            line = row_meta.readline()
            while line:
                table_name = line.split("*")[0]
                row_key = line.split("*")[1].replace("\n", "")
                if table_name not in table_list["tables"]:
                    table_list["tables"].append(table_name)
                if table_name not in tables_rows:
                    tables_rows[table_name] = []
                if row_key not in tables_rows[table_name]:
                    tables_rows[table_name].append(row_key)
                line = row_meta.readline()
    except IOError:
        open(META_WAL_PATH + "row.meta", 'w').close()

def recover_from_col_meta():
    # Recover table_columns and table info
    try:
        with open(META_WAL_PATH + "col.meta", "r") as col_meta:
            line = col_meta.readline()
            while line:
                json_value = json.loads(line)
                table_name = json_value.get("name")
                if table_name not in tables_columns:
                    tables_columns[table_name] = {}
                    for column_family in json_value.get("column_families"):
                        key = column_family.get("column_family_key")
                        columns = column_family.get("columns")
                        tables_columns[table_name][key] = columns
                if table_name not in tables_info:
                    tables_info[table_name] = json_value
                line = col_meta.readline()
    except IOError:
        open(META_WAL_PATH + "col.meta", 'w').close()


def write_ahead_log(operation, table, content):
    with open(META_WAL_PATH + "wal.log", "a") as log:
        log.write(str(operation) + "*" + table + "*" + json.dumps(content) + "\n")

def recover_from_log():
    try:
        with open(META_WAL_PATH + "wal.log", "r") as log:
            line = log.readline()
            while line:
                opertaion = line.split("*")[0]
                table_name = line.split("*")[1]
                content = json.loads(line.split("*")[2])
                print(content)

                # create a table
                if opertaion == "1":
                    create_table(content)
                # insert a cell
                elif opertaion == "2":
                    insert_cell(line.split("*")[2], table_name)
                # reset memtable size
                elif opertaion == "3":
                    global tables_max_size
                    new_size = int(content.get("memtable_max"))
                    tables_max_size = new_size
                    spill_to_the_disk()
                line = log.readline()
    except IOError:
        open(META_WAL_PATH + "wal.log", 'w').close()


def check_tables():
    print("============= Table Rows ===============")
    print(tables_rows)
    print("============= Table Columns ===============")
    print(tables_columns)
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
                        load_table = dict(sorted(load_table.items(), key = lambda x: x[0]))
                        json.dump(load_table, new_db)
            except:
                with open(DISK_PATH + table + ".json", "w") as db:
                    sorted_table = dict(sorted(memtables[table].items(), key = lambda x : x[0]))
                    json.dump(sorted_table, db)
        # clean memtables
        clean_memtables()
        # clean wal_log
        f = open(META_WAL_PATH + "wal.log", 'r+')
        f.truncate()

def clean_memtables():
    memtables.clear()
    global num_row_key
    num_row_key = 0

def create_table(input):
    table_name = input.get("name")
    if table_name in table_list.get("tables"):
        return False
    else:
        metadata_for_col_index(input)
        table_list["tables"].append(table_name)
        # For show table info
        tables_info[table_name] = input
        tables_columns[table_name] = {}
        tables_rows[table_name] = []
        # put new column families and columns into column in-memory index
        for column_family in input.get("column_families"):
            key = column_family.get("column_family_key")
            columns = column_family.get("columns")
            tables_columns[table_name][key] = columns
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
        print(column_family, column, row, data)
        if column_family in tables_columns[table_name]:
            if column in tables_columns[table_name][column_family]:
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

                # put new row into row in-memory index
                if row not in tables_rows[table_name]:
                    tables_rows[table_name].append(row)
                    # If this is a new row key, write it in metadata
                    metadata_for_row_index(table_name, row)
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
                        write_ahead_log(1, "default", json.loads(data))
                        self._set_response(200)

                # Insert a cell
                elif path_1 == 'tables' and len(request_path.split("/")) > 3:
                    table_name = request_path.split("/")[3]
                    if insert_cell(data, table_name):
                        spill_to_the_disk()
                        write_ahead_log(2, table_name, json.loads(data))
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
                        write_ahead_log(3, "default", json_value)
                        spill_to_the_disk()
                        metadata_for_max_size(str(new_size))
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
        recover_from_col_meta()
        recover_from_row_meta()
        recover_from_log()
        recover_from_max_size_meta()
        check_tables()
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
