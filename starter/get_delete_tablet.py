from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import collections
from collections import OrderedDict
import os

memtables = {"table_kill": {'sample_a': {'fam1:key1': OrderedDict([(12350, '6'), (12351, '7'), (12352, '8')])},
                            'sample_b': {'fam1:key1': OrderedDict([(12350, '6'), (12351, '7'), (12352, '8')])},
                            'sample_c': {'fam1:key1': OrderedDict([(12350, '6'), (12351, '7'), (12352, '8')])}}}
# sstable = {}
# get_table = ['List Table', 'Get Table Info', 'Retrieve a cell', 'Retrieve cells']
# post_table = ['Create Table', 'Insert a cell']
# delete_table = ['Destroy Table']
table_list = {"tables": ["table_kill"]}
tables_info = {}
tables_columns = {}
tables_rows = {}


class MyHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def retrieve_cell(self, table_name):
        data = None
        content_length = self.headers['content-length']
        cell_dic = collections.defaultdict(list)
        value_dic = {}
        if content_length:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            # print(data)
            data_json = json.loads(data)
            row_value = data_json.get("row")
            cell_dic["row"] = row_value
            col_key = str(data_json.get("column_family")) + ":" + str(data_json.get("column"))
            if row_value in memtables[table_name]:
                if col_key in memtables[table_name][row_value]:
                    value_dic["values"] = next(reversed(memtables.get(table_name).get(row_value).get(col_key).values()))
                    value_dic["time"] = next(reversed(memtables.get(table_name).get(row_value).get(col_key)))
            # Come to disk to find
            else:
                for file in os.listdir("disk/"):
                    if file == table_name + ".json":
                        file_path = os.path.join("disk/", file)
                        with open(file_path, 'r') as rf:
                            disk_dic = json.loads(rf.read())
                            # print(disk_dic)
                            if row_value in disk_dic:
                                if col_key in disk_dic[row_value]:
                                    entry = disk_dic.get(row_value).get(col_key)
                                    (key, value), = entry.items()
                                    value_dic["values"] = value
                                    value_dic["times"] = key
                                else:
                                    self._set_response(400)
                                    return
                            else:
                                # bad request
                                self._set_response(400)
                                return
                    else:
                        self._set_response(404)
                        return
                # 400 bad request column family not in disk and memtable
            cell_dic["data"].append(value_dic)
            data_json = json.dumps(cell_dic)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))
            return

    def retrieve_range(self, table_name):
        content_length = self.headers['content-length']
        cell_dic = collections.defaultdict(list)
        value_dic = {}
        if content_length:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            data_json = json.loads(data)
            col_key = str(data_json.get("column_family")) + ":" + str(data_json.get("column"))
            lower_row = data_json.get("row_from")
            upper_row = data_json.get("row_to")
            # search memtable and all rows exist
            cells_dic = {}
            cells_dic["rows"] = []
            if upper_row in memtables[table_name] and lower_row in memtables[table_name]:
                for row_name, value in memtables[table_name].items():
                    if lower_row <= row_name <= upper_row:
                        if col_key in memtables[table_name][row_name]:
                            # value_dic = {}
                            data_dic = collections.defaultdict()
                            data_dic["row"] = row_name
                            data_dic["data"] = []
                            for time, v in memtables[table_name][row_name][col_key].items():
                                child_dic = {"value": v, "time":time}
                                data_dic["data"].append(child_dic)
                                print(data_dic)
                            cells_dic["rows"].append(data_dic)
                        else:
                            self._set_response(404)
                            return
            else:
                self._set_response(404)
                return
            print(cells_dic)
            data_json = json.dumps(cells_dic)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))
            return

    def retrieve_a_row(self, table_name):
        content_length = self.headers['content-length']
        if content_length:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            data_json = json.loads(data)
            row_name = data_json.get("row")






    def do_GET(self):
        # example: this is how you get path and command
        print(self.path)
        print(self.command)
        if self.command == 'GET':
            url = self.path.split('/')[1:]
            if url[0] == 'api' and url[1] == 'tables' and len(url) <= 4:
                # List Tables
                if len(url) == 2:
                    data = table_list
                    data_json = json.dumps(data)
                    self._set_response(200)
                    self.wfile.write(data_json.encode("utf8"))
                # Get Table info
                elif len(url) == 3:
                    table_name = url[2]
                    if table_name in tables_info:
                        data = tables_info.get(table_name)
                        data_json = json.dumps(data)
                        self._set_response(200)
                        self.wfile.write(data_json.encode("utf8"))
                else:
                    self._set_response(404)
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'cell':
                # Retrieve a cell
                table_name = url[2]
                # print(table_name)
                self.retrieve_cell(table_name)
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'cells':
                # retrieve cells from range
                table_name = url[2]
                self.retrieve_range(table_name)
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'row':
                table_name = url[2]
                self.retrieve_a_row(table_name)


    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']

        if content_length != None:
            content_length = int(content_length)
            data = self.rfile.read(content_length)

            # print the content, just for you to see it =)
            print(data)

        self._set_response(200)

    def do_DELETE(self):
        # example: send just a 200
        # self._set_response(200)
        content_length = self.headers['content-length']
        if content_length:
            content_length = int(content_length)
            data = self.rfile.read(content_length)
            url = self.path.split('/')[1:]
            if len(url) >= 2:
                if url[1] == "tables":
                    table_name = url[2]
                    if table_name not in table_list["tables"]:
                        # table not exist in table list
                        self._set_response(404)
                        return
                    elif table_name in memtables:
                        # if table in memtable
                        print("before")
                        print(memtables)
                        removed_dic = memtables.pop(table_name)
                        # delete table_columns
                        # remove table from table lists
                        table_list["tables"].remove(table_name)
                        if table_name in tables_columns:
                            del tables_columns[table_name]
                        if table_name in tables_rows:
                            del tables_rows[table_name]
                        self._set_response(200)
                    else:
                        # table in disk
                        table_list["tables"].remove(table_name)
                        for file in os.listdir("disk/"):
                            if file == table_name + ".json":
                                file_path = os.path.join("disk/", file)
                                os.remove(file_path)
                        self._set_response(200)

if __name__ == "__main__":
    server_address = ("localhost", 8083)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()

