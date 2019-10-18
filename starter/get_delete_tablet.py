from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import collections
from collections import OrderedDict

mem_table = {'table_kill': {'sample_a': {'fam1:key1': OrderedDict([(12350, '6'), (12351, '7'), (12352, '8')])}}}
# sstable = {}
# get_table = ['List Table', 'Get Table Info', 'Retrieve a cell', 'Retrieve cells']
# post_table = ['Create Table', 'Insert a cell']
# delete_table = ['Destroy Table']
table_list = {"tables": []}
tables_info = {}


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
            data_json = json.loads(data)
            row_value = data_json.get("row")
            cell_dic["row"] = row_value
            col_key = str(data_json.get("column_family")) + ":" + str(data_json.get("column"))
            if col_key in mem_table[table_name][row_value]:
                value_dic["values"] = next(reversed(mem_table.get(table_name).get(row_value).get(col_key).values()))
                value_dic["time"] = next(reversed(mem_table.get(table_name).get(row_value).get(col_key)))
            # TODO search from disk
            else:
                # 400 bad request column family not in disk and memtable
                self._set_response(400)
            cell_dic["data"].append(value_dic)
            # print(cell_dic)
            data_json = json.dumps(cell_dic)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))
        else:

            self._set_response(200)
            # self.wfile.write(data_json.encode("utf8"))

    def retrieve_range(self, table_name):
        # TODO retrieve cells from mem table and disk
        self._set_response(200)

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
                    table_name = url[2].split(':')[-1]
                    if table_name in tables_info:
                        data = tables_info.get(table_name)
                        data_json = json.dumps(data)
                        self._set_response(200)
                        self.wfile.write(data_json.encode("utf8"))
                else:
                    self._set_response(404)
            elif url[0] == 'api' and url[1] == 'table':
                # Retrieve a cell
                table_name = url[2]
                print(table_name)
                self.retrieve_cell(table_name)

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
        self._set_response(200)


if __name__ == "__main__":
    server_address = ("localhost", 8081)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()

