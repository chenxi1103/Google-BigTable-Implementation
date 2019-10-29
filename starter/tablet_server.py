from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import json
import collections
import os
import sys

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
# global num_row_key
num_row_keys = {}

# In-disk database location
DISK_PATH = "disk/"
# Metadata & WAL log location
META_PATH = "META/"
WAL_PATH = "WAL/"


ROW_META_FILE_NAME = "row.meta"
COL_META_FILE_NAME = "col.meta"
WAL_LOG_FILE_NAME = ".log"
MEMTABLE_SIZE_FILE_NAME = "memtable_max_size.meta"

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


def get_disk_json(table_name):
    list = []
    with open(DISK_PATH + table_name + ".table", "r") as db:
        line = db.readline()
        while line:
            list.append(json.loads(line))
            line = db.readline()
    list.reverse()
    return list


def metadata_for_row_index(table_name, row_key):
    with open(META_PATH + ROW_META_FILE_NAME, "a") as meta:
        meta.write(str(table_name) + "*" + str(row_key) + "\n")


def metadata_for_col_index(json_value):
    with open(META_PATH + COL_META_FILE_NAME, "a") as meta:
        meta.write(json.dumps(json_value) + "\n")


def metadata_for_max_size(size):
    f = open(META_PATH + MEMTABLE_SIZE_FILE_NAME, 'r+')
    f.truncate()
    f.write(size)


def recover_from_max_size_meta():
    try:
        with open(META_PATH + MEMTABLE_SIZE_FILE_NAME, 'r') as max_size_meta:
            line = max_size_meta.readline()
            if len(line) > 0:
                global tables_max_size
                new_size = int(line)
                tables_max_size = new_size
                spill_to_the_disk()
    except IOError:
        if not os.path.exists(META_PATH):
            os.mkdir(META_PATH)
        open(META_PATH + MEMTABLE_SIZE_FILE_NAME, 'w').close()


def recover_from_row_meta():
    # Recover tables_rows
    try:
        with open(META_PATH + ROW_META_FILE_NAME, "r") as row_meta:
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
        if not os.path.exists(META_PATH):
            os.mkdir(META_PATH)
        open(META_PATH + ROW_META_FILE_NAME, 'w').close()


def recover_from_col_meta():
    # Recover table_columns and table info
    try:
        with open(META_PATH + COL_META_FILE_NAME, "r") as col_meta:
            line = col_meta.readline()
            while len(line) > 1:
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
        if not os.path.exists(META_PATH):
            os.mkdir(META_PATH)
        open(META_PATH + COL_META_FILE_NAME, 'w').close()


def write_ahead_log(operation, table, content):
    try:
        with open(WAL_PATH + table + WAL_LOG_FILE_NAME, "a") as log:
            log.write(str(operation) + "*" + table + "*" + json.dumps(content) + "\n")
    except IOError:
        with open(WAL_PATH + table + WAL_LOG_FILE_NAME, "w") as log:
            log.write(str(operation) + "*" + table + "*" + json.dumps(content) + "\n")


def spill_to_the_disk():
    for table in num_row_keys:
        if num_row_keys[table] >= tables_max_size:
            try:
                with open(DISK_PATH + table + ".table", "a") as db:
                    SSTable = {}
                    for row_key in memtables[table]:
                        SSTable[row_key] = memtables[table][row_key]
                    SSTable = dict(sorted(SSTable.items(), key=lambda x: x[0]))
                    db.write(json.dumps(SSTable) + "\n")
                    clean_memtables(table)
                    os.remove(WAL_PATH + table + WAL_LOG_FILE_NAME)

            except IOError:
                with open(DISK_PATH + table + ".table", "w") as db:
                    sorted_table = dict(sorted(memtables[table].items(), key=lambda x: x[0]))
                    json.dump(sorted_table, db)


def clean_memtables(table_name):
    del memtables[table_name]
    num_row_keys[table_name] = 0


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

        # For num_row_key
        num_row_keys[table_name] = 0

        # put new column families and columns into column in-memory index
        for column_family in input.get("column_families"):
            key = column_family.get("column_family_key")
            columns = column_family.get("columns")
            tables_columns[table_name][key] = columns
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

    def recover_from_log(self):
        try:
            f_list = os.listdir(WAL_PATH)
            for file in f_list:
                if os.path.splitext(file)[1] == '.log':
                    table = os.path.splitext(file)[0]
                    with open(WAL_PATH + table + WAL_LOG_FILE_NAME, "r") as log:
                        line = log.readline()
                        while line:
                            opertaion = line.split("*")[0]
                            table_name = line.split("*")[1]
                            content = json.loads(line.split("*")[2])
                            # create a table
                            if opertaion == "1":
                                create_table(content)
                            # insert a cell
                            elif opertaion == "2":
                                self.insert_cell(line.split("*")[2], table_name)
                            # reset memtable size
                            elif opertaion == "3":
                                global tables_max_size
                                new_size = int(content.get("memtable_max"))
                                tables_max_size = new_size
                                spill_to_the_disk()
                            line = log.readline()
        except IOError:
            os.makedirs(WAL_PATH)

    def insert_cell(self, input, table_name):
        if table_name not in table_list["tables"]:
            self._set_response(404)
            return
        else:
            if not check_json(input):
                self._set_response(400)
                return
            dict = json.loads(input)
            column_family = dict.get("column_family")
            column = dict.get("column")
            row = dict.get("row")
            data = dict.get("data")
            col_index = column_family + ":" + column
            if column_family in tables_columns[table_name] and column in tables_columns[table_name][column_family]:
                if column_family in tables_columns[table_name]:
                    if column in tables_columns[table_name][column_family]:
                        if table_name not in memtables:
                            memtables[table_name] = {}
                        if row not in memtables[table_name]:
                            memtables[table_name][row] = {}
                            num_row_keys[table_name] += 1
                        if col_index not in memtables[table_name][row]:
                            memtables[table_name][row][col_index] = collections.OrderedDict()
                            memtables[table_name][row][col_index][data[0]["time"]] = data[0]["value"]
                        else:
                            for Time in memtables[table_name][row][col_index]:
                                if memtables[table_name][row][col_index][Time] == data[0]["value"]:
                                    del memtables[table_name][row][col_index][Time]
                            memtables[table_name][row][col_index][data[0]["time"]] = data[0]["value"]
                            if len(memtables[table_name][row][col_index]) > 5:
                                memtables[table_name][row][col_index].popitem(last=False)

                        # put new row into row in-memory index
                        if row not in tables_rows[table_name]:
                            tables_rows[table_name].append(row)
                            # If this is a new row key, write it in metadata
                            metadata_for_row_index(table_name, row)

                        write_ahead_log(2, table_name, dict)
                        spill_to_the_disk()
                        self._set_response(200)
                        return
            else:
                self._set_response(400)

    def find_in_disk(self, table_name, cell_dic, row_value, col_key):
        for file in os.listdir(DISK_PATH):
            if file == table_name + ".table":
                SSTables = get_disk_json(table_name)
                for SSTable in SSTables:
                    if str(row_value) in SSTable:
                        if col_key in SSTable[str(row_value)]:
                            for time, v in SSTable[str(row_value)][col_key].items():
                                child_dic = {"value": v, "time": float(time)}
                                cell_dic["data"].append(child_dic)
                            return cell_dic

    def retrieve_cell(self, table_name):
        content_length = self.headers['content-length']
        cell_dic = collections.defaultdict(list)
        if content_length:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            data_json = json.loads(data)
            row_value = data_json.get("row")
            cell_dic["row"] = row_value
            col_key = str(data_json.get("column_family")) + ":" + str(data_json.get("column"))
            if table_name not in table_list["tables"] or row_value not in tables_rows[table_name]:
                self._set_response(400)
                return
            if data_json.get("column_family") not in tables_columns[table_name] or \
                    data_json.get("column") not in tables_columns[table_name][data_json.get("column_family")]:
                self._set_response(400)
                return
            # must in memtable or disk
            # search in memtables
            cell_dic["data"] = []
            if table_name in memtables:
                # not in memtable, to disk
                if row_value not in memtables[table_name] or col_key not in memtables[table_name][row_value]:
                    cell_dic = self.find_in_disk(table_name, cell_dic, row_value, col_key)
                # go to memtable
                else:
                    # if col_key in memtables[table_name][row_value]:
                    for t, vs in memtables[table_name][row_value][col_key].items():
                        child_dic = {"value": vs, "time": float(t)}
                        cell_dic["data"].append(child_dic)
            else:
                cell_dic = self.find_in_disk(table_name, cell_dic, row_value, col_key)
        data_json = json.dumps(cell_dic)
        self._set_response(200)
        self.wfile.write(data_json.encode("utf8"))

    def retrieve_range(self, table_name):
        content_length = self.headers['content-length']
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
            # if upper_row in memtables[table_name] and lower_row in memtables[table_name]:
            if table_name in memtables:
                for row_name, value in memtables[table_name].items():
                    if lower_row <= row_name <= upper_row:
                        if col_key in memtables[table_name][row_name]:
                            data_dic = collections.defaultdict()
                            data_dic["row"] = row_name
                            data_dic["data"] = []
                            for time, v in memtables[table_name][row_name][col_key].items():
                                child_dic = {"value": v, "time": float(time)}
                                data_dic["data"].append(child_dic)
                            cells_dic["rows"].append(data_dic)
                        else:
                            self._set_response(404)
                            return
            # search disk
            for file in os.listdir(DISK_PATH):
                if file == table_name + ".table":
                    SSTables = get_disk_json(table_name)
                    for SSTable in SSTables:
                        for single_row, _ in SSTable.items():
                            if lower_row <= single_row <= upper_row:
                                if col_key in SSTable[single_row]:
                                    dic_range = collections.defaultdict()
                                    dic_range["row"] = single_row
                                    dic_range["data"] = []
                                    for ti, va in SSTable[single_row][col_key].items():
                                        child_dic = {"value": va, "time": float(ti)}
                                        dic_range["data"].append(child_dic)
                                    cells_dic["rows"].append(dic_range)
                                else:
                                    self._set_response(404)
                                    return

            data_json = json.dumps(cells_dic)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))
            return

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
                if table_name not in table_list["tables"]:
                    self._set_response(404)
                    return
                self.retrieve_cell(table_name)
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'cells':
                # retrieve cells from range
                table_name = url[2]
                self.retrieve_range(table_name)
            elif url[0] == 'api' and url[1] == 'table' and url[-1] == 'row':
                table_name = url[2]
                self.retrieve_a_row(table_name)

    def do_POST(self):
        data = None
        content_length = self.headers['content-length']

        if content_length != None:
            content_length = int(content_length)
            data = str(self.rfile.read(content_length).decode("utf-8"))
            request_path = self.path
            if len(request_path.split("/")) >= 3:
                path_1 = request_path.split("/")[2]
                # Create a table
                if path_1 == 'tables' and len(request_path.split("/")) == 3:
                    if not check_json(data):
                        self._set_response(400)
                        return
                    flag = create_table(json.loads(data))
                    if not flag:
                        self._set_response(409)
                    else:
                        write_ahead_log(1, "default", json.loads(data))
                        self._set_response(200)

                # Insert a cell
                elif path_1 == 'table' and len(request_path.split("/")) > 3:
                    table_name = request_path.split("/")[3]
                    self.insert_cell(data, table_name)

                # Reset memtable size
                elif path_1 == 'memtable':
                    if not check_json(data):
                        self._set_response(400)
                        return
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

    def do_DELETE(self):
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
                    else:
                        table_list["tables"].remove(table_name)
                        del tables_info[table_name]
                        if table_name in tables_columns:
                            del tables_columns[table_name]
                        if table_name in tables_rows:
                            del tables_rows[table_name]

                        if table_name in memtables:
                            # if table in memtable
                            memtables.pop(table_name)

                        for file in os.listdir(DISK_PATH):
                            if file == table_name + ".table":
                                file_path = os.path.join(DISK_PATH, file)
                                os.remove(file_path)

                        write_ahead_log(4, table_name, "")
                        self._set_response(200)
                check_tables()

def join_master(host_name, host_port, master_host_name, master_host_port):
    data = {"host_name" : host_name, "host_port" : host_port}
    url = "http://"+master_host_name+":"+ str(master_host_port) + "/api/join"
    requests.post(url, json=data)

if __name__ == "__main__":
    host_name = sys.argv[1]
    host_port = int(sys.argv[2])
    master_host_name = sys.argv[3]
    master_host_port = int(sys.argv[4])

    server_address = (host_name, host_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    try:
        if not os.path.exists(DISK_PATH):
            os.mkdir(DISK_PATH)
        recover_from_col_meta()
        recover_from_row_meta()
        handler_class.recover_from_log(handler_class)
        recover_from_max_size_meta()
        join_master(host_name, host_port, master_host_name, master_host_port)
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
