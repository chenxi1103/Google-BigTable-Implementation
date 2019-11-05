from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import json
import collections
import os
import sys
import heapq
from shutil import copyfile
from threading import Thread

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

global shard_max_size
shard_max_size = 1000

# In-memory number of row key
# global num_row_key
num_row_keys = {}

# In-disk database location
DISK_PATH = "disk/"
# Metadata & WAL log location
META_PATH = "META/"
WAL_PATH = "WAL/"

# Server Name
global TABLET_SERVER_NAME
TABLET_SERVER_NAME = ""

global MASTER_SERVER_NAME
MASTER_SERVER_NAME = ""

ROW_META_FILE_NAME = "row.meta"
COL_META_FILE_NAME = "col.meta"
WAL_LOG_FILE_NAME = ".log"
MEMTABLE_SIZE_FILE_NAME = "memtable_max_size.meta"


############################### Shard Logic Method #################################
def check_shard_request():
    url = "http://" + MASTER_SERVER_NAME + "/api/shard_request"
    for table_name in tables_rows:
        if len(tables_rows[table_name]) >= shard_max_size:
            rowkeys = sorted(tables_rows[table_name])
            shard_row = rowkeys[:shard_max_size // 2 + 1]
            print(shard_row)
            original_row = rowkeys[shard_max_size // 2:]
            jsonvalue = {"tablet": TABLET_SERVER_NAME, "table": table_name, "shard_row": shard_row,
                         "original_row": original_row}
            requests.post(url, json.dumps(jsonvalue))


def update_shard_request(table_name, shard_row_key_set, tablet_server_name):
    """
    This method is used to notify the shard server to update:
    1. tables_rows
    2. tables_columns
    3. tables_info
    4. table_list
    :param table_name: shard table's name
    :param shard_row_key_set: shard table's row_key list (sorted)
    :param tablet_server_name: shard tablet's server name (hostname:port)
    :return: void
    """
    table_rows = {table_name: shard_row_key_set}
    table_columns = {table_name: tables_columns[table_name]}
    table_info = {table_name: tables_info[table_name]}
    jsonvalue = {"table": table_name, "table_rows": table_rows, "table_columns": table_columns,
                 "table_info": table_info}
    url = "http://" + tablet_server_name + "/api/update_shard"
    requests.post(url, json.dumps(jsonvalue))


def update_shard_server(jsonvalue):
    """
    If current server is the shard server, the method would parse input json to update the corresponding:
    1. tables_rows
    2. tables_columns
    3. tables_info
    4. table_list
    :param jsonvalue: a json format input data contains information of tables_rows/tables_columns/tables_info/table_list
    :return: void
    """
    table_name = jsonvalue["table"]
    table_rows = jsonvalue["table_rows"]
    table_columns = jsonvalue["table_columns"]
    table_info = jsonvalue["table_info"]
    # update table_list
    if table_name not in table_list["tables"]:
        table_list["tables"].append(table_name)
    # update table rows
    tables_rows[table_name] = []
    for row_key in table_rows[table_name]:
        metadata_for_row_index(table_name, row_key)
        tables_rows[table_name].append(row_key)
    # update table columns
    tables_columns[table_name] = table_columns[table_name]
    columns_json = {"name": table_name, "column_families": []}
    for column in tables_columns[table_name]:
        jsonitem = {"column_family_key": column, "columns": []}
        for column_key in tables_columns[table_name][column]:
            jsonitem["columns"].append(column_key)
        columns_json["column_families"].append(jsonitem)
    metadata_for_col_index(columns_json)
    # update table info
    tables_info[table_name] = table_info[table_name]


def shard_finish_request(table_name, tablet_server_name, row_from_pre, row_to_pre, row_from_post, row_to_post):
    """
    After finishing sharding job, notify Master server to update "tables_info"
    :param table_name: sharding table's name
    :param tablet_server_name: shard tablet server name
    :param row_from_pre: current tablet server's table row_from
    :param row_to_pre: current tablet server's table row_to
    :param row_from_post: shard tablet server's table row_from
    :param row_to_post: shard tablet server's table row_to
    :return: void
    """
    url = "http://" + MASTER_SERVER_NAME + "/api/shard_finish"
    jsonvalue = {"table": table_name,
                 "data": [{"hostname": TABLET_SERVER_NAME.split(":")[0], "port": TABLET_SERVER_NAME.split(":")[1],
                           "row_from": row_from_pre, "row_to": row_to_pre},
                          {"hostname": tablet_server_name.split(":")[0],
                           "port": tablet_server_name.split(":")[1], "row_from": row_from_post, "row_to": row_to_post}]}
    requests.post(url, json.dumps(jsonvalue))


def shard_to_other_tablet(data):
    """
    Shard data to another tablet server
    :param data: Get input from Master server. Input data contains the shard server's server name
    :return: void
    """
    json_value = json.loads(data)
    tablet_server_name = json_value["tablet"]
    table_name = json_value["table"]
    shard_row = json_value["shard_row"]
    original_row = json_value["original_row"]
    pre_list = []
    post_list = []
    origin_row_key_set = set()

    # Count memtable's row_key first
    if table_name in memtables:
        for row_key in memtables[table_name]:
            origin_row_key_set.add(row_key)

    with open(TABLET_SERVER_NAME + "/" + DISK_PATH + table_name + ".table", "r") as db:
        line = db.readline()
        while line:
            jsonitem = json.loads(line)
            pre_dict = {}
            post_dict = {}
            for row_key in jsonitem:
                if row_key in original_row:
                    pre_dict[row_key] = jsonitem[row_key]
                    origin_row_key_set.add(row_key)
                if row_key in shard_row:
                    post_dict[row_key] = jsonitem[row_key]
            if len(pre_dict) > 0:
                pre_list.append(json.dumps(pre_dict))
            if len(post_dict) > 0:
                post_list.append(json.dumps(post_dict))
            line = db.readline()

    with open(TABLET_SERVER_NAME + "/" + DISK_PATH + table_name + ".table", "w") as this_db:
        for data in pre_list:
            this_db.write(data + "\n")

    with open(tablet_server_name + "/" + DISK_PATH + table_name + ".table", "w") as shard_db:
        for data in post_list:
            print(data)
            shard_db.write(data + "\n")

    origin_row_key_set = sorted(list(origin_row_key_set))

    # update table rows
    tables_rows[table_name] = origin_row_key_set

    # update row meta
    metadata_for_row_index_shard()

    # update shard server
    update_shard_request(table_name, shard_row, tablet_server_name)

    # update master server
    shard_finish_request(table_name, tablet_server_name, origin_row_key_set[0],
                         origin_row_key_set[len(origin_row_key_set) - 1],
                         shard_row[0], shard_row[len(shard_row) - 1])


############################### Recover Logic Method / Log / MetaData #################################
def recover_disk(dead_tablet, take_tablet):
    """
    Recover disk data from dead tablet to take_over tablet
    :param dead_tablet: dead tablet server name
    :param take_tablet: take_over tablet server name
    :return: void
    """
    f_list = os.listdir(dead_tablet + "/" + DISK_PATH)
    for file in f_list:
        if os.path.exists(dead_tablet + "/" + DISK_PATH + file):
           try:
             copyfile(dead_tablet + "/" + DISK_PATH + file, take_tablet + "/" + DISK_PATH + file)
             os.remove(dead_tablet + "/" + DISK_PATH + file)
           except:
             pass

def metadata_for_row_index(table_name, row_key):
    """
    Log metadata for adding new row key
    :param table_name: table name
    :param row_key: row key
    :return: void
    """
    with open(TABLET_SERVER_NAME + "/" + META_PATH + ROW_META_FILE_NAME, "a") as meta:
        meta.write(str(table_name) + "*" + str(row_key) + "\n")


def metadata_for_row_index_shard():
    """
    Log metadata for new row keys set after sharding
    :return: void
    """
    with open(TABLET_SERVER_NAME + "/" + META_PATH + ROW_META_FILE_NAME, "w") as meta:
        for table in tables_rows:
            for row_key in tables_rows[table]:
                meta.write(str(table) + "*" + str(row_key) + "\n")


def metadata_for_col_index(json_value):
    """
    Log metadata for adding new col index
    :param json_value: column family/ column keys info
    :return: void
    """
    with open(TABLET_SERVER_NAME + "/" + META_PATH + COL_META_FILE_NAME, "a") as meta:
        meta.write(json.dumps(json_value) + "\n")


def metadata_for_max_size(size):
    """
    Log metadata for changing max memtable size
    :param size: max memtable size
    :return: void
    """
    f = open(TABLET_SERVER_NAME + "/" + META_PATH + MEMTABLE_SIZE_FILE_NAME, 'r+')
    f.truncate()
    f.write(size)


def recover_from_max_size_meta(tablet_server_name):
    # Recover max memtable size
    try:
        with open(tablet_server_name + "/" + META_PATH + MEMTABLE_SIZE_FILE_NAME, 'r') as max_size_meta:
            line = max_size_meta.readline()
            if len(line) > 0:
                global tables_max_size
                new_size = int(line)
                tables_max_size = new_size
                spill_to_the_disk()
    except IOError:
        if not os.path.exists(tablet_server_name + "/" + META_PATH):
            os.mkdir(tablet_server_name + "/" + META_PATH)
        open(tablet_server_name + "/" + META_PATH + MEMTABLE_SIZE_FILE_NAME, 'w').close()


def recover_from_row_meta(tablet_server_name):
    # Recover tables_rows
    try:
        with open(tablet_server_name + "/" + META_PATH + ROW_META_FILE_NAME, "r") as row_meta:
            line = row_meta.readline()
            while line:
                table_name = line.split("*")[0]
                row_key = line.split("*")[1].replace("\n", "")
                if table_name not in table_list["tables"]:
                    table_list["tables"].append(table_name)
                if table_name not in tables_rows:
                    tables_rows[table_name] = []
                if row_key not in tables_rows[table_name]:
                    heapq.heappush(tables_rows[table_name], row_key)
                    # tables_rows[table_name].append(row_key)
                line = row_meta.readline()
        update_row_key_to_master()

    except IOError:
        if not os.path.exists(tablet_server_name + "/" + META_PATH):
            os.mkdir(tablet_server_name + "/" + META_PATH)
        open(tablet_server_name + "/" + META_PATH + ROW_META_FILE_NAME, 'w').close()


def recover_from_col_meta(tablet_server_name):
    # Recover table_columns and table info
    try:
        with open(tablet_server_name + "/" + META_PATH + COL_META_FILE_NAME, "r") as col_meta:
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
        if not os.path.exists(tablet_server_name + "/" + META_PATH):
            os.mkdir(tablet_server_name + "/" + META_PATH)
        open(tablet_server_name + "/" + META_PATH + COL_META_FILE_NAME, 'w').close()


def write_ahead_log(operation, table, content):
    """
    Write ahead log mechanism
    :param operation: 1 for "create table", 2 for "insert a cell", 3 for "set max memtable size", 4 for "destroy table"
    :param table: table name
    :param content: json information for that operation
    :return: void
    """
    try:
        with open(TABLET_SERVER_NAME + "/" + WAL_PATH + table + WAL_LOG_FILE_NAME, "a") as log:
            log.write(str(operation) + "*" + table + "*" + json.dumps(content) + "\n")
    except IOError:
        with open(TABLET_SERVER_NAME + "/" + WAL_PATH + table + WAL_LOG_FILE_NAME, "w") as log:
            log.write(str(operation) + "*" + table + "*" + json.dumps(content) + "\n")


############################### Spill to disk Logic Method #################################
def spill_to_the_disk():
    """
    Check if any table's #row_key exceeds max memtable size. If so, spill them to disk and clean
    the corresponding memtable
    :return: void
    """
    for table in num_row_keys:
        if num_row_keys[table] >= tables_max_size:
            try:
                with open(TABLET_SERVER_NAME + "/" + DISK_PATH + table + ".table", "a") as db:
                    SSTable = {}
                    for row_key in memtables[table]:
                        SSTable[row_key] = memtables[table][row_key]
                    SSTable = dict(sorted(SSTable.items(), key=lambda x: x[0]))
                    db.write(json.dumps(SSTable) + "\n")
                    clean_memtables(table)
                    os.remove(TABLET_SERVER_NAME + "/" + WAL_PATH + table + WAL_LOG_FILE_NAME)

            except IOError:
                with open(TABLET_SERVER_NAME + "/" + DISK_PATH + table + ".table", "w") as db:
                    sorted_table = dict(sorted(memtables[table].items(), key=lambda x: x[0]))
                    json.dump(sorted_table, db)


def clean_memtables(table_name):
    # clean the corresponding memtable after spilling
    del memtables[table_name]
    num_row_keys[table_name] = 0


############################### Other Methods #################################
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
        # num_row_keys_shard[table_name] = set()

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


def update_row_key_to_master():
    """
    Everytime the row_key from and to changes, notify the Master server to update
    :return: void
    """
    url = "http://" + MASTER_SERVER_NAME + "/api/update_rowkey"
    jsonvalue = {"tablet": TABLET_SERVER_NAME, "data": tables_rows}
    requests.post(url, json.dumps(jsonvalue))


def get_disk_json(table_name):
    list = []
    with open(TABLET_SERVER_NAME + "/" + DISK_PATH + table_name + ".table", "r") as db:
        line = db.readline()
        while line:
            list.append(json.loads(line))
            line = db.readline()
    list.reverse()
    return list


def insert_cell(input, table_name, recover):
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
                    if table_name not in num_row_keys:
                        num_row_keys[table_name] = 0
                    num_row_keys[table_name] += 1
                    # tell master server row key is updated
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
                    heapq.heappush(tables_rows[table_name], row)
                    # tables_rows[table_name].append(row)
                    # If this is a new row key, write it in metadata
                    metadata_for_row_index(table_name, row)
                    update_row_key_to_master()
                    check_shard_request()

                if not recover:
                    write_ahead_log(2, table_name, dict)

                spill_to_the_disk()
                return True
    else:
        return False


def recover_from_log(tablet_server_name):
    """
    Recover the server state by reading write_ahead_log
    :param tablet_server_name: tablet server's name
    :return: void
    """
    try:
        f_list = os.listdir(tablet_server_name + "/" + WAL_PATH)
        for file in f_list:
            if os.path.splitext(file)[1] == '.log':
                table = os.path.splitext(file)[0]
                with open(tablet_server_name + "/" + WAL_PATH + table + WAL_LOG_FILE_NAME, "r") as log:
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
                            insert_cell(line.split("*")[2], table_name, True)
                        # reset memtable size
                        elif opertaion == "3":
                            global tables_max_size
                            new_size = int(content.get("memtable_max"))
                            tables_max_size = new_size
                            spill_to_the_disk()
                        line = log.readline()
    except IOError:
        os.makedirs(tablet_server_name + "/" + WAL_PATH)


class MyHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def find_in_disk(self, table_name, cell_dic, row_value, col_key):
        for file in os.listdir(TABLET_SERVER_NAME + "/" + DISK_PATH):
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
            for file in os.listdir(TABLET_SERVER_NAME + "/" + DISK_PATH):
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

    def recover_from_other(self, dead_tablet):
        recover_from_col_meta(dead_tablet)
        with open(dead_tablet + "/" + META_PATH + COL_META_FILE_NAME, 'r') as col_file:
            line = col_file.readline()
            while line:
                line = json.loads(line)
                metadata_for_col_index(line)
                line = col_file.readline()
        open(dead_tablet + "/" + META_PATH + COL_META_FILE_NAME, 'w').close()


        recover_from_row_meta(dead_tablet)
        row_list = []
        with open(dead_tablet + "/" + META_PATH + ROW_META_FILE_NAME, 'r') as row_file:
            line = row_file.readline()
            while line:
                row_list.append(line)
                line = row_file.readline()

        with open(TABLET_SERVER_NAME + "/" + META_PATH + ROW_META_FILE_NAME, 'a') as cur_row_file:
            for line in row_list:
                cur_row_file.write(line)

        open(dead_tablet + "/" + META_PATH + ROW_META_FILE_NAME, 'w').close()

        recover_from_max_size_meta(dead_tablet)
        open(dead_tablet + "/" + META_PATH + MEMTABLE_SIZE_FILE_NAME, 'w').close()

        recover_disk(dead_tablet, TABLET_SERVER_NAME)
        recover_from_log(dead_tablet)
        for file in os.listdir(dead_tablet + "/" + WAL_PATH):
            copyfile(dead_tablet + "/" + WAL_PATH + "/" + file, TABLET_SERVER_NAME + "/" + WAL_PATH + "/" + file)
            file_path = os.path.join(dead_tablet + "/" + WAL_PATH, file)
            os.remove(file_path)

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
            elif url[0] == 'api' and url[1] == 'heart':
                self._set_response(200)
                return
            else:
                self._set_response(404)
                return

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
                        json_val = json.loads(data)
                        write_ahead_log(1, json_val["name"], json_val)
                        self._set_response(200)

                # Insert a cell
                elif path_1 == 'table' and len(request_path.split("/")) > 3:
                    table_name = request_path.split("/")[3]

                    if table_name not in table_list["tables"]:
                        self._set_response(404)
                        return
                    if not check_json(data):
                        self._set_response(400)
                        return
                    flag = insert_cell(data, table_name, False)
                    if flag:
                        self._set_response(200)
                        return
                    else:
                        self._set_response(400)
                        return

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
                        return
                    except:
                        self._set_response(400)
                        return

                # update shard
                elif path_1 == 'update_shard':
                    json_value = json.loads(data)
                    update_shard_server(json_value)
                    self._set_response(200)
                    return

                # allocate shard server
                elif path_1 == 'tablet':
                    self._set_response(200)
                    t = Thread(target=shard_to_other_tablet, args=(data,))
                    t.start()
                    # shard_to_other_tablet(json_value["tablet"], json_value["table"])
                    return
                elif path_1 == 'read' and len(request_path.split("/")) > 3:
                    dead_tablet = request_path.split("/")[3]
                    self.recover_from_other(dead_tablet)
                    self._set_response(200)

    def do_DELETE(self):
        url = self.path.split('/')[1:]
        print(url)
        if len(url) >= 2:
            if url[1] == "tables":
                table_name = url[2]
                print(table_name)
                if table_name not in table_list["tables"]:
                    # table not exist in table list
                    self._set_response(404)
                    return
                else:
                    print("delete")
                    delete_table(table_name)
                    self._set_response(200)


def join_master(host_name, host_port, master_host_name, master_host_port):
    data = {"host_name": host_name, "host_port": host_port}
    url = "http://" + master_host_name + ":" + str(master_host_port) + "/api/join"
    requests.post(url, json=data)


def delete_table(table_name):
    print(1)
    table_list["tables"].remove(table_name)
    del tables_info[table_name]
    if table_name in tables_columns:
        del tables_columns[table_name]
    if table_name in tables_rows:
        del tables_rows[table_name]

    if table_name in memtables:
        # if table in memtable
        memtables.pop(table_name)

    for file in os.listdir(TABLET_SERVER_NAME + "/" + DISK_PATH):
        if file == table_name + ".table":
            file_path = os.path.join(TABLET_SERVER_NAME + "/" + DISK_PATH, file)
            os.remove(file_path)

    delete_meta(table_name)
    delete_wal(table_name)


def delete_meta(tablename):
    print(2)
    # Recover tables_rows
    row_metadata = []
    with open(TABLET_SERVER_NAME + "/" + META_PATH + ROW_META_FILE_NAME, "r") as row_meta:
        line = row_meta.readline()
        while line:
            table_name = line.split("*")[0]
            if table_name != tablename:
                row_metadata.append(line)
            line = row_meta.readline()

    with open(TABLET_SERVER_NAME + "/" + META_PATH + ROW_META_FILE_NAME, "w") as row_meta_w:
        for line in row_metadata:
            row_meta_w.write(line)

    col_metadata = []
    with open(TABLET_SERVER_NAME + "/" + META_PATH + COL_META_FILE_NAME, "r") as col_meta:
        line = col_meta.readline()
        while len(line) > 1:
            json_val = json.loads(line)
            print(json_val)
            table_name = json_val["name"]
            if table_name != tablename:
                col_metadata.append(line)
            line = col_meta.readline()

    with open(TABLET_SERVER_NAME + "/" + META_PATH + COL_META_FILE_NAME, "w") as col_meta_w:
        for line in col_metadata:
            col_meta_w.write(line)


def delete_wal(tablename):
    print(3)
    if os.path.exists(TABLET_SERVER_NAME + "/" + WAL_PATH + tablename + WAL_LOG_FILE_NAME):
        os.remove(TABLET_SERVER_NAME + "/" + WAL_PATH + tablename + WAL_LOG_FILE_NAME)


if __name__ == "__main__":
    host_name = sys.argv[1]
    host_port = int(sys.argv[2])
    master_host_name = sys.argv[3]
    master_host_port = int(sys.argv[4])

    TABLET_SERVER_NAME = host_name + ":" + str(host_port)
    MASTER_SERVER_NAME = master_host_name + ":" + str(master_host_port)

    server_address = (host_name, host_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    try:
        if not os.path.exists(TABLET_SERVER_NAME):
            os.mkdir(TABLET_SERVER_NAME)
        if not os.path.exists(TABLET_SERVER_NAME + "/" + DISK_PATH):
            os.mkdir(TABLET_SERVER_NAME + "/" + DISK_PATH)
        recover_from_col_meta(TABLET_SERVER_NAME)
        recover_from_row_meta(TABLET_SERVER_NAME)
        recover_from_log(TABLET_SERVER_NAME)
        recover_from_max_size_meta(TABLET_SERVER_NAME)
        join_master(host_name, host_port, master_host_name, master_host_port)
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
