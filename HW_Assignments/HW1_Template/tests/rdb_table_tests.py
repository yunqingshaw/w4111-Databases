from src.RDBDataTable import RDBDataTable
import logging
import os
import pandas as pd

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
# data_dir = os.path.abspath("../Data/Baseball")

def t_load(table_name, key_columns):
    connect_info = {
        "host": 'localhost',
        "user": 'dbuser',
        "password": 'dbuserdbuser',
        "db": 'lahman2019raw',
        "charset": 'utf8mb4'
    }
    rdb_tb1 = RDBDataTable(table_name, connect_info, key_columns)
    print("Created table = \n" + str(pd.DataFrame(rdb_tb1.get_rows())))
    return rdb_tb1


def t_find_by_primary_key(table, t_name, key_fields, field_list=None):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Key fields are:" + str(key_fields) + "\n"
    s += "Field list is: " + str(field_list) + "\n"
    try:
        result = table.find_by_primary_key(key_fields, field_list)
        s += "The query result is: " + str(result) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)


def t_find_by_template(table, t_name, template, field_list=None):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Template is : " + str(template) + "\n"
    try:
        result = table.find_by_template(template, field_list)
        s += "The query result is: \n" + str(pd.DataFrame(result)) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)


def t_delete_by_key(table, t_name, key_fields):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Key fields are: " + str(key_fields) + "\n"
    try:
        s += "Table before delete: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
        result = table.delete_by_key(key_fields)
        s += str(result) + " rows are deleted. Table after delete: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)

def t_delete_by_template(table, t_name, template):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Template is : " + str(template) + "\n"
    try:
        s += "Table before delete: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
        result = table.delete_by_template(template)
        s += str(result) + " rows are deleted. Table after delete: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)

def t_update_by_key(table, t_name, key_fields, new_values):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Key fields are: " + str(key_fields) + "\n"
    s += "New values are: " + str(new_values) + "\n"
    try:
        s += "Table before update: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
        result = table.update_by_key(key_fields, new_values)
        s += str(result) + " rows are updated. Table after update: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)

def t_update_by_template(table, t_name, template, new_values):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Template is : " + str(template) + "\n"
    s += "New values are: " + str(new_values) + "\n"
    try:
        s += "Table before update: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
        result = table.update_by_template(template, new_values)
        s += str(result) + " rows are updated. Table after update: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)

def t_insert(table, t_name, new_record):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "New record is: " + str(new_record) + "\n"
    try:
        s += "Table before insert: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
        table.insert(new_record)
        s += "Table after insert: \n" + str(pd.DataFrame(table.get_rows())) + "\n"
    except Exception as e:
        s += "Exception: " + str(e) + "\n"
    print(s)


t_rdb = t_load("People", ["playerID"])
template_1 = {
        "birthYear": "1981",
        "birthMonth": "12"
    }

# t_find_by_template(t_rdb, "normal case", template_1)

template_2 = {
        "playerID": "aardsda01",
        "birthYear": "1980"
}

# t_find_by_template(t_rdb, "unmatched field", template_2)

# t_delete_by_key(t_rdb, "normal case", ["zychto01"])
# t_delete_by_key(t_rdb, "no matched record", ["taverwi02"])

# t_delete_by_template(t_rdb, "normal case", template_1)
# t_delete_by_template(t_rdb, "no matching record", template_2)

new_values_1 = {
    "birthYear": "1997",
    "birthMonth": "6",
    "birthDay": "3"
}
# t_update_by_key(t_rdb, "normal case", ["aaronha01"], new_values_1)

# t_update_by_template(t_rdb, "normal case", template_1, new_values_1)

new_record_1 = {
    "playerID": "yunqing01",
    "birthCountry": "China",
    "birthState": "Jiangxi",
    "birthCity": "Jian"
}

new_record_2 = {
    "birthCountry": "China",
    "birthState": "Jiangxi",
    "birthCity": "Jian"
}

new_record_3 = {
    "playerID": "yunqing01",
    "birthTime": "03-15-AM"
}

new_record_4 = {
    "playerID": "aaronha01"
}
t_insert(t_rdb, "normal case", new_record_1)
t_insert(t_rdb, "Lack of primary key", new_record_2)
t_insert(t_rdb, "unmatched key", new_record_3)
t_insert(t_rdb, "Record existed", new_record_4)