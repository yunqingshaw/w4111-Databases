
from src.CSVDataTable import CSVDataTable
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
data_dir = os.path.abspath("../Data/Baseball")


def t_load(file_name, key_columns):

    connect_info = {
        "directory": data_dir,
        "file_name": file_name
    }

    csv_tbl = CSVDataTable(file_name, connect_info, key_columns)
    print("Created table = " + str(csv_tbl))
    return csv_tbl

def t_find_by_primary_key(table, t_name, key_fields, field_list=None):
    s = "=================================\n"
    s += "Test name is: " + t_name + "\n"
    s += "Key fields are:" + str(key_fields) + "\n"
    s += "Field list is: " + str(field_list) + "\n"
    try:
        result = table.find_by_primary_key(key_fields, field_list)
        s += "The query result is: " + str(pd.DataFrame(result)) + "\n"
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

# t_csv = t_load("People.csv", ["playerID"])
#
# t_find_by_primary_key(t_csv, "normal case", ["aardsda01"])
# t_find_by_primary_key(t_csv, "incorrect key numbers", ["aardsda01", "1981"])
# t_find_by_primary_key(t_csv, "no matching record", ["aardsda02"])
# t_find_by_primary_key(t_csv, "field list is not none", ["aardsda01"], ["birthYear", "birthMonth", "birthDay"])

template_1 = {
        "birthYear": "1981",
        "birthMonth": "12"
    }
template_3 = {
    "yearID": "1871",
    "stint": "1"
}
# t_find_by_template(t_csv, "normal case", template_1)

template_2 = {
        "playerID": "aardsda01",
        "birthYear": "1980"
}
field_list_1 = ["birthYear", "birthMonth", "birthDay"]
# t_find_by_template(t_csv, "unmatched field", template_2)
# t_find_by_template(t_csv, "given field_list", template_1, field_list_1)
# t_delete_by_key(t_csv, "normal case", ["zychto01"])
# t_delete_by_key(t_csv, "no matched record", ["taverwi02"])

#t_delete_by_template(t_csv, "normal case", template_1)
#t_delete_by_template(t_csv, "no matching record", template_2)

new_values_1 = {
    "birthYear": "1997",
    "birthMonth": "6",
    "birthDay": "3"
}
# t_update_by_key(t_csv, "normal case", ["aardsda01"], new_values_1)

# t_update_by_template(t_csv, "normal case", template_1, new_values_1)

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
# t_insert(t_csv, "normal case", new_record_1)
# t_insert(t_csv, "Lack of primary key", new_record_2)
# t_insert(t_csv, "unmatched key", new_record_3)
# t_insert(t_csv, "Record existed", new_record_4)

t_csv_1 = t_load("Batting.csv", ["playerID"])
# t_find_by_template(t_csv_1, "normal case", template_3)
t_find_by_primary_key(t_csv_1, "field list is not none", ["abercda01"], ["yearID", "stint"])