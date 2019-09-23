
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):
        """
        show first and last rows with separators in between
        :return:
        """
        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):
        """when no rows provided as para, use this function to load from existing file
        :return:
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, 'w') as txt_file:
            csv_d_wtr = csv.DictWriter(txt_file, fieldnames = full_name)
            csv_d_wtr.writeheader()
            csv_d_wtr.write(self._rows)

        self._logger.debug("CSVDataTable.save: Saved " + str(len(self._rows)) + " rows")

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row[k]:
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        result = []
        _key_columns = self._data["key_columns"]
        if len(key_fields) != len(_key_columns):
            raise Exception(str(len(_key_columns)) + " key fields are expected!")
        else:
            for row in self._rows:
                flag = True
                index = 0
                for key in _key_columns:
                    if row[key] != key_fields[index]:
                        flag = False
                        break
                    else:
                        index = index + 1
                if flag:
                    if field_list is not None:
                        new_row = {}
                        for field in field_list:
                            new_row[field] = row[field]
                        result.append(new_row)
                    else:
                        result.append(copy.copy(row))
            if len(result) == 0:
                raise Exception("No matching record was found.")
            elif len(result) > 1:
                raise Exception("More than one records were found.")
            #else:
                #self._logger.debug("The record was found successfully.")
        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = []
        for row in self._rows:
            if self.matches_template(row, template):
                if field_list is None:
                    result.append(copy.copy(row))
                else:
                    new_row = {}
                    for field in field_list:
                        new_row[field] = row[field]
                    result.append(new_row)
        if len(result) == 0:
            raise Exception("No record was found by template.")
        return  result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        rows_to_delete = self.find_by_primary_key(key_fields)
        for row in rows_to_delete:
            self._rows.remove(row)
        return len(rows_to_delete)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        rows_to_delete = self.find_by_template(template)
        for row in rows_to_delete:
            self._rows.remove(row)
        return len(rows_to_delete)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        rows_to_update = self.find_by_primary_key(key_fields)
        for row_to_update in rows_to_update:
            # for key in new_values.keys():
            #     row_to_update[key] = new_values[key]
            row_pre = copy.copy(row_to_update)
            row_new = {}
            self._rows.remove(row_to_update)
            for key in row_pre:
                if key in new_values.keys():
                    row_new.update([(key, new_values[key])])
                else:
                    row_new.update([(key, row_pre[key])])
            self._rows.append(row_new)
        return len(rows_to_update)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.  A dict?
        :return: Number of rows updated.
        """
        rows_to_update = self.find_by_template(template)
        for row_to_update in rows_to_update:
            row_pre = copy.copy(row_to_update)
            row_new = {}
            self._rows.remove(row_to_update)
            for key in row_pre:
                if key in new_values.keys():
                    row_new.update([(key, new_values[key])])
                else:
                    row_new.update([(key, row_pre[key])])
            self._rows.append(row_new)
        return len(rows_to_update)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        for key in new_record.keys():
            if key not in self._rows[0].keys():
                raise Exception("Unmatched keys found!")

        for key in self._data["key_columns"]:
            if key not in new_record.keys():
                raise Exception("Lack of some primary keys!")

        keys = []
        for key, value in new_record.items():
            if key in self._data["key_columns"]:
                keys.append(key)
        for key in keys:
            for row in self._rows:
                if new_record[key] == row[key]:
                    raise Exception("The record already exists, please update it.")

        row_to_insert = {}
        for key in self._rows[0].keys():
            if key in new_record.keys():
                row_to_insert[key] = new_record[key]
            else:
                row_to_insert[key] = None
        self._add_row(row_to_insert)

    def get_rows(self):
        return self._rows

