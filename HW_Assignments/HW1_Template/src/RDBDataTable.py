from src.BaseDataTable import BaseDataTable
import sys
sys.path.append("f:\python3.7.4\lib\site-packages")
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._table_name = table_name
        self._cnx = pymysql.connect(host=connect_info["host"],
                                    user=connect_info["user"],
                                    password=connect_info["password"],
                                    db=connect_info["db"],
                                    charset=connect_info["charset"],
                                    cursorclass=pymysql.cursors.DictCursor)
        self._cur = self._cnx.cursor()
        self._key_columns = key_columns

    def find_by_primary_key(self, key_fields, field_list=None):
        """
        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if len(key_fields) != len(self._key_columns):
            raise Exception(str(len(self._key_columns)) + " key fields are expected!")
        else:
            sql = "select"
            if field_list is None:
                sql += " *"
            else:
                index = 0
                for field in field_list:
                    if index != 0:
                        sql += ","
                    sql += " " + str(field)
                    index += 1
            sql += " from " + str(self._table_name) + " where"
            index = 0
            for key in self._key_columns:
                if index != 0:
                    sql += " and"
                sql += " " + str(key) + "='" + str(key_fields[index]) + "'"
                index += 1
            res = self._cur.execute(sql)
            result = self._cur.fetchall()
            if len(result) == 0:
                raise Exception("No matching record was found.")
            elif len(result) > 1:
                raise Exception("More than one records were found.")
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
        sql = "select"
        if field_list is None:
            sql += " *"
        else:
            index = 0
            for field in field_list:
                if index != 0:
                    sql += ","
                sql += " " + str(field)
                index += 1
        sql += " from " + str(self._table_name) + " where"
        index = 0
        for key, val in template.items():
            if index != 0:
                sql += " and"
            sql += " " + str(key) + "='" + str(val) + "'"
            index += 1
        res = self._cur.execute(sql)
        result = self._cur.fetchall()
        if len(result) == 0:
            raise Exception("No matching record was found.")
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        if len(key_fields) != len(self._key_columns):
            raise Exception(str(len(self._key_columns)) + " key fields are expected!")
        else:
            sql = "delete from " + str(self._table_name) + " where"
            index = 0
            for key in self._key_columns:
                if index != 0:
                    sql += " and"
                sql += " " + str(key) + "='" + str(key_fields[index]) + "'"
                index += 1
            result = self._cur.execute(sql)
            # result = self._cur.fetchall()
            # if len(result) == 0:
            #     raise Exception("No matching record was found.")
            if result == 0:
                raise Exception("No matching record was found.")
        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        sql = "delete from " + str(self._table_name) + " where"
        index = 0
        for key, val in template.items():
            if index != 0:
                sql += " and"
            sql += " " + str(key) + "='" + str(val) + "'"
            index += 1
        result = self._cur.execute(sql)
        if result == 0:
            raise Exception("No matching record was found.")
        return result


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if len(key_fields) != len(self._key_columns):
            raise Exception(str(len(self._key_columns)) + " key fields are expected!")
        else:
            sql = "update " + str(self._table_name) + " set"
            index = 0
            for key, val in new_values.items():
                if index != 0:
                    sql += ","
                sql += " " + str(key) + "=\"" + str(val) + "\""
                index += 1
            sql += " where"
            index = 0
            for key in self._key_columns:
                if index != 0:
                    sql += " and"
                sql += " " + str(key) + "=\"" + str(key_fields[index]) + "\""
            result = self._cur.execute(sql)
            if result == 0:
                raise Exception("No matching record was found.")
            return result


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        sql = "UPDATE " + str(self._table_name) + " SET"
        index = 0
        for key, val in new_values.items():
            if index != 0:
                sql += ","
            sql += " " + str(key) + "=\"" + str(val) + "\" "
            index += 1
        sql += " WHERE"
        index = 0
        for key, val in template.items():
            if index != 0:
                sql += " and"
            sql += " " + str(key) + "=" + str(val)
            index += 1
        sql += ";"
        result = self._cur.execute(sql)
        if result == 0:
            raise Exception("No matching record was found.")
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        sql = "INSERT INTO " + str(self._table_name)
        index = 0
        sql += " ("
        for key, val in new_record.items():
            if index != 0:
                sql += ", "
            sql += str(key)
            index += 1
        sql += ") \n VALUES ("
        index = 0
        for key, val in new_record.items():
            if index != 0:
                sql += ", "
            sql += "\"" + str(val) + "\""
            index += 1
        sql += ")"
        self._cur.execute(sql)

    def get_rows(self):
        sql = "select * from " + self._table_name
        res = self._cur.execute(sql)
        return self._cur.fetchall()




