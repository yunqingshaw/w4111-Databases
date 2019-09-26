CSVDataTable.py
***************************
__init__(): set up a CVSDataTable obejct. If given data, then use that data, else load from file.

__str__(): use to print the information of object, include name, key_columns, connect_info and the table rows

_add_row(): used when load or insert or update

_load(): load data table from existing file

_save(): copy the object's data back to file

_matches_template(): used in all "by_key" functions

find_by_primary_key: check the given values of primary keys, if no problem, iterate rows to match the corresponding record. check the number of return record, which must be only one

find_by_template: iterate rows to match the keys and values in template, return records can be multiple. if field_list is given, only show the concerning fields of the return record.

delete_by_key/template: first find the records, then remove from data table

update_by_key/template: first find the records, then delete and add the new ones

insert: check the info contains the primary key, and the info doesn't contain unkown field, and the primary key of record to be inserted is different from any record existing

*********************************
RDBDataTable.py
**********************************
mainly use sql function, but need to construct the command string.



 
