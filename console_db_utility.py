"""
It's simple console program what interacts with database
"""

import os
import sys
from collections import namedtuple
import json
import psycopg2

CompanyUnit = namedtuple('CompanyUnit', 'id parent_id name')


def connect(database, user, password, host="localhost", port=5432):
    """
    Create 'company_units' table if it doesn't exist and return
    object connection DB.
    """

    with psycopg2.connect(host=host, port=port, database=database, user=user,
                          password=password) as db, db.cursor() as cursor:

        command = """
            CREATE TABLE IF NOT EXISTS company_units (
                id INTEGER PRIMARY KEY NOT NULL,
                parentId INTEGER,
                name TEXT NOT NULL
            )
            """
        cursor.execute(command)

        try:
            command = """
                ALTER TABLE company_units ADD CONSTRAINT fk_company_units
                    FOREIGN KEY (parentId) REFERENCES company_units(id)
                    """
            cursor.execute(command)
        except psycopg2.ProgrammingError as err:
            print(err)

        db.commit()
        return db


def import_JSON(db, filepath, clear_table=True):
    """
    This function import data from JSON file to 'company_units' table.
    If import operation is successful then it return True else False.
    The function has an optional argument 'clear_table'. If it is True
    then function delete all rows from 'company units' table before
    import operation.
    """

    data_json = None
    filepath = os.path.abspath(filepath)
    table_name = 'company_units'
    with open(filepath) as f:
        try:
            data_json = json.load(f)
        except json.decoder.JSONDecodeError as err:
            print('Attempt to import a bad JSON file with name {0}\n\t'
                  '{1}'.format(f.name, err))
            return False
            # raise ValueError(err_msg)
        if not fit_schema(data_json):
            print('JSON structure doesn\'t fit the scheme of "{0}" table.'
                  '\n\tLoading file: {1}'.format(table_name, f.name))
            return False
            # raise TypeError(err_message)

    command = 'INSERT INTO {} (id, ParentId, Name) VALUES ' \
              '(%s, %s, %s)'.format(table_name)
    with db.cursor() as cursor:
        try:
            #check table on exists
            cursor.execute('SELECT * FROM {}'.format(table_name))
        except psycopg2.ProgrammingError as err:
            print(err)
            return False

        if clear_table:
            cursor.execute('DELETE FROM {} WHERE 1 = 1'.format(table_name))

        for row in data_json:
            try:
                id = int(row['id'])
                parentId = None if row['ParentId'] is None \
                    else int(row['ParentId'])
                name = None if row['Name'] is None else str(row['Name'])
            except ValueError as err:
                db.rollback()
                print('Invalid data from file with name {0}\n\t{1}'.format(
                    f.name, err))
                return False

            try:
                cursor.execute(command, (id, parentId, name))
            except psycopg2.DatabaseError as err:
                db.rollback()
                print('Database Error: unable to import data from file'
                      ' with name {0}\n\t{1}'.format(f.name, err))
                return False

        db.commit()
        print('File imported successfully.')
        return True


def fit_schema(data_json):
    """
    This function return True if JSON structure fit
    the scheme of 'company_units' table else False
    """

    ids = ['id', 'ParentId', 'Name']
    if isinstance(data_json, list) and len(data_json):
        for element in data_json:
            valid = isinstance(element, dict) and \
                    list(element.keys()) == ids
            if not valid:
                return False
        return True

    return False
