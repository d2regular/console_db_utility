#!/usr/bin/env python3


"""
It's simple console program what interacts with database
"""

import os
import sys
import json
from collections import namedtuple
import psycopg2
from configparser import ConfigParser


def main():
    # get command line arguments
    args = process_sysargv()
    filename = args['filename']
    clear_table = args['clear_table']

    db = None
    try:
        # set db connection
        params = config('database_1.ini')
        db = psycopg2.connect(**params)

        # create table
        if not create_table(db):
            print('Error creating table\n')
            exit(1)

        # import from JSON-file to table
        if not import_JSON(db, filename, clear_table=clear_table):
            exit(1)

        # interactive menu of program
        menu = '(S)Select  (Q)uit'
        valid = frozenset('SQ')
        while True:
            print('\n\nSelect employees who belong to the same root unit '
                  'of company')
            menu_choice = get_menu_choice(menu, valid)
            if menu_choice == 'Q':
                return

            employee_id = get_integer('Input employee ID', 'employee ID')
            if menu_choice is False:
                return
            # show result query
            unit_employees(db, employee_id)

    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        exit(1)
    finally:
        if db is not None:
            db.close()


def config(filename='database.ini', section='postgresql'):
    """Read connection config. Return parameters of connection"""

    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Bad config file.\nSection {0} not found in the {1} file\n'.format(
                section, filename))

    return db


def create_table(db):
    """
    Create 'company_units' table if it doesn't exist and return True
    if operation was successful else False
    """
    create_command = """
                        CREATE TABLE IF NOT EXISTS company_units (
                            id INTEGER PRIMARY KEY NOT NULL,
                            parentId INTEGER,
                            name TEXT NOT NULL
                        )
                        """

    fk_command = """
                ALTER TABLE company_units ADD CONSTRAINT fk_company_units
                    FOREIGN KEY (parentId) REFERENCES company_units(id)
                    """

    try:
        cursor = db.cursor()
        cursor.execute(create_command)

        # foreign key exception
        try:
            cursor.execute(fk_command)
        except psycopg2.ProgrammingError:
            pass

        db.commit()
    except psycopg2.DatabaseError as err:
        print(str(err).strip())
        return False
    else:
        return True
    finally:
        cursor.close()


def import_JSON(db, filename, clear_table=False):
    """
    This function import data from JSON file to 'company_units' table.
    If import operation is successful then it return True else False.
    The function has an optional argument 'clear_table'. If it is True
    then function delete all rows from 'company units' table before
    import operation.
    """

    data_json = None
    filename = os.path.abspath(filename)
    table_name = 'company_units'
    f = None
    try:
        f = open(filename)
        data_json = json.load(f)
        if not fit_schema(data_json):
            print(
                '\nJSON structure doesn\'t fit the scheme of "{0}" table.'
                '\n\tLoading file: {1}'.format(table_name, f.name))
            return False
    except json.decoder.JSONDecodeError as err:
        print('\nAttempt to import a bad JSON file with name {0}\n\t'
              '{1}'.format(f.name, err))
        return False
    except IOError as err:
        print(err)
        return False
    finally:
        if f is not None:
            f.close()

    command = 'INSERT INTO {} (id, ParentId, Name) VALUES ' \
              '(%s, %s, %s)'.format(table_name)
    cursor = None
    try:
        cursor = db.cursor()
        try:
            # check table on exists
            cursor.execute('SELECT * FROM {}'.format(table_name))

            if clear_table:
                cursor.execute('DELETE FROM {} WHERE 1 = 1'.format(table_name))
        except psycopg2.ProgrammingError as err:
            print(err)
            return False

        for row in data_json:
            try:
                id = int(row['id'])
                parentId = None if row['ParentId'] is None \
                    else int(row['ParentId'])
                name = None if row['Name'] is None else str(row['Name'])

                cursor.execute(command, (id, parentId, name))
            except ValueError as err:
                db.rollback()
                print('\nInvalid data from file with name {0}\n\t{1}'.format(
                    f.name, err))
                return False
            except psycopg2.DatabaseError as err:
                db.rollback()
                print('\nDatabase Error: unable to import data from file'
                      ' with name {0}\n\t{1}'.format(f.name, err))
                return False

    except psycopg2.DatabaseError as err:
        print(err)
        return False
    finally:
        if cursor is not None:
            cursor.close()

    try:
        db.commit()
        print('\nFile imported successfully.')
        return True
    except psycopg2.DatabaseError as err:
        print(err)
        return False


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


def unit_employees(db, id):
    """
    This function search employees who belong to a specific root unit of
    the company and output searching result to stdout.
    Function accept employee ID and with help it select data.
    """

    command = """
        --search all child units of certain company unit
        WITH RECURSIVE r AS (
            --search all company units to which  the employee belongs
            WITH RECURSIVE r2 AS (
                SELECT id, parentId, name FROM company_units
                    WHERE id = %s
                UNION 
                SELECT c2.id, c2.parentId, c2.name FROM company_units AS c2
                JOIN r2
                    ON c2.id = r2.parentId
            )
            --search the root company unit to which  the employee belongs
            SELECT r2.id, r2.parentId, r2.name FROM r2
                WHERE r2.parentId IS NULL 
            UNION
            SELECT c.id, c.parentId, c.name FROM company_units AS c
            JOIN r
                ON c.parentId = r.id
        )
        
        /*search only employees who belong to a specific root unit 
        of the company*/
        SELECT r.id, r.parentId, r.name FROM r
        EXCEPT 
        SELECT c.id, c.parentId, c.name FROM company_units AS c
        JOIN company_units AS c2
            ON c.id = c2.parentId
            GROUP BY c.id  
    """

    cursor = None
    try:
        cursor = db.cursor()
        cursor.execute(command, (id,))
        select_data = cursor.fetchall()
    except psycopg2.DatabaseError as err:
        print('\nDatabase Error: unable to get data from table'
              ' "company_units"\n\t{}'.format(err))
    else:
        row_count = 1
        print()
        print(' {0:<6} {1:<10} {2:<150}'.format('Num', 'ID', 'Name'))
        print('{:-<6} {:-<10} {:-<150}'.format('', '', ''))
        for row in select_data:
            print(' {0:<6} {1:<10} {2:<150}'.format(
                row_count, row[0], row[2]))
            print('{:-<6} {:-<10} {:-<150}'.format('', '', ''))
            row_count += 1
    finally:
        if cursor is not None:
            cursor.close()


def get_integer(message, name="integer", exit_key='Q'):
    """
    This function accept integer or Exit Key from stdin. Return integer
    or False if was inputted Exit Key.
    """

    exit_message = '(Press [{}] to stop operation)'.format(exit_key)
    while True:
        try:
            line = input('\n{0} {1}: '.format(exit_message, message))
            if line == str(exit_key):
                return False
            num = int(line)
            return num
        except ValueError:
            print("ERROR {0} must be an integer".format(name))


def get_menu_choice(message, valid, force_lower=False):
    """
    This function accept menu choice from stdin and return it
    """

    while True:
        line = input('\n{}: '.format(message))
        if line not in valid:
            print("ERROR only {0} are valid choices".format(
                ", ".join(["'{0}'".format(x) for x in sorted(valid)])))
        else:
            return line if not force_lower else line.lower()


def process_sysargv():
    """
    Process arguments and options from command line.
    """

    Option = namedtuple('Option', 'short_opt, long_opt, description')
    options = {
        'help': Option('-h', '--help', 'show this help message and exit'),
        'clear_table': Option('-C', '--clear-table',
                              'delete all rows from "company units" table '
                              'before import operation. [default: False]')
    }

    usage = "Usage: {} arg [options] \n".format(sys.argv[0])

    args = sys.argv[1:]

    if len(args) < 1 or len(args) > 2:
        print("incorrect number of arguments")
        exit(1)

    if len(args) == 1:
        if args[0] in ['-h', '--help']:
            print(usage)
            for opt in options.values():
                print('\t{0}, {1:<20} {2}'.format(
                    opt.short_opt, opt.long_opt, opt.description))
            exit(0)

        return {'filename': args[0], 'clear_table': False}

    if args[1] not in ['-C', '--clear-table']:
        print("incorrect argument: ", args[1])
        exit(1)

    return {'filename': args[0], 'clear_table': True}


main()
