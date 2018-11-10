
Console DB utility
===============

It's simple console program what interacts with database.
It imports data from a JSON file to table and allows to select data from it.


Used database: PostgreSQL 9.6.10
Used python: Python 3.6.6
It is tested with: Linux Mint 19



Configure
--------------

1. Create configuration file for database connection with name 'database.ini' and save it to the same directory where launch console_db_utility.py. Add to the configuration file all connection parameters like this example:

		[postgresql]
		host=localhost
		database=suppliers
		user=postgres
		password=postgres


2. install dependencies from requirements.txt

		pip install -r requirements.txt


Usage example
--------------

	./console_db_utility.py filename.json

or

	./console_db_utility.py filename.json -C

	./console_db_utility.py filename.json --clear-table
to remove all rows from the table before importing


Database model
--------------
The database model consists of a single table. It has the structure:

Tables name: "company_units"
Columns:
1. Name='id', definition='integer not null', constraints='primary key'
2. Name='parentId', definition='integer', constraints='foreign key (parentId) references company_units(id)'
3. Name='name', definition='text not null'

