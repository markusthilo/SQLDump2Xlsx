#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-10-27'
__license__ = 'GPL-3'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from datetime import datetime

import fileinput
import csv
import sys


class Excel:
	'Excel file'

	def __init__(self, filename, headline):
		'Generate writer'
		self.workbook = Workbook(filename)
		self.worksheet = self.workbook.add_worksheet()
		self.bold = self.workbook.add_format({'bold': True})
		for col in range(len(headline)):
			self.worksheet.write(0, col, headline[col], self.bold)
		self.__row_cnt__ = 1
		self.datetime = self.workbook.add_format()
		self.datetime.set_num_format('yyyy-mm-dd hh:mm')

	def append(self, row):
		'Append one row to Excel worksheet'
		for col_cnt in range(len(row)):
			if isinstance(row[col_cnt], datetime):
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt], self.datetime)
			else:
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt])
		self.__row_cnt__ += 1

	def close(self):
		'Close Excel file'
		self.workbook.close()

if __name__ == '__main__':	# start here if called as application
	db = Mysql.connect(host='localhost', user='root', password='lka712', database='test')
	cursor = db.cursor()
	cursor.execute('SHOW tables;')
	for table in cursor.fetchall():
		cursor.execute(f'SELECT * FROM {table[0]};')
		rows = cursor.fetchall()
		if len(rows) > 0:
			print(f'Working {table[0]} ...')
			xlsx = Excel(f'{table[0]}.xlsx', cursor.column_names)
			for row in rows:
				xlsx.append(row)
			xlsx.close()	
	print('All done!)




# This prevents prematurely closed pipes from raising
# an exception in Python
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

# allow large content in the dump
csv.field_size_limit(sys.maxsize)

def is_insert(line):
    """
    Returns true if the line begins a SQL insert statement.
    """
    return line.startswith('INSERT INTO') or False


def get_values(line):
    """
    Returns the portion of an INSERT statement containing values
    """
    return line.partition('` VALUES ')[2]


def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True


def parse_values(values, outfile):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
    for reader_row in reader:
        for column in reader_row:
            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    writer.writerow(latest_row)
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.
            latest_row.append(column)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            writer.writerow(latest_row)


def main():
    """
    Parse arguments and start the program
    """
    # Iterate over all lines in all files
    # listed in sys.argv[1:]
    # or stdin if no args given.
    try:
        for line in fileinput.input():
            # Look for an INSERT statement and parse it.
            if is_insert(line):
                values = get_values(line)
                if values_sanity_check(values):
                    parse_values(values, sys.stdout)
    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == "__main__":
    main()

'''
BEGIN {
  # file starts with DDL statements that go into header.sql

  table = "header";
  sql = 1
}

{
  # -- step 1 --
  # determine whether current line contains a DDL sql statement or
  # table data

  if ($0 ~ "^INSERT INTO "){
    # this is a data line for the current table, it goes into a csv file
    sql = 0
  }
  else if ($0 ~ "^DROP TABLE IF EXISTS"){
    # a new table is coming up
    # remember the name
    # output goes to sql file
    table = gensub(/DROP TABLE IF EXISTS `(.+)`;/, "\\1", "g" $0);
    sql = 1
  }
  else {
    # any other lines belong to the sql file of the current table
    sql = 1
  }

  # -- step 2 --
  # transform and write the line into target file

  if (sql == 1){
    # sql lines are appended to the <table_name>.sql file
    print > table".sql";
  }
  else {

    # data lines are split and written as individual csv records

    # inserts are of the form:
    # INSERT INTO `borders` VALUES ('A','D',784),...,('ZW','Z',797);

    # splitting on three separators:
    #   INSERT INTO `table_name` VALUES (   -- beginning of line
    #   ),(                                 -- record separator
    #   );                                  -- end of line

    # split records are collected in array 'a'

    n = split($0, a, /(^INSERT INTO `[^`]*` VALUES \()|(\),\()|(\);$)/)

    for(i=1;i<=n;i++) {
      # first and last splits may be empty strings
      len = length(a[i])
      if (len > 0) {
        # if record is not empty, output to <table_name>.csv file
        data = a[i]
        print data > table".csv";
      }
    }
  }
}

END {}
'''



