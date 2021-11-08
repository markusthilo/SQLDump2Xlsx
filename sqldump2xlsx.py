#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-10-27'
__license__ = 'GPL-3'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from sqlparse import parse
from sqlparse.tokens import Keyword, DML
from datetime import datetime
from argparse import ArgumentParser, FileType
from os import path
from sys import exit as sysexit

class Excel(Workbook):
	'Write to Excel File'

	def add_table(self, sheetname, headline):
		'Add a SQL Table to the Excel File'
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

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='dummy', database='test', directory=''):
		'Generate client to a given database'
		db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.cursor = db.cursor()
		self.directory = directory

	def fetchall(self):
		cursor.execute('SHOW tables;')
		for table in cursor.fetchall():
			cursor.execute(f'SELECT * FROM {table[0]};')
			rows = cursor.fetchall()
			if len(rows) > 0:
				xlsx = Excel(f'{table[0]}.xlsx', cursor.column_names)
				for row in rows:
					xlsx.append(row)
				xlsx.close()

class SQLParser:
	'Parse without a running SQL server'

	def __init__(self, dumpfiles):
		'Open SQL Dump'
		self.dumpfiles = dumpfiles

	def fetchall(self):
		'Line by line'
		for dumpfile in self.dumpfiles:
			for line in dumpfile:
				try:
					for token in parse(line)[0]:
						yield token
				except IndexError:
					continue

	def search_insert(self):
		pos = 'outside'
		for token in self.fetchall():
			if token.ttype == DML and token.value.upper() == 'INSERT':
				pos = 'INSERT'
			if pos == 'INSERT' and token.ttype == Keyword and token.value.upper() == 'INTO':
				pos = 'INTO'
			if pos == 'INTO' and token.ttype == None:
				pos = 'table'
				table = token.value.split('(')
				tablename = table[0]
				colnames = table[1].split(')')[0].split(',')
				print('table', tablename, colnames)
			if pos == 'table' and token.ttype == Keyword and token.value.upper() == 'VALUES':
				pos = 'VALUES'
			if pos == 'VALUES' and token.ttype == None:
				print(token.value)		

	def normalize(self, element):
		pass			



if __name__ == '__main__':	# start here if called as application
	argparser = ArgumentParser(description='Decode SQL dump files')
	argparser.add_argument('dumpfile', nargs='*', type=FileType('rt'),
		help='File to read,', metavar='FILE'
	)
	args = argparser.parse_args()
	sqlparser = SQLParser(args.dumpfile)
	sqlparser.search_insert()



