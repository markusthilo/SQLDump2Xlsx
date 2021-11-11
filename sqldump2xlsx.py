#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-10-27'
__license__ = 'GPL-3'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from datetime import datetime
from re import sub, search
from csv import writer
from argparse import ArgumentParser, FileType
from os import path
from sys import exit as sysexit

class StrDecoder:
	'Methods to decode strings'

	def decode_quotes(self, ins):
		'Decode string'
		inside = None
		ms = ''
		while ins != '':
			char = ins[0]
			ins = ins[1:]
			if char in self.quotes:
				if char == inside:
					return ms, ins
				inside = char
			else:
				ms += char
		return ms, ins

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

	def __init__(self, dumpfiles, quotes=('"', "'"), brackets=('(', ')')):
		'Open SQL Dump'
		self.quotes = quotes
		self.brackets = brackets
		self.dumpfiles = dumpfiles

	def fetchall(self):
		'Line by line'
		for dumpfile in self.dumpfiles:
			for line in dumpfile:
				if search(' *--|^$', line) == None:
					yield line.rstrip('\n')

	def norm_str(self, ins):
		'Normalize a string'
		if isinstance(ins, str):
			return sub('\W+', '', ins)
		else:
			return ins

	def norm_iter(self, it):
		'Normalize elements of an iterable an return a list'
		return [ self.norm_str(e) for e in it ]

	def cut_line(self, pos):
		'Cut line, check for end of line and append next if necessary'
		if pos == len(self.line) -1:
			self.line = next(self.fetchline)
		else:
			self.line = self.line[pos:]

	def decode_value(self):
		'Find a given command'
		m_value = search('[ ("]|$', self.line.upper())
		value = self.norm_str(self.line[:m_value.start()])
		self.cut_line(m_value.end())
		return value

	def decode_list(self):
		'Decode a list / columns'
		inside = None
		bcount = 1
		ms = ''
		while True:
			if self.line == '':
				self.line += next(self.fetchline)
			char = self.line[0]
			self.line = self.line[1:]
			if char == self.brackets[0]:
				continue
			if char == inside:
				if char == self.quotes:
					inside = None
				else:
					inside = char
				ms += char
			elif char == self.brackets[0]:
				bcount += 1
				if bcount < 1:
					continue
				else:
					ms += char
			elif char == self.brackets[1]:
				bcount -= 1
				if bcount < 1:
					break
				else:
					ms += char
			else:
				ms += char
		return self.norm_iter(ms.split(','))

	def find_cmd(self, cmd):
		'Find a given command'
		m_cmd = search(f'^{cmd} | {cmd} | {cmd}$|^{cmd}$', self.line.upper())
		if m_cmd == None:
			return False
		self.cut_line(m_cmd.end())
		return True

	def check_seperator(self):
		'Comma or semicolon?'
		m_comma = search(' *,', self.line)
		if m_comma != None:
			self.cut_line(m_comma.end())
			return ','
		m_semicolon = search(' *;', self.line)
		if m_semicolon != None:
			self.cut_line(m_semicolon.end())
			return ';'
		return None

	def decode_insert(self):
		'Decode SQL INSERT'
		self.fetchline = self.fetchall()
		for self.line in self.fetchline:
			if self.find_cmd('INSERT') and self.find_cmd('INTO'):
				yield {'tablename': self.decode_value(), 'colnames': self.decode_list()}
				if self.find_cmd('VALUES'):
					while True:
						yield self.decode_list()
						seperator = self.check_seperator()
						if seperator == ';':
							break
						if seperator == None:
							raise RuntimeError

if __name__ == '__main__':	# start here if called as application
	argparser = ArgumentParser(description='Decode SQL dump files')
	argparser.add_argument('dumpfile', nargs='*', type=FileType('rt'),
		help='File to read,', metavar='FILE'
	)
	args = argparser.parse_args()
	sqlparser = SQLParser(args.dumpfile)
	for line in sqlparser.decode_insert():
		print(line)
