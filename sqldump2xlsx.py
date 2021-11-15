#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-11-15_alpha'
__license__ = 'GPL-3'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from datetime import datetime
from re import sub, search
from csv import writer
from argparse import ArgumentParser, FileType
from os import chdir, mkdir, getcwd
from logging import basicConfig, DEBUG, info, warning, error
from sys import exit as sysexit
from sys import stdout

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

	def __init__(self, outfile, dialect='excel', delimiter='\t'):
		'Generate Excel file and writer'
		self.workbook = Workbook(outfile)
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

class CSV:
	'Write to CSV files'

	def __init__(self, outfile, dialect='excel', delimiter='\t'):
		'Generate CSV file and writer'
		self.writer = writer(
			outfile,
			dialect=dialect,
			delimiter=delimiter
		)

	def append(self, row):
		'Append one row to Excel worksheet'


		
		if len(self.stats.data) > 0:
			if not self.noheadline:
				self.csvwriter.writerow(self.stats.data[0].keys())
			if self.reverse:
				for line in reversed(self.stats.data):
					self.__writerow__(line)
			else:
				for line in self.stats.data:
					self.__writerow__(line)
		else:
			self.csvwriter.writerow(['No data'])

	def __writerow__(self, line):
		'Write one row to CSV file'
		if not self.unixtime:
			line = self.chng_humantime(line)
		if not self.intbytes:
			line = self.chng_humanbytes(line)
		line = self.add_geostring(line)
		self.csvwriter.writerow(line.values())

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

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='dummy', database='test'):
		'Generate client to a given database'
		db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.cursor = db.cursor()

	def fetchall(self):
		cursor.execute('SHOW tables;')
		for table in cursor.fetchall():
			cursor.execute(f'SELECT * FROM {table[0]};')
			rows = cursor.fetchall()
			yield {'tablename': table[0], 'colnames': cursor.column_names}
			for row in rows:
				yield row

class Outdir:
	'Directory to write files'

	def __init__(self, outdir=None):
		'Prepare to write results and logfile'
		if outdir != None:
			try:
				mkdir(outdir)
			except FileExistsError:
				pass
			chdir(outdir)
		basicConfig(
			filename = datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt'),
			format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
			datefmt='%Y-%m-%d %H:%M:%S',
			encoding = 'utf-8',
			level = DEBUG
		)
		info('Starting work, writing to ' + getcwd())

if __name__ == '__main__':	# start here if called as application
	argparser = ArgumentParser(description='Decode SQL dump files')
	argparser.add_argument('-c', '--csv', action='store_true',
		help='Generate CSV files, not Excel'
	)
	argparser.add_argument('-d', '--database', type=str,
		help='Name of database to connect', metavar='STRING'
	)
	argparser.add_argument('-o', '--outdir', type=str,
		help='Directory to write generated files', metavar='DIRECTORY'
	)
	argparser.add_argument('-p', '--password', type=str,
		help='Username to connect to a SQL server', metavar='STRING'
	)
	argparser.add_argument('-s', '--host', type=str,
		help='Hostname to connect to a SQL server', metavar='STRING'
	)
	argparser.add_argument('-u', '--user', type=str,
		help='Username to connect to a SQL server', metavar='STRING'
	)
	argparser.add_argument('dumpfile', nargs='*', type=FileType('rt'),
		help='File(s) to read,', metavar='FILE'
	)
	args = argparser.parse_args()
	sqlparser = SQLParser(args.dumpfile)
	Outdir(args.outdir)

	#csvwriter = writer(stdout)
	for line in sqlparser.decode_insert():	# DEBUG
		print(line)
	#	csvwriter.writerow(line)
	c = CSV(stdout)
	sysexit(0)