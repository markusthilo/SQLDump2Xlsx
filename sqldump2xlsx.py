#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-11-15'
__license__ = 'GPL-3'
__email__ = 'markus.thilo@gmail.com'
__status__ = 'Testing'
__description__ = 'Genrate Excel files from a SQL table without relations'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from datetime import datetime
from re import sub, search
from csv import writer as csvwriter
from argparse import ArgumentParser, FileType
from os import chdir, mkdir, getcwd
from logging import basicConfig, DEBUG, info, warning, error
from sys import exit as sysexit

class Excel:
	'Write to Excel File'

	def __init__(self, table):
		'Generate Excel file and writer'
		self.workbook = Workbook(table['tablename'] + '.xlsx')
		self.worksheet = self.workbook.add_worksheet(table['tablename'])
		self.bold = self.workbook.add_format({'bold': True})
		for col in range(len(table['colnames'])):
			self.worksheet.write(0, col, table['colnames'][col], self.bold)
		self.__row_cnt__ = 1

	def append(self, row):
		'Append one row to Excel worksheet'
		for col_cnt in range(len(row)):
			if isinstance(row[col_cnt], datetime):
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt], self.datetime)
			else:
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt])
		self.__row_cnt__ += 1

	def close(self):
		'Close file = write Excel file'
		self.workbook.close()

class Csv:
	'Write to CSV files'

	def __init__(self, table):
		'Generate CSV file and writer'
		self.csvfile = open(table['tablename'] + 'sql', 'wt')
		self.writer = csvwriter(self.csvfile, dialect='excel', delimiter='\t')

	def append(self, row):
		'Append one row to CSV file'
		self.writer.writerow(row)

	def close(self):
		'Close file'
		self.csvfile.close()

class SQLParser:
	'Parse without a running SQL server'

	def __init__(self, dumpfile, quotes=('"', "'"), brackets=('(', ')')):
		'Open SQL Dump'
		self.quotes = quotes
		self.brackets = brackets
		self.dumpfile = dumpfile

	def readlines(self):
		'Line by line'
		for line in self.dumpfile:
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

	def fetchall(self):
		'Decode SQL INSERT'
		self.fetchline = self.readlines()
		for self.line in self.fetchline:
			if self.find_cmd('INSERT') and self.find_cmd('INTO'):
				info('Found INSERT')
				yield {'tablename': self.decode_value(), 'colnames': self.decode_list()}
				if self.find_cmd('VALUES'):
					while True:
						yield self.decode_list()
						seperator = self.check_seperator()
						if seperator == ';':
							break
						if seperator == None:
							warning('Missing seperator - some rows might get ignored')
							break

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='root', database='test'):
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

class Worker:
	'Main loop to work table by table'

	def __init__(self, decoder, Writer, outdir=None):
		'Prepare directory to write results and logfile'
		if outdir != None:
			try:
				mkdir(outdir)
			except FileExistsError:
				pass
			chdir(outdir)
		basicConfig(
			filename = datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt'),
			format = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
			datefmt = '%Y-%m-%d %H:%M:%S',
			encoding = 'utf-8',
			level = DEBUG
		)
		info('Starting work, writing into directory ' + getcwd())
		info('Input method is ' + str(decoder))
		for row in decoder.fetchall():
			if isinstance(row, dict):
				try:
					writetable.close()
				except NameError:
					pass
				info('Processing table ' + row['tablename'])
				writetable = Writer(row)
			else:
				writetable.append(row)
		writetable.close()
		info('All done!')

if __name__ == '__main__':	# start here if called as application
	argparser = ArgumentParser(description=__description__)
	argparser.add_argument('-c', '--csv', action='store_true',
		help='Generate CSV files, not Excel'
	)
	argparser.add_argument('-d', '--database', type=str, default='test',
		help='Name of database to connect (default: test)', metavar='STRING'
	)
	argparser.add_argument('-o', '--outdir', type=str,
		help='Directory to write generated files (default: current)', metavar='DIRECTORY'
	)
	argparser.add_argument('-p', '--password', type=str, default='root',
		help='Username to connect to a SQL server (default: root)', metavar='STRING'
	)
	argparser.add_argument('-s', '--host', type=str, default='localhost',
		help='Hostname to connect to a SQL server (default: localhost)', metavar='STRING'
	)
	argparser.add_argument('-u', '--user', type=str, default='root',
		help='Username to connect to a SQL server (default: root)', metavar='STRING'
	)
	argparser.add_argument('dumpfile', nargs='?', type=FileType('rt'),
		help='SQL dump file to read (if none: try to connect  a server)', metavar='FILE'
	)
	args = argparser.parse_args()
	if args.dumpfile == None:
		decoder = SQLClient(host=args.host, user=args.user, password=args.password, database=args.database)
	else:
		decoder = SQLParser(args.dumpfile)
	if args.csv:
		writer = Csv
	else:
		writer = Excel
	worker = Worker(decoder, writer, args.outdir)
	sysexit(0)