#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.1_2021-11-18'
__license__ = 'GPL-3'
__email__ = 'markus.thilo@gmail.com'
__status__ = 'Testing'
__description__ = 'Generate Excel files from a SQL table without relations'

from mysql import connector as Mysql
from xlsxwriter import Workbook
from datetime import datetime
from re import sub, search
from csv import writer as csvwriter
from argparse import ArgumentParser, FileType
from os import chdir, mkdir, getcwd, path, listdir
from warnings import warn
from sys import exit as sysexit
from sys import stdout, stderr

class Excel:
	'Write to Excel File'

	def __init__(self, table):
		'Generate Excel file and writer'
		self.tablename = table['tablename']
		fname_no_ext = self.tablename
		fcnt = 1
		while path.exists(fname_no_ext + '.xlsx'):
			fcnt += 1
			fname_no_ext += f'_SAME_TABLE_{fcnt:04d}'
		self.workbook = Workbook(fname_no_ext + '.xlsx',
			{
				'use_zip64': True,
				'read_only_recommended': True
			}
		)
		self.worksheet = self.workbook.add_worksheet(table['tablename'][:31])
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
		self.tablename = table['tablename']
		filename = table['tablename'] + 'csv'
		if path.exists(filename):
			self.csvfile = open(filename, 'a')
		else:
			self.csvfile = open(filename, 'w')
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
		self.name = sub('\.[^.]*$', '', dumpfile.name)

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

	def fetchall(self, logger):
		'Decode SQL INSERT'
		self.fetchline = self.readlines()
		for self.line in self.fetchline:
			if self.find_cmd('INSERT') and self.find_cmd('INTO'):
				tablename = self.decode_value()
				logger.put('Found INSERT INTO ' + tablename)
				yield {'tablename': tablename, 'colnames': self.decode_list()}
				if self.find_cmd('VALUES'):
					while True:
						yield self.decode_list()
						seperator = self.check_seperator()
						if seperator == ';':
							break
						if seperator == None:
							logger.put('WARNING: Missing seperator - some rows might beeing ignored')
							break

	def close(self):
		'Close input SQL dump file'
		self.dumpfile.close()

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='root', database='test'):
		'Generate client to a given database'
		db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.cursor = db.cursor()
		self.name = database

	def fetchall(self, logger):
		cursor.execute('SHOW tables;')
		for table in cursor.fetchall():
			logger.put('Executing SELECT * FROM ' + table[0])
			cursor.execute(f'SELECT * FROM {table[0]};')
			rows = cursor.fetchall()
			yield {'tablename': table[0], 'colnames': cursor.column_names}
			for row in rows:
				yield row

	def close(self):
		'Dummy'
		pass

class Logger:
	'Simple logging as the standard library is for different needs'

	def __init__(self, handler=[stderr.write]):
		'Create logger and logfile'
		self.filename = datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt')
		self.logfile = open(self.filename, 'a')
		self.handler = handler
		stderr.write = self.error
		self.buffer = ''

	def put(self, msg):
		'Put a message to log and handler if given'
		if self.handler != None:
			self.handler(msg)
		print(datetime.now().strftime('%Y-%m-%d %H%M%S.%f ')
			+ msg.replace('\n', ' '),
			file=self.logfile)

	def error(self, from_stderr):
		'Handle error from stderr'
		if from_stderr == '\n':
			self.put(self.buffer)
			self.buffer = ''
		else:
			self.buffer += from_stderr

	def close(self):
		'Close logfile'
		self.logfile.close()

class Worker:
	'Main loop to work table by table'

	def __init__(self, decoder, Writer, outdir=None, handler=None):
		'Parse'
		if outdir == None:
			outdir = decoder.name
		try:
			mkdir(outdir)
		except FileExistsError:
			if listdir(outdir):
				raise RuntimeError('Destination directory needs to be emtpy')
		chdir(outdir)
				logger = Logger(handler)
		logger.put('Starting work, writing into directory ' + getcwd())
		logger.put('Input method is ' + str(decoder))
		for row in decoder.fetchall(logger):
			if isinstance(row, dict):
				try:
					writetable.close()
				except NameError:
					pass
				logger.put('Processing table ' + row['tablename'])
				writetable = Writer(row)
			else:
				writetable.append(row)
		writetable.close()
		decoder.close()
		logger.put('All done!')
		logger.close()

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
		decoder = SQLClient(
			host=args.host,
			user=args.user,
			password=args.password,
			database=args.database
		)
	else:
		decoder = SQLParser(args.dumpfile)
	if args.csv:
		writer = Csv
	else:
		writer = Excel
	Worker(decoder, writer, outdir=args.outdir, handler=print)
	sysexit(0)
