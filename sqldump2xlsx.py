#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.2_2021-12-06'
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
			if isinstance(row[col_cnt], bytes):
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt].decode())
			elif isinstance(row[col_cnt], datetime):
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt].strftime('%Y-%m-%d_%H:%M:%S.%f'))
			if isinstance(row[col_cnt], str) or isinstance(row[col_cnt], int) or isinstance(row[col_cnt], float):
				self.worksheet.write(self.__row_cnt__, col_cnt, row[col_cnt])
			else:
				self.worksheet.write(self.__row_cnt__, col_cnt, str(row[col_cnt]))
		self.__row_cnt__ += 1

	def close(self):
		'Close file = write Excel file'
		self.workbook.close()

class Csv:
	'Write to CSV files'

	def __init__(self, table):
		'Generate CSV file and writer'
		self.tablename = table['tablename']
		filename = table['tablename'] + '.csv'
		if path.exists(filename):
			self.csvfile = open(filename, 'a', encoding='utf-8', newline='')
		else:
			self.csvfile = open(filename, 'w', encoding='utf-8', newline='')
		self.writer = csvwriter(self.csvfile, dialect='excel', delimiter='\t')
		self.writer.writerow(table['colnames'])

	def append(self, row):
		'Append one row to CSV file'
		self.writer.writerow(row)

	def close(self):
		'Close file'
		self.csvfile.close()

class SQLParser:
	'Parse without a running SQL server'

	def __init__(self,
		dumpfile,
		quotes = '\'"`',
		blanks = ' \t',
		openbt = '(',
		closebt = ')',
		seperator = ',',
		eoc = ';',
		eol = '\n',
		escape = '\\',
		skiplinereg = '^[\t ]*--'
	):
		'Initialize parser object, open SQL Dump and set definitions'
		self.dumpfile = dumpfile
		self.quotes = quotes
		self.blanks = blanks
		self.openbt = openbt
		self.closebt = closebt
		self.seperator = seperator
		self.eoc = eoc
		self.eol = eol
		self.escape = escape
		self.skiplinereg = skiplinereg
		self.endvalue = quotes + blanks + (seperator + eoc)
		self.name = sub('\.[^.]*$', '', dumpfile.name)
		self.fetchline = self.readlines()
		self.line = ''

	def readlines(self):
		'Line by line'
		for rawline in self.dumpfile:
			cleanline = rawline.rstrip(self.eol)
			if cleanline != '':
				yield cleanline

	def check_next_line(self):
		'Check for end of line and get next if nexessary. Return True on EOF.'
		while self.line == '':
			try:
				self.line = next(self.fetchline)
			except:
				return True
			if search(self.skiplinereg, self.line) != None:
				self.line == ''
		return False

	def fetch_next_char(self, skipblanks=False):
		'Fetch the next character'
		while True:
			if self.check_next_line():
				return None
			char = self.line[0]
			self.line = self.line[1:]
			if not skipblanks or not char in self.blanks:
				return char

	def find_char(self, *chars):
		'Find a one of given characters'
		while True:
			char = self.fetch_next_char()
			if char == None:
				return None
			if char in chars:
				return char
		return None

	def find_cmd(self, cmd):
		'Find a given command. Return True on EOF.'
		len_cmd = len(cmd)
		while True:
			if self.check_next_line():
				return True
			if self.line[:len_cmd].upper() == cmd:
				self.line = self.line[len_cmd:]
				return False
			self.line = self.line[1:]
		return True

	def fetch_quotes(self, quote):
		'Fetch everything inside quotes'
		text = ''
		while True:
			char = self.fetch_next_char()
			if char == self.escape:
				text += char
				char = self.fetch_next_char()
				if char == None:
					return text
				text += char
				continue
			if char == quote:
				return text
			text += char

	def fetch_value(self, isinbrackets=True, skipafterquotes=True):
		'Fetch a value. Might be table name or column. Seek bracket on isinbrackets=False'
		value = ''
		while True:
			char = self.fetch_next_char(skipblanks=True)
			if char == None:
				return value, None
			if char in self.quotes:
				value += self.fetch_quotes(char)
				if skipafterquotes:
					return value, self.find_char(self.seperator, self.closebt, self.eoc)
				continue
			if char == self.openbt:
				inbrackets, endchar = self.fetch_value()
				if inbrackets == None:
					return value, None
				value += char + inbrackets + endchar
				continue
			if char == self.closebt or ( not isinbrackets and char in self.endvalue ):
				return value, char
			value += char

	def fetch_list(self, skipbracket=False):
		'Decode a list / columns. Skip search for 1st bracket on True'
		lst = []
		if not skipbracket:
			char = self.find_char(self.openbt)
			if char == None:
				return lst, None
		while True:
			value, endchar = self.fetch_value()
			lst.append(value)
			if endchar == self.closebt:
				return lst, endchar

	def fetch_cmd(self, *patterns):
		'Find next cmd and return it'
		while True:
			char = self.fetch_next_char()
			if char == None:
				return None, None
			if char in self.quotes:
				self.fetch_quotes(char)
			if char in patterns:
				return char, char
			if char.isalpha():
				found = char.upper()
				while True:
					char = self.fetch_next_char()
					if char == None:
						return found, None
					if not char.isalnum():
						if found in patterns:
							return found, char
						break
					found += char.upper()
			found = ''

	def fetchall(self, logger):
		'Decode SQL'
		while True:
			cmd, nextchar = self.fetch_cmd('INSERT', 'CREATE')
			if cmd == None or nextchar == None:
				break
			if cmd == 'CREATE':
				if self.find_cmd('TABLE'):
					break
				tablename, nextchar = self.fetch_value(isinbrackets=False)
				if tablename == None or nextchar == None:
					break
				logger.put('Found CREATE TABLE ' + tablename)
				colnames, nextchar = self.fetch_list()
				print(f'DEBUG: colnames= >{colnames}<, nextchar = >{nextchar}<') 
				if colnames != None:
					yield {'tablename': tablename, 'colnames': colnames}
				if nextchar == None:
					break
				continue
			if cmd == 'INSERT':
				if self.find_cmd('INTO'):
					break
				newtablename = self.fetch_value(isinbrackets=False)
				if newtablename == None:
					break
				newtablename = newtablename[:-1]
				logger.put('Found INSERT INTO ' + newtablename)
				cmd, seperator = self.fetch_cmd('VALUES', '(')
				if cmd != 'VALUES':
					newcolnames = self.fetch_list(skipbracket=(cmd=='('))
					if newcolnames == None:
						if newtablename != tablename:
							break
					else:
						colnames = newcolnames
					
					print('DEBUG', newcolnames)
					print(f'DEBUG self.line: >{self.line}<')
					
					if self.find_cmd('VALUES'):
						break
				tablename = newtablename
				yield {'tablename': tablename, 'colnames': colnames}
				skipbracket = seperator == '('
				while True:
					values = self.fetch_list(skipbracket=skipbracket)
					skipbracket = False
					yield values
					seperator = self.fetch_seperator()
					if seperator == ',':
						continue
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
		self.db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.name = database

	def fetchall(self, logger):
		cursor = self.db.cursor()
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

	def __init__(self, info=None, logfile=None):
		'Create logger and logfile'
		self.info = info
		self.logfile = logfile
		self.buffer = ''
		self.orig_stderr_write = stderr.write
		stderr.write = self.handler_stderr

	def logfile_open(self):
		'Create and open logfile with timestamp'
		self.logfile = open(datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt'), 'w')

	def put(self, msg):
		'Put a message to stdout, info handler and/or logfile'
		if self.info == None:
			print(msg)
		else:
			self.info(msg)
		if self.logfile != None:
			print(self.timestamp() + msg, file=self.logfile)

	def handler_stderr(self, stream):
		'Handle write stream from stderr'
		if self.logfile != None or self.info != None:
			if stream == '\n':
				if self.logfile != None:
					msg = self.buffer.replace('\n', ' ').rstrip(' ')
					print(self.timestamp() + 'ERROR ' + msg, file=self.logfile)
				if self.info != None:
					self.info(msg)
				else:
					self.orig_stderr_write(self.buffer + '\n')
				self.buffer = ''
			else:
				self.buffer += stream
		else:
			self.orig_stderr_write(stream)

	def timestamp(self):
		'Give timestamp for now'
		return datetime.now().strftime('%Y-%m-%d %H%M%S.%f ')

	def close(self):
		'Close logfile'
		self.logfile.close()

class Worker:
	'Main loop to work table by table'

	def __init__(self, decoder, Writer, outdir=None, info=None):
		'Parse'
		logger = Logger(info=info)
		if outdir == None:
			outdir = decoder.name
		try:
			mkdir(outdir)
		except FileExistsError:
			if listdir(outdir):
				raise RuntimeError('Destination directory needs to be emtpy')
		chdir(outdir)
		logger.logfile_open()
		logger.put('Starting work, writing into directory ' + getcwd())
		logger.put('Input method is ' + str(decoder))
		thistable = None
		for row in decoder.fetchall(logger):
			if row == thistable:
				continue
			if isinstance(row, dict):
				try:
					writetable.close()
				except NameError:
					pass
				writetable = Writer(row)
				thistable = row
			elif row != None:
				writetable.append(row)
		try:
			writetable.close()
		except:
			raise RuntimeError('No files generated')
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
	argparser.add_argument('dumpfile', nargs='?', type=FileType('rt', encoding='utf8'),
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
		Writer = Csv
	else:
		Writer = Excel
	Worker(decoder, Writer, outdir=args.outdir)
	sysexit(0)
