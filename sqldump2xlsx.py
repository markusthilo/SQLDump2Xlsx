#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.3_2022-01-24'
__license__ = 'GPL-3'
__email__ = 'markus.thilo@gmail.com'
__status__ = 'Testing'
__description__ = 'Generate Excel files from SQL dump without relations'

from mysql import connector as Mysql
from sqlite3 import connect as Sqlite
from xlsxwriter import Workbook
from datetime import datetime
from re import sub, search, match
from csv import writer as csvwriter
from argparse import ArgumentParser, FileType
from os import chdir, mkdir, getcwd, path, listdir
from warnings import warn
from sys import exit as sysexit
from sys import stdout, stderr

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

class SQLDump:
	'Handle dump file'

	SQL_COMMANDS = (
		'*',
		'AND',
		'AS',
		'BETWEEN',
		'BY',
		'COMMIT',
		'CREATE',
		'DATABASE',
		'DELETE',
		'DISTINCT',
		'DROP',
		'EXISTS',
		'FULL',
		'GRANT',
		'GROUP',
		'HAVING',
		'IN',
		'INDEX',
		'INNER',
		'INSERT',
		'INTO',
		'JOIN',
		'LEFT',
		'LIKE',
		'OR',
		'ORDER',
		'PRIMARY',
		'REVOKE',
		'RIGHT',
		'ROLLBACK',
		'SAVEPOINT',
		'SELECT',
		'TABLE',
		'TOP',
		'TRUNCATE',
		'UNION',
		'UPDATE',
		'VIEW',
		'WHERE'
	)

	def __init__(self, dumpfile):
		'Create object for one sql dump file'
		self.dumpfile = dumpfile

	def get_char(self, line):
		'Fetch the next character'
		if not line:
			return '', line
		return line[0], line[1:]

	def get_word(self, line):
		'Get one word'
		word = ''
		while True:
			char, line = self.get_char(line)
			if char == None or char in ' \t,;()"\'\n':
				return word, char, line
			word += char

	def fetch_quotes(self, quote, line):
		'Fetch everything inside quotes'
		text = ''
		while True:
			char, line = self.get_char(line)
			if not char:	# read next line if line is empty
				line = self.dumpfile.readline()
				if not line:	# eof
					return text, line
				text += '\\n'	# generate newline char
				continue
			if char == '\\':	# get next char when escaped
				text += char
				char, line = self.get_char(line)
				if char == None:
					continue
				text += char
				continue
			if char == quote:
				return text, line
			text += char

	def read_cmds(self):
		'Line by line'
		line = '\n'
		char = '\n'
		cmd = list()
		while char:	# loop until eof
			if char == ';':	# give back whole comment on ;
				yield cmd
				cmd = list()
			elif char in '(),':	# special chars
				cmd.append(char)
			elif char.isalnum():	# instruction or argument
				word, char, line = self.get_word(char + line)
				cmd.append(word)
				continue
			elif char in '\'"`':	# skip everything inside quotes
				text, line = self.fetch_quotes(char, line)
				cmd.append(char + text + char)
			while not line or line == '\n':	# read from dumpfile
				line = self.dumpfile.readline()
				if not line:	# eof
					char = ''
					break
				line = line.lstrip(' \t')	# skip leading blanks
				if line[0] in '-/':	# ignore comments and unimportand lines
					line = ''
					continue
			char, line = self.get_char(line)	# char by char
		if cmd != list():	# tolerate missing last ;
			yield cmd

class SQLDecoder:
	'Decode SQL dump to SQLite compatible commands'

	def __init__(self, dumpfile):
		'Generate naked Object'
		self.sqldump = SQLDump(dumpfile)

	def get_next(self, part_cmd):
		'Get next element'
		if part_cmd == list():	# to be save
			return '', list()
		return part_cmd[0], part_cmd[1:]

	def get_next_upper(self, part_cmd):
		'Get next element and normalize tu upper chars'
		if part_cmd == list():	# to be save
			return '', list()
		return part_cmd[0].upper(), part_cmd[1:]

	def check_strings(self, part_cmd, *strings):
		'Check for matching, strings must be uppercase'
		if part_cmd == list():	# to be save
			return '', list()
		if part_cmd[0].upper() in strings:
			return part_cmd[0], part_cmd[1:]
		return '', part_cmd

	def seek_strings(self, part_cmd, *strings):
		'Seek matching string'
		first_part_cmd = list()
		while part_cmd != list():
			matching, part_cmd = self.check_strings(part_cmd, *strings)
			if matching:
				return first_part_cmd, matching, part_cmd
			first_part_cmd.append(part_cmd[0])	# shift
			part_cmd = part_cmd[1:]
		return first_part_cmd, '', list()

	def skip_brackets(self, part_cmd):
		'Ignore everything inside brackets'
		bracket_cnt = 0
		while True:
			element, part_cmd = self.get_next(part_cmd)
			if element == ')' and bracket_cnt == 0:
				return part_cmd
			if element == '(':
				bracket_cnt += 1
			elif element == '}':
				bracket_cnt -= 1

	def get_list(self, part_cmd):
		'Get comma seperated list, take only the first elements behind the comma'
		elements = list()
		matching = ','
		while part_cmd != list():
			element, part_cmd = self.get_next(part_cmd)
			if not element:
				return elements, part_cmd
			if matching in '),' and not element.upper() in self.sqldump.SQL_COMMANDS:
				elements.append(element)
			first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(', ')', ',')
			if matching == '(':
				part_cmd = self.skip_brackets(part_cmd)
			elif not matching or matching == ')':
				return elements, part_cmd

	def fetchall(self, logger):
		'Fetch all tables'
		for cmd in self.sqldump.read_cmds():
			main_cmd, part_cmd = self.get_next_upper(cmd)
			print(part_cmd)
			if main_cmd == 'CREATE':	# CREATE TABLE
				element, part_cmd = self.get_next_upper(part_cmd)
				if element != 'TABLE':
					continue
				first_part_cmd, matching, part_cmd = self.seek_strings(cmd[1:], '(')
				if not matching:	# skip if no definitions in ()
					continue
				table_name = first_part_cmd[-1]
				column_names, part_cmd = self.get_list(part_cmd)
				print('>>>', main_cmd, table_name, column_names, part_cmd)
		return

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='root', database='test'):
		'Generate client to a given database'
		self.db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.name = database

	def fetchall(self, logger):
		'Fetch all tables'
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

class Worker:
	'Main loop to work table by table'

	def __init__(self, decoder, Writer, outdir=None, info=None):
		'Parse'
		logger = Logger(info=info)
		decoder.fetchall(logger)
		return	# DEBUG

		#if outdir == None:
		#	outdir = decoder.name
		#try:
		#	mkdir(outdir)
		#except FileExistsError:
		#	if listdir(outdir):
		#		raise RuntimeError('Destination directory needs to be emtpy')
		#chdir(outdir)
		#logger.logfile_open()
		#logger.put('Starting work, writing into directory ' + getcwd())
		#logger.put('Input method is ' + str(decoder))
		
		
		
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
		decoder = SQLDecoder(args.dumpfile)
	if args.csv:
		Writer = Csv
	else:
		Writer = Excel
	Worker(decoder, Writer, outdir=args.outdir)
	sysexit(0)
