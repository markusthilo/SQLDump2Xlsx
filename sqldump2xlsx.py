#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.3_2022-01-28'
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
from os import chdir, mkdir, getcwd, listdir
from pathlib import Path
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

	def logfile_open(self, sourcefile=None):
		'Create and open logfile with timestamp'
		if sourcefile == None:
			self.logfile = open(datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt'), 'w')
		else:
			self.logfile = open(Path(sourcefile.name).stem + '_log.txt', 'w')

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

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, host='localhost', user='root', password='root', database='test'):
		'Generate client to a given database'
		self.db = Mysql.connect(host=host, user=user, password=password, database=database)
		self.name = database

	def close(self):
		'Close connection to database'
		self.db.close()

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

class SQLDump:
	'Handle dump file'

	SQL_COMMANDS = (
		'*',
		'AND',
		'AS',
		'BETWEEN',
		'BY',
		'COMMIT',
		'CONSTRAINT',
		'COPY',
		'CREATE',
		'DATABASE',
		'DELETE',
		'DISTINCT',
		'DROP',
		'EXISTS',
		'FROM',
		'FULL',
		'FOREIGN',
		'GRANT',
		'GROUP',
		'HAVING',
		'IN',
		'INDEX',
		'INNER',
		'INSERT',
		'INTO',
		'JOIN',
		'KEY',
		'LEFT',
		'LIKE',
		'LOCK',
		'NOT',
		'OR',
		'ORDER',
		'PRIMARY',
		'REFERENCES',
		'REVOKE',
		'RIGHT',
		'ROLLBACK',
		'SAVEPOINT',
		'SELECT',
		'SET',
		'TABLE',
		'TABLES',
		'TOP',
		'TRUNCATE',
		'UNION',
		'UNIQUE',
		'UNLOCK',
		'UPDATE',
		'VIEW',
		'WHERE'
	)

	def __init__(self, dumpfile, maxfieldsize=255):
		'Create object for one sql dump file'
		self.dumpfile = dumpfile
		self.maxfieldsize = maxfieldsize
		self.name = dumpfile.name

	def close(self):
		'Close SQL dump file'
		self.dumpfile.close()

	def get_char(self, line):
		'Fetch the next character'
		if not line:
			return '', ''
		return line[0], line[1:]

	def get_word(self, line):
		'Get one word'
		word = ''
		while line:
			char, line = self.get_char(line)
			if char == None or char in ' \t,;()"\'\n':
				break
			word += char
		return word, char, line

	def fetch_quotes(self, quote, line):
		'Fetch everything inside quotes'
		text = ''
		while line:
			char, line = self.get_char(line)
			if not char:	# read next line if line is empty
				line = self.dumpfile.readline()
				if not line:	# eof
					break
				text += '\\n'	# generate newline char
				continue
			if char == '\\':	# get next char when escaped
				nextchar, line = self.get_char(line)
				if nextchar == None:
					continue
				text += char + nextchar
				continue
			if char == quote:
				break
			text += char
		if self.maxfieldsize == 0:
			return text, line
		return text[:self.maxfieldsize], line

	def read_cmds(self):
		'Line by line'
		line = '\n'
		char = '\n'
		cmd = list()
		while char:	# loop until eof
			if char == ';':	# give back whole comment on ;
				yield cmd
				cmd = list()
			elif char == '\\':	# \.
				char, line = self.get_char(line)
				if char == '.':
					yield cmd
					cmd = list()
				else:
					cmd += '\\' + char
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

	def __init__(self, dumpfile, sqlite=None, maxfieldsize=255):
		'Generate decoder for SQL dump file'
		self.sqldump = SQLDump(dumpfile, maxfieldsize=maxfieldsize)
		self.sqlite = sqlite

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
		while part_cmd != list():
			element, part_cmd = self.get_next(part_cmd)
			if element == ')' and bracket_cnt == 0:
				return part_cmd
			if element == '(':
				bracket_cnt += 1
			elif element == ')':
				bracket_cnt -= 1
		return list()

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

	def el2str(self, elements):
		'Generate string from elements'
		return ' ' + ' '.join(elements)

	def list2str(self, in_brackets):
		'Generate string with brackets from a list of elements'
		return ' (' + ', '.join(in_brackets) + ')'

	def list2qmarks(self, in_brackets):
		'Generate string linke (?, ?, ?) from a list of elements'
		return ' (' + '?, ' * (len(in_brackets) - 1) + '?)'

	def unbracket(self, in_brackets):
		'Remove brackets from strings in an iterable'
		return [ string.strip('\'"`') for string in in_brackets ]

	def transall(self, logger):
		'Fetch all tables'
		for raw_cmd in self.sqldump.read_cmds():
			cmd_str, part_cmd = self.get_next_upper(raw_cmd)
			if cmd_str == 'CREATE':	# CREATE TABLE
				element, part_cmd = self.get_next_upper(part_cmd)
				if element != 'TABLE':
					continue
				cmd_str += ' TABLE'
				first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(')
				if not matching:	# skip if no definitions in ()
					continue
				cmd_str += self.el2str(first_part_cmd)
				in_brackets, part_cmd = self.get_list(part_cmd)
				if in_brackets == list():
					continue
				cmd_str += self.list2str(in_brackets) + ';'
				logger.put('Generating table in SQLite DB by ' + cmd_str)
				yield cmd_str, ()
				continue
			if cmd_str == 'INSERT':	# INSERT INTO
				element, part_cmd = self.get_next_upper(part_cmd)
				if element != 'INTO':
					continue
				cmd_str += ' INTO'
				first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(', 'VALUES')
				if not matching:	# skip if no nothing to insert
					continue
				if matching == '(':
					in_brackets, part_cmd = self.get_list(part_cmd)
					cmd_str += self.el2str(first_part_cmd) + self.list2str(in_brackets)
					first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, 'VALUES')
				base_str = cmd_str + self.el2str(first_part_cmd) + ' VALUES'
				logger.put('Putting data to SQLite DB by ' + base_str)
				while part_cmd != list():	# one command per value/row
					first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(')
					if not matching:	# skip if no values
						continue
					in_brackets, part_cmd = self.get_list(part_cmd)
					cmd_str = base_str + self.list2qmarks(in_brackets)
					first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, ',', ';')
					yield cmd_str + ';', self.unbracket(in_brackets)
					if matching == ';' :
						break
					continue
			print(raw_cmd)
			if cmd_str == 'COPY':	# COPY FROM stdin
				first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(')
				if not matching:	# skip if no nothing to insert
					continue
				in_brackets, part_cmd = self.get_list(part_cmd)
				base_str += 'INSERT INTO' + self.el2str(first_part_cmd) + self.list2str(in_brackets)
				first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, 'FROM') + ' VALUES'
				logger.put(f'Putting data to SQLite DB by {base_str} from original command {cmd_str}')
				values = next(self.sqldump.read_cmds())
				while values != list():	# read values
					
					while values != list():	# read one row of values
						
						first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '\\.', ';')
						if not matching:	# skip if no values
							continue
						in_brackets, part_cmd = self.get_list(part_cmd)
						cmd_str = base_str + self.list2qmarks(in_brackets)
						first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, ',', ';')
						yield cmd_str + ';', self.unbracket(in_brackets)
						if matching == ';' :
							break
						continue

	def fetchall(self, logger):
		'Fetch all tables'
		if self.sqlite == None:
			fname = Path(self.sqldump.name).stem + '.db'
			if Path(fname).exists():
				raise RuntimeError(f'File {fname} exists')
			self.db = Sqlite(fname)
		else:
			self.db = Sqlite(sqlite)
		cursor = self.db.cursor()
		for cmd_str, values in self.transall(logger):
			cursor.execute(cmd_str, values)
		self.db.commit()
		cursor.execute("SELECT name FROM sqlite_schema WHERE type = 'table';")
		for table in cursor.fetchall():
			logger.put('Fetching data from SQLite DB by SELECT * FROM ' + table[0])
			cursor.execute(f'SELECT * FROM {table[0]};')
			rows = cursor.fetchall()
			yield {'tablename': table[0], 'colnames': list(map(lambda des: des[0], cursor.description))}
			for row in rows:
				yield row

	def close(self):
		'Close SQLite database'
		self.db.close()

class Excel:
	'Write to Excel File'

	def __init__(self, table, maxwidth=255, maxtnamewidth=31):
		'Generate Excel file and writer'
		self.tablename = table['tablename']
		self.maxwidth = maxwidth
		self.workbook = Workbook(self.tablename + '.xlsx',
			{
				'use_zip64': True,
				'read_only_recommended': True
			}
		)
		self.worksheet = self.workbook.add_worksheet(table['tablename'][:maxtnamewidth])
		self.bold = self.workbook.add_format({'bold': True})
		for col in range(len(table['colnames'])):
			self.worksheet.write(0, col, table['colnames'][col], self.bold)
		self._row_cnt = 1

	def append(self, row):
		'Append one row to Excel worksheet'
		for col_cnt in range(len(row)):
			if self.maxwidth != 0 and isinstance(row[col_cnt], str):
				field = row[col_cnt][:self.maxwidth]
			else:
				field = row[col_cnt]
			self.worksheet.write(self._row_cnt, col_cnt, row[col_cnt])
		self._row_cnt += 1

	def close(self):
		'Close file = write Excel file'
		self.workbook.close()

class Csv:
	'Write to CSV files'

	def __init__(self, table):
		'Generate CSV file and writer'
		self.tablename = table['tablename']
		filename = table['tablename'] + '.csv'
		self.csvfile = open(filename, 'w', encoding='utf-8', newline='')
		self.writer = csvwriter(self.csvfile, dialect='excel', delimiter='\t')
		self.writer.writerow(table['colnames'])

	def append(self, row):
		'Append one row to CSV file'
		self.writer.writerow(row)

	def close(self):
		'Close file'
		self.csvfile.close()

class Main:
	'Main object'

	def __init__(self, decoder, Writer,
		outdir = None,
		logfile = None,
		info = None	
	):
		'Parse'
		logger = Logger(info=info, logfile=logfile)
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
	argparser.add_argument('-o', '--outdir', type=str,
		help='Directory to write generated files (default: current)', metavar='DIRECTORY'
	)
	argparser.add_argument('-q', '--sqlite', type=FileType('w', encoding='utf8'),
		help='SQLite database file to write when source is SQL dump file', metavar='FILE'
	)
	argparser.add_argument('-s', '--host', type=str, default='localhost',
		help='Hostname to connect to a SQL server (default: localhost)', metavar='STRING'
	)
	argparser.add_argument('-d', '--database', type=str, default='test',
		help='Name of database to connect (default: test)', metavar='STRING'
	)
	argparser.add_argument('-u', '--user', type=str, default='root',
		help='Username to connect to a SQL server (default: root)', metavar='STRING'
	)
	argparser.add_argument('-p', '--password', type=str, default='root',
		help='Username to connect to a SQL server (default: root)', metavar='STRING'
	)
	argparser.add_argument('-m', '--max', type=int, default=255,
		help='Set maximum field size while reading (0 = no limit, defailt: 255)', metavar='INTEGER'
	)
	argparser.add_argument('-l', '--log', type=FileType('w', encoding='utf8'),
		help='Set logfile (default: *_log.txt in destination directory)', metavar='FILE'
	)
	argparser.add_argument('-c', '--csv', action='store_true',
		help='Generate CSV files, not Excel'
	)
	argparser.add_argument('-x', '--noxlsx', action='store_true',
		help='Do not generate Excel or CSV when source is SQL dump file'
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
		decoder = SQLDecoder(args.dumpfile, sqlite=args.sqlite, maxfieldsize=args.max)
	if args.noxlsx:
		Writer = None
	else:
		if args.csv:
			Writer = Csv
		else:
			Writer = Excel
	Main(decoder, Writer,
		outdir = args.outdir,
		logfile = args.log, 	
	)
	sysexit(0)
