#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.3_2022-02-22'
__license__ = 'GPL-3'
__email__ = 'markus.thilo@gmail.com'
__status__ = 'Testing'
__description__ = 'Generate Excel files from SQL dump or SQLite database'

from mysql import connector as Mysql
from sqlite3 import connect as SqliteConnect
from xlsxwriter import Workbook
from datetime import datetime
from csv import writer as csvwriter
from argparse import ArgumentParser, FileType
from pathlib import Path

from sys import exit as sysexit
from sys import stdout, stderr

class Logger:
	'Simple logging as the standard library is for different needs'

	def __init__(self, info=None, logfile=None):
		'Create logger and logfile'
		self.info = info
		if logfile != None:
			self.logfh = open(logfile, 'wt', encoding='utf8')
		else:
			self.logfh = None
		self.buffer = ''
		self.orig_stderr_write = stderr.write
		stderr.write = self.handler_stderr

	def logfile_open(self, logfile=None, outdir=Path(), filename=None):
		'Create and open logfile with timestamp'
		if self.logfh != None:
			self.close()
		if logfile == None:
			if filename == None:
				filename = datetime.now().strftime('%Y-%m-%d_%H%M%S_log.txt')
			logfile = outdir / filename
		self.logfh = open(logfile, 'wt', encoding='utf8')

	def put(self, msg):
		'Put a message to stdout, info handler and/or logfile'
		if self.info == None:
			print(msg)
		else:
			self.info(msg)
		if self.logfh != None:
			print(self.timestamp() + msg, file=self.logfh)

	def handler_stderr(self, stream):
		'Handle write stream from stderr'
		if self.logfh != None or self.info != None:
			if stream == '\n':
				if self.logfh != None:
					msg = self.buffer.replace('\n', ' ').rstrip(' ')
					print(self.timestamp() + 'ERROR ' + msg, file=self.logfh)
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
		self.logfh.close()

class SQLClient:
	'Client for a running SQL Server'

	def __init__(self, logger,
			host='localhost',
			user='root',
			password='root',
			database='test'):
		'Generate client to a given database'
		self.logger = logger
		self.db = Mysql.connect(host=host, user=user, password=password, database=database)


	def close(self):
		'Close connection to database'
		self.db.close()

	def fetchall(self):
		'Fetch all tables and put into SQLite db'
		cursor = self.db.cursor()
		cursor.execute('SHOW tables;')
		tables = cursor.fetchall()
		for table in tables:
			tablename = f'`{table[0]}`'
			cursor.execute(f'SELECT * FROM {tablename};')
			sqlite_cmd = f'CREATE TABLE {tablename} (`'
			sqlite_cmd += '`, `'.join( e[0] for e in cursor.description )
			sqlite_cmd += '`);'
			self.logger.put(f'Executing in SQLite: {sqlite_cmd}')
			yield sqlite_cmd, ()
			self.logger.put(f'Filling {tablename}')
			for row in cursor.fetchall():
				sqlite_cmd = f'INSERT INTO {tablename} VALUES ('
				sqlite_cmd += '?, ' * (len(row) - 1)
				sqlite_cmd += '?);'
				yield sqlite_cmd, tuple( str(e) for e in row )

class SQLite:
	'Read and write SQLite file'

	def __init__(self, logger, sqlitefile):
		'Open database'
		self.db = SqliteConnect(sqlitefile)
		self.cursor = self.db.cursor()
		self.logger = logger

	def fetchall(self):
		'Generator to fetch all tables'
		self.cursor.execute("SELECT name FROM sqlite_schema WHERE type = 'table';")
		for table in self.cursor.fetchall():
			self.logger.put('Fetching data from SQLite DB by SELECT * FROM ' + table[0])
			self.cursor.execute(f'SELECT * FROM {table[0]};')
			rows = self.cursor.fetchall()
			yield {
				'tablename': table[0],
				'colnames': list(map(lambda des: des[0], self.cursor.description))
			}
			for row in rows:
				yield row

	def fill(self, translator):
		'Fill sqlite db by giving a generator for commands'
		for cmd_str, values in translator():
			try:
				self.cursor.execute(cmd_str, values)
			except:
				self.logger.put('SQLite reported errors while executing '
					+ cmd_str
					+ ' with value(s) '
					+ str(values)
				)
		self.db.commit()

	def close(self):
		'Close SQLite database'
		self.db.close()

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

	def __init__(self, dumpfile):
		'Create object for one sql dump file'
		self.dumpfh = open(dumpfile, 'rt', encoding='utf8')

	def close(self):
		'Close SQL dump file'
		self.dumpfh.close()

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
				line = self.dumpfh.readline()
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
		return text, line

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
				line = self.dumpfh.readline()
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

	def __init__(self, logger, dumpfile):
		'Generate decoder for SQL dump file'
		self.logger = logger
		self.name = dumpfile.stem
		self.sqldump = SQLDump(dumpfile)

	def close(self):
		'Close SQL dump'
		self.sqldump.close()

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

	def list2quotes(self, in_brackets):
		'Generate string with brackets from a list of elements'
		return ' (`' + '`, `'.join(in_brackets) + '`)'

	def list2qmarks(self, in_brackets):
		'Generate string linke (?, ?, ?) from a list of elements'
		return ' (' + '?, ' * (len(in_brackets) - 1) + '?)'

	def unbracket(self, in_brackets):
		'Remove brackets from strings in an iterable'
		return [ string.strip('\'"`') for string in in_brackets ]

	def transall(self):
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
				self.logger.put('Generating table in SQLite DB by ' + cmd_str)
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
				self.logger.put('Filling SQLite db by ' + base_str + '...')
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
			if cmd_str == 'COPY':	# COPY FROM stdin
				first_part_cmd, matching, part_cmd = self.seek_strings(part_cmd, '(')
				if not matching:	# skip if no nothing to insert
					continue
				in_brackets, part_cmd = self.get_list(part_cmd)
				base_str = f'INSERT INTO `{first_part_cmd[0]}`' + self.list2quotes(in_brackets)
				self.logger.put(f'Putting data to SQLite DB by {base_str} from original command {cmd_str}')
				values = next(self.sqldump.read_cmds())
				set_len = len(in_brackets)
				base_str += ' VALUES' + self.list2qmarks(in_brackets) + ';'
				for value_ptr in range(0, len(values), set_len):	# loop through values
					yield base_str, values[value_ptr:value_ptr+set_len]

class Excel:
	'Write to Excel File'

	def __init__(self, table, outdir=Path(), maxfieldsize=255, maxtnamewidth=31):
		'Generate Excel file and writer'
		self.tablename = table['tablename']
		self.maxfieldsize = maxfieldsize
		self.filename = self.tablename + '.xlsx'
		self.workbook = Workbook(outdir / self.filename,
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
		col_cnt = 0
		if self.maxfieldsize > 0:
			for col in row:
				self.worksheet.write(self._row_cnt, col_cnt, col[:self.maxfieldsize])
				col_cnt += 1
		else:
			for col in row:
				self.worksheet.write(self._row_cnt, col_cnt, col)
				col_cnt += 1
		self._row_cnt += 1

	def close(self):
		'Close file = write Excel file'
		self.workbook.close()

class Csv:
	'Write to CSV files'

	def __init__(self, table, outdir=Path(), maxfieldsize=255):
		'Generate CSV file and writer'
		self.tablename = table['tablename']
		self.filename = table['tablename'] + '.csv'
		self.maxfieldsize = maxfieldsize
		self.csvfh = open(outdir / self.filename, 'w', encoding='utf-8', newline='')
		self.writer = csvwriter(self.csvfh, dialect='excel', delimiter='\t')
		self.writer.writerow(table['colnames'])

	def append(self, row):
		'Append one row to CSV file'
		if self.maxfieldsize > 0:
			self.writer.writerow( col[:self.maxfieldsize] for col in row )
		else:
			self.writer.writerow(row)

	def close(self):
		'Close file'
		self.csvfh.close()

class Worker:
	'Main class'

	def __init__(self, Writer,
		outdir = None,
		sqlitefile = None,
		logfile = None,
		info = None,
		maxfieldsize = 255
	):
		'Generate the worker'
		self.Writer = Writer
		self.outdir = outdir
		self.sqlitefile = sqlitefile
		self.logger = Logger(logfile=logfile, info=info)
		self.maxfieldsize = maxfieldsize

	def write(self):
		'Write to file with given class Witer'
		if self.Writer == None:
			return		
		thistable = None
		for row in self.sqlite.fetchall():
			if row == thistable:
				continue
			if isinstance(row, dict):
				try:
					writetable.close()
				except NameError:
					pass
				writetable = self.Writer(row,
					outdir=self.outdir,
					maxfieldsize=self.maxfieldsize
				)
				thistable = row
			elif row != None:
				writetable.append(row)
		try:
			writetable.close()
		except:
			raise RuntimeError('No files generated')

	def mk_outdir(self, name):
		'Make outdir and check if emty'
		if self.Writer != None or self.logger.logfh == None or self.sqlitefile == None:
			if self.outdir == None:
				self.outdir = Path() / name
			self.outdir.mkdir(parents=True, exist_ok=True)
			if any(self.outdir.iterdir()):
				raise RuntimeError('Destination directory needs to be emtpy')
			if self.logger.logfh == None:
				self.logger.logfile_open(outdir=self.outdir)
			self.logger.put('Writing into directory ' + str(self.outdir.resolve()))

	def mk_log(self, name):
		'Make logfile'
		if self.Writer != None or ( self.sqlitefile == None and self.outdir != None ):
			if self.logger.logfh == None:
				self.logger.logfile_open(outdir=self.outdir)
		elif self.logger.logfh == None:
			self.logger.logfile_open(filename=Path(name + '_log.txt'))

	def mk_sqlite(self, name):
		'Make empty sSQLite file'
		if self.sqlitefile == None:
			if self.outdir == None:
				self.sqlitefile = Path() / ( name + '.db' )
			else:
				self.sqlitefile = self.outdir / ( name + '.db' )
		if self.sqlitefile.exists():
			raise RuntimeError(f'File {str(self.sqlitefile.resolve())} exists')
		self.sqlite = SQLite(self.logger, self.sqlitefile)

	def fromfile(self, dumpfile):
		'Fetch from SQL dump or SQLite db file'
		with open(dumpfile, 'rb') as dumpfh:	# dumpfile or sqlite db file?
			self.is_sqlite = ( dumpfh.read(16) == b'SQLite format 3\x00' )
		self.mk_outdir(dumpfile.stem)
		self.mk_log(dumpfile.stem)
		if self.is_sqlite:
			self.sqlite = SQLite(self.logger, dumpfile)
			self.write()
		else:
			self.mk_sqlite(dumpfile.stem)
			self.sqldecoder = SQLDecoder(self.logger, dumpfile)
			self.sqlite.fill(self.sqldecoder.transall)
			self.write()
			self.sqldecoder.close()
		self.logger.put(f'All done parsing from {dumpfile.name}')
		self.logger.close()

	def fromserver(self, host=None, user=None, password=None, database=None):
		'Fetch from SQL server'
		self.mk_outdir(database)
		self.mk_log(database)
		self.mk_sqlite(database)
		sqlclient = SQLClient(self.logger,
			host = host,
			user = user,
			password = password,
			database = database
		)
		self.sqlite.fill(sqlclient.fetchall)
		self.write()
		sqlclient.close()
		self.logger.put(f'All done fetching from SQL server')
		self.logger.close()

if __name__ == '__main__':	# start here if called as application
	argparser = ArgumentParser(description=__description__)
	argparser.add_argument('-o', '--outdir', type=Path,
		help='Directory to write generated files (default: current)', metavar='DIRECTORY'
	)
	argparser.add_argument('-q', '--sqlite', type=Path,
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
		help='Set maximum field size (0 = no limit, default: 255)', metavar='INTEGER'
	)
	argparser.add_argument('-l', '--log', type=Path,
		help='Set logfile (default: *_log.txt in destination directory)', metavar='FILE'
	)
	argparser.add_argument('-c', '--csv', action='store_true',
		help='Generate CSV files, not Excel'
	)
	argparser.add_argument('-x', '--noxlsx', action='store_true',
		help='Do not generate Excel or CSV, SQLite only (useless if source is SQLite)'
	)
	argparser.add_argument('dumpfile', nargs='?', type=Path,
		help='SQL dump file to read (if none: try to connect  a server)', metavar='FILE'
	)
	args = argparser.parse_args()
	if args.noxlsx:
		Writer = None
	else:
		if args.csv:
			Writer = Csv
		else:
			Writer = Excel
	worker = Worker(Writer,
		outdir = args.outdir,
		sqlitefile = args.sqlite,
		logfile = args.log,
		maxfieldsize = args.max
	)
	if args.dumpfile == None:
		worker.fromserver(
			host = args.host,
			user = args.user,
			password = args.password,
			database = args.database
		)
	else:
		worker.fromfile(args.dumpfile)
	sysexit(0)
