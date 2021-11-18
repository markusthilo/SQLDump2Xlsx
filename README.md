# SQLDump2Xlsx

Generate Excel files from a SQL table without relations

## Usage of the command line version

$ ./sqldump2xlsx.py [-h] [-c] [-d STRING] [-o DIRECTORY] [-p STRING] [-s STRING] [-u STRING] [FILE]

or

$ python3 sqldump2xlsx.py [-h] [-c] [-d STRING] [-o DIRECTORY] [-p STRING] [-s STRING] [-u STRING] [FILE]

or

$ ./sqldump2xlsx.sh [-h] [-c] [-d STRING] [-o DIRECTORY] [-p STRING] [-s STRING] [-u STRING] [FILE]

### Positional arguments

#### FILE
SQL dump file to read (if none: try to connect a server)

### Optional arguments

####  -h, --help
show this help message and exit
####  -c, --csv
Generate CSV files, not Excel
####  -d STRING, --database STRING
Name of database to connect (default: test)
####  -o DIRECTORY, --outdir DIRECTORY
Directory to write generated files (default: current)
####  -p STRING, --password STRING
Username to connect to a SQL server (default: root)
####  -s STRING, --host STRING
Hostname to connect to a SQL server (default: localhost)
####  -u STRING, --user STRING
Username to connect to a SQL server (default: root)

## Installation

### Using git and pip

A Python 3 (project started on 3.9) environment is needed.

Run this:

$ git clone https://github.com/markusthilo/SQLDump2Xlsx.git

$ cd SQLDump2Xlsx

$ pip install -r requirements.txt

## GUI ##

I added sqldump2xlsx_gui.py to build a Windows executable with GUI if someone wants to run the tool on a noobish operating system... :-)

## Warning

This is in testing / alpha state.

Accuracy is not guaranteed.
