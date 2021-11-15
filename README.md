# SQLDump2Xlsx

Genrate Excel files from a SQL table without relations

## Usage of the command line version

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

$ git clone https://github.com/markusthilo/SQLDump2Xlsx.git

$ cd SQLDump2Xlsx

$ pip install -r requirements.txt

## Warning

This is in testing / alpha state. Accuracy is not guaranteed.
