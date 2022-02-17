#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Markus Thilo'
__version__ = '0.2_2021-11-30'
__license__ = 'GPL-3'
__email__ = 'markus.thilo@gmail.com'
__status__ = 'Testing'
__description__ = 'GUI for sqldump2xlsx.py on Windows'

from tkinter import Tk, StringVar, IntVar, PhotoImage, E, W, END, RIGHT
from tkinter.ttk import Label, Button, Notebook, Frame
from tkinter.ttk import LabelFrame, Entry, Radiobutton
from tkinter.filedialog import askopenfilename, askdirectory
from tkinter.messagebox import showerror
from tkinter.scrolledtext import ScrolledText
from datetime import datetime
from sys import stderr, stdout
from os import listdir
from sqldump2xlsx import *

class Main(Tk):
	'Main window'

	def __init__(self, icon_base64):
		'Define the main window'
		super().__init__()
		self.title('SQLDump2Xlsx')
		self.resizable(0, 0)
		self.iconphoto(False, PhotoImage(data = icon_base64))
		self.notebook = Notebook(self)
		self.notebook.pack(padx=10, pady=10, expand=True)
		### File ###
		self.frame_file = Frame(self.notebook)
		self.frame_file.pack(fill='both', expand=True)
		self.notebook.add(self.frame_file, text='File')
		self.filename = StringVar()
		Button(self.frame_file,
			text = 'SQL dump file:',
			command = lambda: self.filename.set(
				askopenfilename(
					title = 'Select SQL dump file',
					filetypes = (("SQL files","*.sql"),("All files","*.*"))
					)
				)
			).grid(column=0, row=0, sticky=W, padx=10, pady=10)
		Entry(self.frame_file, textvariable=self.filename, width=112).grid(
			column=0, row=1, columnspan=2, padx=10, pady=10)
		Button(self.frame_file,
			text = 'Parse',
			command = lambda: self.parse('file')
		).grid(column=1, row=2, sticky=E, padx=10, pady=10)
		### Server ###
		self.frame_server = Frame(self.notebook)
		self.frame_server.pack(fill='both', expand=True)
		self.notebook.add(self.frame_server, text='Server')
		self.host = self.server_field(0, 'Server:', 'localhost')
		self.user = self.server_field(1, 'Username:', 'root')
		self.password = self.server_field(2, 'Password:', 'root')
		self.database = self.server_field(3, 'Database:', 'test')
		Button(self.frame_server,
			text = 'Parse',
			command = lambda: self.parse('server')
			).grid(column=1, row=4, sticky=E, padx=10, pady=10)
		### Options ###
		self.frame_options = Frame(self)
		self.frame_options.pack(fill='both', expand=True)
		### Output file format ###
		self.fileformat = StringVar(None, 'xlsx')
		self.labelframe_fileformat = LabelFrame(self.frame_options, text='Output file format')
		self.labelframe_fileformat.pack(padx=10, pady=10, side='left')
		Radiobutton(self.labelframe_fileformat,
			text = 'Xlsx',
			value = 'xlsx',
			variable = self.fileformat
		).pack(padx=10, pady=10, side='left')
		Radiobutton(self.labelframe_fileformat,
			text = 'CSV',
			value = 'csv',
			variable = self.fileformat
		).pack(padx=10, pady=10, side='left')
		### Maximum field size ###
		self.maximum = IntVar()
		self.maximum.set(255)
		self.labelframe_maximum = LabelFrame(self.frame_options, text='Maximum field size')
		self.labelframe_maximum.pack(padx=10, pady=10, side='left')
		Entry(self.labelframe_maximum, textvariable=self.maximum, width=8).pack(padx=10, pady=10)

		### Infos ###
		self.labelframe_infos = LabelFrame(self, text='Infos')
		self.labelframe_infos.pack(padx=10, pady=10, fill='x')
		self.infos = ScrolledText(self.labelframe_infos, padx=10, pady=10, width=80, height=10)
		self.infos.bind("<Key>", lambda e: "break")
		self.infos.insert(END, 'Select SQL dump file or connect to server')
		self.infos.pack(padx=10, pady=10)
		### Quit button ###
		Button(self,
			text="Quit", command=self.destroy).pack(padx=10, pady=10, side=RIGHT)

	def server_field(self, row, label, default):
		'Field for parameters'
		entry = StringVar(None, default)
		Label(self.frame_server, text=label).grid(
			column=0, row=row, sticky=W, padx=10, pady=10)
		Entry(self.frame_server, textvariable=entry, width=66).grid(
			column=1, row=row, sticky=E, padx=10, pady=10)
		return entry

	def parse(self, source):
		'Get destination'
		if self.filename.get() == '' and source == 'file':
			return
		outdir = askdirectory(
			title = 'Choose directory to write generatde file(s)',
			mustexist=False
		)
		if listdir(outdir):
			showerror('Error', 'Destination directory needs to be emtpy')
			return
		if source == 'file':
			try:
				dumpfile = open(self.filename.get(), 'rt')
				decoder = SQLParser(dumpfile)
			except:
				showerror('Error', 'Could not open file\n' + self.filename.get())
				return
		else:
			try:
				decoder = SQLClient(
					host= self.host.get(),
					user= self.user.get(),
					password = self.password.get(),
					database = self.database.get()
				)
			except:
				showerror('Error', 'Could not connect to MySQL server')
				return
		if self.fileformat.get() == 'csv':
			Writer = Csv
		else:
			Writer = Excel
		self.infos.config(state='normal')
		self.infos.delete(1.0, END)
		self.infos.configure(state='disabled')
		Worker(decoder, Writer, outdir=outdir, info=self.info_handler)

	def info_handler(self, msg):
		'Use logging to show infos'
		def append():
			self.infos.configure(state='normal')
			self.infos.insert(END, msg + '\n')
			self.infos.configure(state='disabled')
			self.infos.yview(END)
		self.infos.after(0, append)

if __name__ == '__main__':	# start here if called as application
	window = Main('''
iVBORw0KGgoAAAANSUhEUgAAAGAAAABgCAMAAADVRocKAAAAw1BMVEUAAAAAAACCgoJCQkLCwsIi
IiKhoaFhYWHi4uISEhKSkpJSUlLR0dExMTGysrJycnLx8fEJCQmJiYlKSkrJyckqKiqpqalpaWnq
6uoZGRmZmZlaWlrZ2dk5OTm6urp6enr6+voFBQWGhoZFRUXFxcUlJSWlpaVmZmbl5eUVFRWVlZVV
VVXW1tY1NTW2trZ2dnb19fUNDQ2Ojo5NTU3Nzc0uLi6tra1ubm7u7u4eHh6dnZ1dXV3e3t4+Pj6+
vr5+fn7///8PfNfhAAAF4UlEQVRo3u2ay26rOhSGEdeJZZAFNtcBYgJCgMFighDk/Z/qLLPbBJLd
lCQ+o3M8bFV/rKv/ZVe7/MtL+y8COOf/CsCbU+QveBoIIcPUplwpwEM4JvradYw5sBjrCJ0VAuaW
rC5sndSiKAyjKETN1nME7dz+erdtbmTNuK0mE4nehooAnh93Ti3k7lYUVVUVRVYwGklscmUAlsj9
g6jq81zL876vosBgsakGwBHtJGC0qlz7WnlfWQWbTDUxKH0c664xWr12W3llGc7gq8miEGoALSSo
tANBegkprORyivI9oY/Gwp1mha0iXPc+0rQqyBLdVNmLzIOPpAmia5U2O6YdozAWLFYKiPNHH4Uq
AcMR0FuN6HyFAJ4cATnkkWMrBLTHPP3jo3VWBpid6Lj/5iOXKis0MlZ3Fmw+IqEaQBpnQdRr9z4y
ktVXAYCWuhqNdW9Cb2Wiw28C9uqBlymc+DZx7mzoo0Y8rbVnAM/7W6BpdhcEwyH8LQAvw7/+4dzd
B0Gf3wJ45Q/ZMdfHINQdes+CHw1Pg2MQ3EW1NqXHSmBYMcAjxyA8a0fviF/TvauEOlYB4NyDFc5t
99AthP4RgM/IN5cWU1iYxo722C2c+Zbb/Cng4feXdKHxMAxxPIC61odYF/cAaYL97T2M7/Skdude
m/r7+kWUgGJ312Gi1B6AQNYHwBaF7ehHE3wJLp8AQjro+mB+F9iM9aQwCuGsU+sjKe90QrpGe3RS
JmLTnGB+WAcaPrMAPtiFD8YIzChxV4CgDgLLSoYWpWixiU7iWM8enFQFTVFvk0k3LE9jgIaOJXJ1
A0kM2N0CQQ2KOg8SlozaTwt06pjBVJKwdQqfZxFdQanDpGFItW5Ffa+dWhvBEIlL0l/StIRZQ4Bn
to/vc+3s6qOgMSBYy6914BMwYfv6qtfOry0MiUv574VmElYbclh6DRBBJjE7PFPJSGfC2ALwAgD0
S+HQ8FyrmG1HyIGvegEApVBjfrYXcSoJwQsAKeQpP9/svDgpsjHKXzHgB/n1Qzf1IdDHme83A4r2
pXbtEcjV4GyY+yxoEvO184AycNJJQk8XBiF+DbDIlnEYvbds/2tUoHqpIfT0JQCibUvqgxiF4T6C
8ntgTDJm0CboS4Ay5XA6JEVzdVMfWePQUnbvtkBmJzKKH4Y17anqWpwt1PDRPbTL2twy+C65tgN/
AUCXvnHom0wS5A0O5MnXyT4c62OTLLEEoHdUBXJlqC0LWtm3cgjZocSd7UdvAy4pk32pycRtjMHW
3oTevPDFgMNmfQ9wMR1hwGG1U4f+uI9C36GUFqAM/i7iTwiv2BFCOLsU8cUeUK0obYWo2eC9CUCu
kzh79Ym6XRD6pg0906mdbnpbm8agZPatDMU3QJ9IJYeYw3641zkDMPV1JxZKn7Krixwc/jGSdTF/
GzDT6x0s9+01uR4UY/vldn9dB/TBrePtnhrpcE5cm8W1+/i2jdQMICGByr5G4Or1FHmqJpxWNLde
9H0E8FDdCGWK5taK2FfoucIHCix2WiAf5ufz7usATgGwq+I45Tycvc8AfHejELbuzkVaP04wmPjl
ZwAP+dcgeqa+B+QRszFe5g8tQAv6tiE0u4Mei4rBpu2HgMuchjtl3BwABgA+teDor6nZn/qRiCk2
Q4WAMs72h36UAMBXCAhbBq3iVgiRIBM2PWUAPredsa+0qHGJrdRF3Nd3zUheFrnDohIAXhpu7VRe
CdYrVgu4lMnVR70EuFQx4NLdAWzFgNn4Bsj5XgK42hjo32dmLsd7kIxUKSBd/+jtXI4LwZgJZ8Uf
AfjhATzE28hQfb2YykdZprefvR/wsJy/ToQZu3KuCiwpt+WbL0hetj5/DDzTrku/xW3bUnmXJB9l
QWw3WWZs90Od/suj7zldhKd40NeOOfV2lbTtLd/dVxK3Sp4aUzoNRO9c5iRJXSewOXw5bU00//pk
fTKLQh/bYIT8twF58Yf9pwf9m2nqlTMc8D6aw1fS+r3b9/8BKtc/NFcHINi4Q8kAAAAASUVORK5C
YII=
	''')
	window.mainloop()
