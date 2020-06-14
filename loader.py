#!/usr/bin/env python

"""
Author: Santiago Andrigo <albionx@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sqlite3

class dataset:

	def __init__(self, databaseName, tableName, creationSql, sourceFile):
		self.databaseName = databaseName
		self.tableName = tableName
		self.creationSql = creationSql
		self.sourceFile = sourceFile

	def setupTable(self):
		print ('Setting up table ' + self.tableName + '...\r',end='')
		with sqlite3.connect(self.databaseName) as database:
			database.execute(self.creationSql)
			database.commit()
		print ('Setting up table ' + self.tableName + ': Complete!')

	def resetTable(self):
		print ('Resetting table ' + self.tableName + '...\r',end='')
		with sqlite3.connect(self.databaseName) as database:
			sql = "DROP TABLE IF EXISTS {};".format(self.tableName)
			database.execute(sql
)			database.commit()
		print ('Resetting table ' + self.tableName + ': Complete!')

	def populateTable(self, batchSize=10000):
		print ('Populating table ' + self.tableName + '...\r',end='')
		self.batchSize = batchSize

		with sqlite3.connect(self.databaseName) as database:
			cursor = database.cursor()
			with open(self.sourceFile) as fileHandle:

				i = int()
				recordsList = list()

				# get schema (and advance the cursor)
				schema = tuple(fileHandle.readline().replace('\n','').split('\t'))
				num_replacements = '?,'*len(schema)

				for line in fileHandle:
					recordsList.append(tuple(line.split('\t')))
					i += 1

					if i == self.batchSize:
						cursor.executemany('''
							INSERT OR IGNORE INTO {} {} VALUES ({})
							'''. format(self.tableName, schema, num_replacements[:-1]), recordsList)
						if cursor.rowcount < len(recordsList):
							print ('Some rows failed to insert')
						recordsList = list() # reset the list
						i = int()
						database.commit()
						continue

				# Write whatever's left
				cursor.executemany('''
					INSERT INTO {} {} VALUES ({})
					'''. format(self.tableName, schema, num_replacements[:-1]), recordsList)
				if cursor.rowcount < len(recordsList):
					print ('Some rows failed to insert')
				database.commit()

		print ('Populating table ' + self.tableName + ': Complete!')

def main():

	# SUB table
	sub = dataset(
		databaseName ='SEC.sqlite',
		tableName ='sub', 
		sourceFile ='2020Q1/sub.txt',
		creationSql = """
		CREATE TABLE IF NOT EXISTS sub (
		    adsh TEXT(20) NOT NULL,
		    cik INTEGER(10) NOT NULL,
			name TEXT(150) NOT NULL,
			sic INTEGER(4),
			countryba TEXT(2) NOT NULL,
			stprba TEXT(2),
			cityba TEXT(30) NOT NULL,
			zipba TEXT(10),
			bas1 TEXT(40),
			bas2 TEXT(40),
			baph TEXT(20),
			countryma TEXT(2),
			stprma TEXT(2),
			cityma TEXT(30),
			zipma TEXT(10),
			mas1 TEXT(40),
			mas2 TEXT(40),
			countryinc TEXT(3) NOT NULL,
			stprinc TEXT(2),
			ein INTEGER(10),
			former TEXT(150),
			changed TEXT(8),
			afs TEXT(5),
			wksi BOOLEAN NOT NULL,
			fye TEXT(4) NOT NULL,
			form TEXT(10) NOT NULL,
			period DATE(8) NOT NULL,
			fy TEXT(4) NOT NULL,
			fp TEXT(2) NOT NULL,
			filed DATE(8) NOT NULL,
			accepted DATE(19) NOT NULL,
			prevrpt BOOLEAN NOT NULL,
			detail BOOLEAN NOT NULL,
			instance TEXT(32) NOT NULL,
			nciks INTEGER(4) NOT NULL,
			aciks TEXT(120),
			PRIMARY KEY (adsh)
		);
		"""
	)
	sub.resetTable()
	sub.setupTable()
	sub.populateTable(batchSize = 100)

	# Tag table
	tag = dataset(
		databaseName ='SEC.sqlite',
		tableName ='tag', 
		sourceFile ='2020Q1/tag.txt',
		creationSql = """
		CREATE TABLE IF NOT EXISTS tag (
		    tag TEXT(256) NOT NULL,
		    version TEXT(20) NOT NULL,
		    custom BOOLEAN NOT NULL,
		    abstract BOOLEAN NOT NULL,
		    datatype TEXT(20),
		    iord TEXT(1) NOT NULL,
		    crdr TEXT(1),
		    tlabel TEXT(512),
		    doc TEXT,
		    PRIMARY KEY (tag, version)
		);
		"""
	)
	tag.resetTable()
	tag.setupTable()
	tag.populateTable(batchSize = 100)

	# Num table
	num = dataset(
		databaseName ='SEC.sqlite',
		tableName ='num', 
		sourceFile ='2020Q1/num.txt',
		creationSql = """
		CREATE TABLE IF NOT EXISTS num (
		    adsh TEXT(20) NOT NULL,
		    tag TEXT(256) NOT NULL,
		    version TEXT(20) NOT NULL,
		    ddate DATE(8) NOT NULL,
		    qtrs INTEGER(8) NOT NULL,
		    uom TEXT(20) NOT NULL,
		    coreg TEXT(256),
		    value NUMERIC(16),
		    footnote TEXT(512),
		    PRIMARY KEY (adsh, tag, version, ddate, qtrs, uom, coreg)
		);
		"""
	)
	num.resetTable()
	num.setupTable()
	num.populateTable(batchSize = 100)

	# Pre table
	pre = dataset(
		databaseName ='SEC.sqlite',
		tableName ='pre', 
		sourceFile ='2020Q1/pre.txt',
		creationSql = """
		CREATE TABLE IF NOT EXISTS pre (
		    adsh TEXT(20) NOT NULL,
		    report INTEGER(6) NOT NULL,
		    line INTEGER(6) NOT NULL,
		    stmt TEXT(2) NOT NULL,
		    inpth BOOLEAN NOT NULL,
		    rfile TEXT(1) NOT NULL,
		    tag TEXT(256),
		    version TEXT(20),
		    plabel TEXT(512),
		    negating TEXT,
		    PRIMARY KEY (adsh, report, line)
		);
		"""
	)
	pre.resetTable()
	pre.setupTable()
	pre.populateTable(batchSize = 100)

if __name__ == '__main__':
	main()