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
try:
    from PyInquirer import style_from_dict, Token, prompt
except ModuleNotFoundError:
    print('PyInquirer missing. To install, please run: pip install PyInquirer')
    quit()
try:
    from tqdm import tqdm
except ModuleNotFoundError:
    print('TQDM missing. To install, please run: pip install TQDM')
    quit()

class dataset:

    def __init__(self, databaseName, tableName, creationSql, filename):
        self.databaseName = databaseName
        self.tableName = tableName
        self.creationSql = creationSql
        self.filename = filename

    def setupTable(self):
        with sqlite3.connect(self.databaseName) as database:
            database.execute(self.creationSql)
            database.commit()

    def resetTable(self):
        with sqlite3.connect(self.databaseName) as database:
            sql = "DROP TABLE IF EXISTS {};".format(self.tableName)
            database.execute(sql)
            database.commit()

    def populateTable(self, fileLocation, adshList=None, filings=None, tagVersionList=None):

        with sqlite3.connect(self.databaseName) as database:
            cursor = database.cursor()

            with open(fileLocation + self.filename) as fileHandle:

                recordsList = list()

                # get schema (while advancing the cursor) and locate the relevant fields for filtering
                schema = tuple(fileHandle.readline().replace('\n', '').split('\t'))
                num_replacements = str('?,' * len(schema))[:-1]

                # Load relevant lines in memory

                # For the Num, Pre and Sub tables, it's only the rows where the filing matches the company, year and form required by the user
                if self.tableName in ['num', 'pre', 'sub']:
                    adsh_position = schema.index('adsh')
                    for line in fileHandle:
                        parsed_line = (line.split('\t'))
                        if parsed_line[adsh_position] in adshList:
                            recordsList.append(parsed_line)

                # For the Tag table, it's only the rows where the 'tag' and 'version' matches that of the lines in the 'pre' statement for a particular filing
                # The method could be kept slighly cleaner if I used a subclass for the tag table, but it seems more complicated than the current approach
                elif self.tableName in ['tag']:
                    tag_position = schema.index('tag')
                    version_position = schema.index('version')
                    for line in fileHandle:
                        parsed_line = (line.split('\t'))
                        if (parsed_line[tag_position], parsed_line[version_position]) in tagVersionList:
                            recordsList.append(parsed_line)

                # drop them into the DB
                cursor.executemany('''
                    INSERT OR IGNORE INTO {} {} VALUES ({})
                    '''. format(self.tableName, schema, num_replacements), recordsList)
                database.commit()


def obtainAdshList(name, filings, fileLocations):
    """ Scans the Submissions files to find the ADSH keys corresponding to the filings selected """

    adshList = list()

    for fileLocation in tqdm(fileLocations, desc='Obtaining filing IDs:         ', bar_format='{desc}{bar}| {percentage:3.0f}% [Duration: {elapsed}]'):

        with open(fileLocation + 'sub.txt') as fileHandle:

            # read schema and advance one line
            schema = tuple(fileHandle.readline().replace('\n', '').split('\t'))
            name_position = schema.index('name')
            adsh_position = schema.index('adsh')
            filing_position = schema.index('form')

            # scan for the appropriate adsh's
            for line in fileHandle:
                parsed_line = line.split('\t')
                if parsed_line[name_position].lower() == name.lower() and parsed_line[filing_position] in filings:
                    adshList.append(parsed_line[adsh_position])

    return adshList


def userInput():
    """ Retrieves the user's choice of company, filing type, year and preference for handling of the data in the database """

    style = style_from_dict({
        Token.Separator: '#6C6C6C',
        Token.QuestionMark: '#FF9D00 bold',
        Token.Selected: '#5F819D',
        Token.Pointer: '#FF9D00 bold',
        Token.Answer: '#5F819D bold',
    })

    questions = [
        {
            'type': 'input',
            'name': 'name',
            'message': 'Which company should we look into?'
        },
        {
            'type': 'checkbox',
            'name': 'years',
            'message': 'Which years should we pull from?',
            'choices': [
                {'name': '2009', 'checked': True}, {'name': '2010', 'checked': True}, {'name': '2011', 'checked': True},
                {'name': '2012', 'checked': True}, {'name': '2013', 'checked': True}, {'name': '2014', 'checked': True},
                {'name': '2015', 'checked': True}, {'name': '2016', 'checked': True}, {'name': '2017', 'checked': True},
                {'name': '2018', 'checked': True}, {'name': '2019', 'checked': True}, {'name': '2020', 'checked': True}
            ],
            'when': lambda answers: answers.get('name', '') != ''
        },
        {
            'type': 'checkbox',
            'name': 'filings',
            'message': 'Ok... which filings should we import?',
            'choices': [{'name': '10-K', 'checked': True}, {'name': '10-Q', 'checked': False}, {'name': '8-K', 'checked': False}],
            'when': lambda answers: answers.get('years', []) != []
        },
        {
            'type': 'list',
            'name': 'dataHandling',
            'message': 'Finally... should we append to the database or wipe it clean before populating?',
            'default': 'append',
            'choices': ['append', 'wipe and start again'],
            'when': lambda answers: answers.get('filings', []) != []
        }
    ]
    print('')
    answers = prompt(questions, style=style)
    print('')
    return answers


def initializeDatasets():
    """ Instantiates the objects corresponding to each dataset, using the 'dataset' class. """

    sub = dataset(
        databaseName='SEC.sqlite',
        tableName='sub',
        filename='sub.txt',
        creationSql="""
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

    # Tag table object

    tag = dataset(
        databaseName='SEC.sqlite',
        tableName='tag',
        filename='tag.txt',
        creationSql="""
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

    # Numbers table object

    num = dataset(
        databaseName='SEC.sqlite',
        tableName='num',
        filename='num.txt',
        creationSql="""
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

    # Presentations table object

    pre = dataset(
        databaseName='SEC.sqlite',
        tableName='pre',
        filename='pre.txt',
        creationSql="""
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

    return [sub, tag, num, pre]


def getFileLocations(years, filings):
    """ Returns a list with the folder names where the data will be found """

    fileLocations = list()
    for year in years:
        if filings == ['10-K']:
            fileLocations.append(year + 'q1/')  # only valid if the user wants solely 10-K's as they are filed in Q1
        else:
            fileLocations.append(year + 'q1/')
            fileLocations.append(year + 'q2/')
            fileLocations.append(year + 'q3/')
            fileLocations.append(year + 'q4/')
    return fileLocations


def obtainTagVersionList(adshList, fileLocation):
    """ Scans the Pre file to find the Tags and the Versions used in the filing in question so as to populate the Tags table with only that subset """

    with open(fileLocation + 'pre.txt') as fileHandle:

        tagVersionList = list()

        # read schema and advance one line
        schema = tuple(fileHandle.readline().replace('\n', '').split('\t'))
        adsh_position = schema.index('adsh')
        tag_position = schema.index('tag')
        version_position = schema.index('version')

        # scan for the appropriate adsh's and create a tuple
        for line in fileHandle:
            parsed_line = line.split('\t')
            if parsed_line[adsh_position] in adshList:
                tagVersionList.append((parsed_line[tag_position], parsed_line[version_position]))

    return tagVersionList

def main():

    # Get the user input decision
    userRequest = userInput()
    if userRequest.get('name', '') == '':
        quit()

    # Initialize the classes
    [sub, tag, num, pre] = initializeDatasets()

    # Set up the data structure holding the file locations to look into
    fileLocations = getFileLocations(years=userRequest['years'], filings=userRequest['filings'])

    # Obtain the adshes from the Sub dataset
    adshList = obtainAdshList(name=userRequest['name'], filings=userRequest['filings'], fileLocations=fileLocations)
    if adshList == []:
        print('Could not find a filing under the company name {} with filings {}'.format(userRequest['name'], userRequest['filings']))
        quit()

    # If the user requested it, start by wiping the dataset clean
    if userRequest.get('dataHandling') == 'wipe and start again':
        for dataset in tqdm([sub, tag, num, pre], desc='Wiping clean existing tables: ', bar_format='{desc}{bar}| {percentage:3.0f}% [Duration: {elapsed}]'):
            dataset.resetTable()

    # Processing
    with tqdm(total=100 * len(fileLocations), bar_format='{desc}  {bar}| {percentage:3.0f}% [Duration: {elapsed}]') as progressBar:

        for fileLocation in fileLocations:

            # Sub table
            progressBar.set_description('Populating table sub from {}'.format(fileLocation))
            sub.setupTable()
            sub.populateTable(adshList=adshList, filings=userRequest['filings'], fileLocation=fileLocation)
            progressBar.update(25)

            # Pre table
            progressBar.set_description('Populating table pre from {}'.format(fileLocation))
            pre.setupTable()
            pre.populateTable(adshList=adshList, fileLocation=fileLocation)
            tagVersionList = obtainTagVersionList(adshList=adshList, fileLocation=fileLocation)
            progressBar.update(25)

            # Tag table
            progressBar.set_description('Populating table tag from {}'.format(fileLocation))
            tag.setupTable()
            tag.populateTable(adshList=adshList, fileLocation=fileLocation, tagVersionList=tagVersionList)
            progressBar.update(25)

            # Num table
            progressBar.set_description('Populating table num from {}'.format(fileLocation))
            num.setupTable()
            num.populateTable(adshList=adshList, fileLocation=fileLocation)
            progressBar.update(25)


if __name__ == '__main__':
    main()
