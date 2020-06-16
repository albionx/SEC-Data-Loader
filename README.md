# SEC Data Loader

This script processes the .txt files from the SEC's Financial Statement Data Sets (https://www.sec.gov/dera/data/financial-statement-data-sets.html) and loads them into respective SQLite Tables, for appropriate querying. The actual data is expressed in the XBRL format, allowing you to build custom reports of the financial data reported by companies to the SEC.

1. Submissions (Sub)
2. Tags (Tags)
3. Numbers (Num)
4. Presentation of Statements (Pre)

An explanation of the schema of these tables is present here:
https://www.sec.gov/files/aqfs.pdf

## Features
- Filter by Company name
- Filter by Filing type (10-K, 10-Q, 8-K)

## To Do
- Add support for iterating through files of various quarters
- Add support for filtering the data to only certain years