# EDGAR log sessionization

A python code to sessionize EDGAR user login data.

## Prerequisites

Python 2.7.14
SQLite 3 (a part of standard Python 2.7 distribution)

## Running the Tests

run the bash file by typing $ ./run.sh

## Running time tests
The running time on 2017/01/10 log csv file (from https://www.sec.gov/dera/data/edgar-log-file-data-set.html) is 1031.97970319s. The test is done locally with my mac pro book 13 (2017). The file storage size is about 2.2 GB. The inactive period time used is 10s.
