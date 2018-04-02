# EDGAR log sessionization

A python code to sessionize EDGAR user login data.

## Prerequisites

Python 2.7.14, 

SQLite 3 (a part of standard Python 2.7 distribution)

## Running the Tests

run the bash file by typing $ ./run.sh

## Download more log files
By modifying and running DownloadLog.py, one can get more log files from www.sec.gov .

## Running time tests

This code simulates real-time streaming data by reading the input file line by line. Reading the whole input file directly into RAM and creating sessions can be much more faster but not scalable. 

There are possibly better ways to insert or update rows ( maybe by preparing a large transaction list or by using multi-threads with multiple connections). 

The running time on 2017/01/10 log csv file (from https://www.sec.gov/dera/data/edgar-log-file-data-set.html) is 1031.97970319s. The test is done locally with my mac book pro 13. The file storage size is about 2.2 GB. The inactive period time used is 10s.
