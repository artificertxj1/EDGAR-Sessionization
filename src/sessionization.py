from __future__ import with_statement
import os
import sys
import argparse
import errno
import csv
import datetime as dt
import sqlite3
import time

#import pandas as pd 
if len(sys.argv)<4:
	sys.stderr.write("Error: log generator needs 3 arguments: \n input data filepath, inactive session filepath, output data filepath\n")
	sys.exit()
logFilePath=sys.argv[1]
inactFilePath=sys.argv[2]
outputFilePath=sys.argv[3]

class generator():

	def __init__(self,logFilePath, outputFilePath, inactTime, dbConn):
		self.logFilePath=logFilePath
		self.outputFilePath=outputFilePath
		self.inactTime=inactTime
		self.dbConn=dbConn
	#######################################

	def streamer(self,writeChunkSize=5000):  ## dbConn is a sqlite connection object
		curTime=None
		chunk=""
		chunkSize=0
		cursor=self.dbConn.cursor()
		with open(self.outputFilePath,"w") as outputFile:
			for row in self.readLine(self.logFilePath):
				if row[1]!=curTime:
					 ##current time
					curTime=row[1]
					##find all inactive session
					cursor.execute('SELECT * FROM ActSess WHERE (julianday(?)-julianday(LastLogIn))*86400.0 >= ?',(curTime,self.inactTime+1.0))
					endedSession=cursor.fetchall()
					for line in endedSession:
						line=list(line)
						startTime,endTime=line[1],line[2]
						line.insert(3,int((endTime-startTime).total_seconds()+1))
						#print(str(line[0]), str(line[1]),str(line[2]),line[3],line[4])
						newLine="%s,%s,%s,%s,%s\n"%(str(line[0]), str(line[1]),str(line[2]),str(line[3]),str(line[4]))
						chunk=chunk+newLine
						chunkSize+=1
						if chunkSize>=writeChunkSize:
							outputFile.write(chunk)
							chunk=""
							chunkSize=0
					## delete all inactive session
					cursor.execute('DELETE FROM ActSess WHERE (julianday(?)-julianday(LastLogIn))*86400.0 >= ?',(curTime,self.inactTime+1.0))
				## insert or update new results 
				## I think there is a possible one-liner to do this using INSERT or REPLACE
				cursor.execute('SELECT * FROM ActSess WHERE IP = ?',(row[0],))
				exists=cursor.fetchall()
				if len(exists)==0:
					cursor.execute('INSERT INTO ActSess VALUES(?,?,?,?)', (row[0],row[1],row[1],1))
				else:
					cursor.execute('UPDATE ActSess SET LastLogIn=?, RequestCnt=RequestCnt+? WHERE IP = ?', (row[1],1,row[0]))
			## after we reach the end of the file
			cursor.execute("SELECT * FROM ActSess") ## find all remaining session
			endedSession=cursor.fetchall()
			for line in endedSession:
				line=list(line)
				startTime,endTime=line[1],line[2]
				line.insert(3,int((endTime-startTime).total_seconds()+1))
				#print(str(line[0]), str(line[1]),str(line[2]),line[3],line[4])
				newLine="%s,%s,%s,%s,%s\n"%(str(line[0]),str(line[1]),str(line[2]),str(line[3]),str(line[4]))
				chunk=chunk+newLine
				if chunkSize>=writeChunkSize:
					outputFile.write(chunk)
					chunk=""
					chunkSize=0
			outputFile.write(chunk)
	###########################################

	def time_tango(self,date,time):
		return dt.datetime.strptime("{} {}".format(date,time),"%Y-%m-%d %H:%M:%S")
	
	###########################################
	def readLine(self,fileName,skipHeader=True):
		if not fileName.endswith('.csv'):
			sys.stderr.write("Error: can only take csv files\n")
			sys.exit()
		with open(fileName,"rb") as csvfile:
			datareader=csv.reader(csvfile)
			if skipHeader:
				next(datareader,None)
			for row in datareader:
				if not row:
					yield row
					break
				date,time=row[1],row[2]
				row[1:2]=[self.time_tango(date,time)]
				#row[8]=int(float(row[8]))
				#row[9]=int(float(row[9]))
				yield row		

################################################
def main():
	if not os.path.exists(logFilePath):
		sys.stderr.write("Error: input file not found\n")
		sys.exit()
	if not os.path.exists(inactFilePath):
		sys.stderr.write("Error: inactive session file not found\n")
		sys.exit()
	if not os.path.exists(os.path.dirname(outputFilePath)):
		try:
			os.makedirs(os.path.dirname(outputFilePath))
		except OSError as ext:
			if ext.errno !=errno.EEXIST:
				raise
	# read the inactive time gap
	inactTimeFile=open(inactFilePath,'r')
	inactTime=float(inactTimeFile.read())
	#print('inactive time gap is %f'%(inactTime))
	
	# try to create in memory sqlite database
	try:
		dbConn=sqlite3.connect(":memory:",detect_types=sqlite3.PARSE_DECLTYPES)  ## in memory database
	except sqlite3.Error as err:
		print('SqLite3 Error: %s \n'%(err.message))
	cursor=dbConn.cursor()
	## create a in memory table to keep all active sessions
	try:
		cursor.execute('CREATE TABLE IF NOT EXISTS ActSess(IP text, FirstLogIn timestamp, LastLogIn timestamp, RequestCnt integer)')
	except sqlite3.Error as err:
		print('SqLite3 Error: %s \n'%(err.message))
	## make log generator object
	gen=generator(logFilePath,outputFilePath,inactTime,dbConn)	
	start_time=time.time()
	gen.streamer()
	end_time=time.time()
	print("Streaming ended. \n Total time elapsed: %s s"%(end_time-start_time))
	dbConn.close()


if __name__=='__main__':
	main()
