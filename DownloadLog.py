import os
from urllib2 import urlopen, URLError, HTTPError
import zipfile
import sys

url="http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170101.zip"

if len(sys.argv)<2:
	downloadFilePath='./input'
else:
	downloadFilePath=sys.argv[1]

def main():
	filename=os.path.basename(url)
	name=os.path.join(downloadFilePath,filename)
	try:
		f = urlopen(url)
        	print "downloading " + url
		with open(name, "wb") as local_file:
           		local_file.write(f.read())
		#name,hdrs=urllib.urlretrieve(url,name)
	#except IOError, e:
	#	sys.stderr.write('Error: cannot retrive %r to %r: %s'%(url, downloadFilePath,e))
	#	sys.exit()
	except HTTPError, e:
        	print("HTTP Error:", e.code, url)
		sys.exit()
    	except URLError, e:
        	print("URL Error:", e.reason, url)
		sys.exit()

	try:
		z=zipfile.ZipFile(name)
	except zipfile.error, e:
		sys.stderr.write('Error: bad zipfile (from %r): %s'%(url,e))
		sys.exit()
	for n in z.namelist():
    		dest = os.path.join(downloadFilePath, n)
    		destdir = os.path.dirname(dest)
    		if not os.path.isdir(destdir):
      			os.makedirs(destdir)
    		data = z.read(n)
    		f = open(dest, 'w')
    		f.write(data)
    		f.close()
  	z.close()
  	os.unlink(name)

if __name__=='__main__':
	main()
