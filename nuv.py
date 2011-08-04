#! /usr/bin/python

def dic(a):
	lista = a.split('\t')
	if(divice.has_key(lista[0])):
		return
	divice[lista[0]]=1	
	key = lista[1] + "_" + lista[2]
	count = dicd.get(key,0)
	dicd[key] = count+1
	return dicd

#def fileTodic():
#	while(myfile.readline())
#		dic(

divice = {}
dicd = {}
myfile = open('data','r')
try:
	for line in myfile:
		print line
		dic(line.rstrip())
finally:
	myfile.close()
print dicd
#stra = "trainid	source	nuvtotal"
#dic(stra)
#stra = "10	appstore	2"
#dic(stra)
#dic(stra)
#stra = "7	moto	70"
#print dic(stra)
