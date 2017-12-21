import csv

cvcwrite=open('PvPProbabilities.csv',"w")

cvcwriter=csv.writer(cvcwrite)
header=["Batsman","Bowler","Probability of 0","Probability of 1","Probability of 2","Probability of 3","Probability of 4","Probability of 6","Probability of W","Balls Faced"]
cvcwriter.writerow(header)

with open('/Users/sayakghosh/Desktop/PES/Sem5/BigData/Project/IPL/week4/PlayerVsPlayer1.csv',"r") as cvcread:
	for line in cvcread:
		row=line.split(',')
		row1=[]
		row1.append(row[0])
		row1.append(row[1])
		row1.append(float(int(row[3])/int(row[2])))
		row1.append(float(int(row[4])/int(row[2])))
		row1.append(float(int(row[5])/int(row[2])))
		row1.append(float(int(row[6])/int(row[2])))
		row1.append(float(int(row[7])/int(row[2])))
		row1.append(float(int(row[8])/int(row[2])))
		row1.append(float(int(row[9])/int(row[2])))
		row1.append(row[2])
		cvcwriter.writerow(row1)

cvcwrite.close()
