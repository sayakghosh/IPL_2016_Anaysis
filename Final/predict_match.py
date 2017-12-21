
import csv
import random

team1_bat_order = []
team1_bow_order = []
team2_bat_order = []
team2_bow_order = []


score_list = [0, 1, 2, 3, 4, 6]


match_no = input("Enter match no.")
# Extraction of squads from the CSV and storing them in respective lists
with open('Input/InputMatch%s.csv'%match_no, "rt") as f:
    match_reader = csv.reader(f)
    next(match_reader)
    for row in match_reader:
        team1_bat_order.append(row[0])
        team1_bow_order.append(row[1])
        team2_bat_order.append(row[2])
        team2_bow_order.append(row[3])

team1_bat_order = [x for x in team1_bat_order if x != '']
team1_bow_order = [x for x in team1_bow_order if x != '']
team2_bat_order = [x for x in team2_bat_order if x != '']
team2_bow_order = [x for x in team2_bow_order if x != '']

team1_bow_order = team1_bow_order[:5]# Restricting to 5 bowlers
team2_bow_order = team2_bow_order[:5]# Restricting to 5 bowlers

# Finding which cluster the batsman and bowler belong to.
def cluster_number(batsman, bowler) :

      #  batsman's cluster number
	with open('Data/Batting_Clusters.csv', 'rt') as f:
	    bat_cluster_reader = csv.reader(f)
	    for row in bat_cluster_reader:
	    	if batsman == row[0]:
	    		curr_bat_cluster_num = row[4]


    #  bowler's cluster number
	with open('Data/Bowling_Clusters.csv', 'rt') as f:
	    bow_cluster_reader = csv.reader(f)
	    for row in bow_cluster_reader:
	    	if bowler == row[0]:
	    		curr_bow_cluster_num = row[4]
	#print(batsman)
	return curr_bat_cluster_num, curr_bow_cluster_num


# Get the corresponding row from PvP Probabilites file
def pvp_plist(batsman, bowler) :
	check = False
	with open('Data/PvPProbabilities.csv', 'rt') as f:
		pvp_reader = csv.reader(f)
		for row in pvp_reader:
		    if batsman == row[0] and bowler == row[1]:
			    check = True
			    prob_list = row[2:9]
			    break

	if check :
		prob_list = list(map(float, prob_list))
		#probs_list = probs_list[2:9]
		return check,prob_list
	else :
		return check,None
# Get the corresponding row from cvc Probabilites file for non-existent combos
def cvc_plist(bat_cluster_number, bowler_cluster_number) :

	with open('Data/CVCProbabilities.csv', 'rt') as f:
	    cvc_reader = csv.reader(f)
	    for row in cvc_reader:
	    	if bat_cluster_number == row[0] and bowler_cluster_number == row[1]:
	    		prob_list = row

	#print(bat_cluster_number, bowler_cluster_number)

	prob_list = list(map(float, prob_list))
	prob_list = prob_list[2:]

	return prob_list


#Run predictor
def random_pick(some_list, probabilities) :

	#print(probabilities[6])
	del(probabilities[6])
	x = random.uniform(0,sum(probabilities))
	cumulative_probability = 0.0
	for item, item_probability in zip(some_list, probabilities):
		cumulative_probability += item_probability
		if x < cumulative_probability: break
	return item


#Computing every ball in an innings
def innings(bat_order, bow_order, inn) :

	tot_wickets = 0
	striker = 1
	non_striker = 0
	striker_notout = 1
	non_striker_notout = 1

	bow_index_order = [0,1,0,1,2,3,4,2,3,4,2,3,4,2,3,4,0,1,0,1]#bowling order
	x = bow_index_order[0]

	total_runs = 0
	k = -1
	print(bat_order[non_striker].rstrip())
	print(bat_order[striker].rstrip())
	for i in range(0,120) :
        # Swap batsmen and Change bowlers after every over
		if i%6 == 0 :
			k += 1
			x = bow_index_order[k]

			tmp_striker = striker
			tmp_notout = striker_notout
			striker = non_striker
			striker_notout = non_striker_notout
			non_striker = tmp_striker
			non_striker_notout = tmp_notout

		curr_bat = bat_order[striker].rstrip() #Striker
		#print(curr_bat)
		other_bat = bat_order[non_striker].rstrip()# Non Striker
		#print(other_bat)
		curr_bow = bow_order[x].rstrip()

        #Prediction
		exists, pvp_p_list = pvp_plist(curr_bat, curr_bow)
		if exists :
			striker_notout *= float(1 - (pvp_p_list[6]))
			prediction = random_pick(score_list, pvp_p_list)
			#print(pvp_p_list[-1])
			#m_notout *= float(1 - (pvp_p_list[6]))
		else :
			#print(curr_bow)
			bat_c_num, bow_c_num = cluster_number(curr_bat, curr_bow)
			cvc_p_list = cvc_plist(bat_c_num, bow_c_num)
			striker_notout *= float(1 - (cvc_p_list[6]))
			prediction = random_pick(score_list, cvc_p_list)
			#print(cvc_p_list[6])
			#m_notout *= float(1 - (cvc_p_list[6]))

        #If out
		if striker_notout<0.4 :
			tot_wickets+=1
			striker=max(striker,non_striker) + 1
			print(bat_order[striker].rstrip())
			striker_notout = 1

			if striker > 10 :
				break

		elif prediction==0 or prediction==2 or prediction==4 or prediction==6:
			total_runs+=prediction


		elif prediction==1 or prediction==3:
			total_runs+=prediction
			tmp_striker = striker
			tmp_notout = striker_notout
			striker = non_striker
			striker_notout = non_striker_notout
			non_striker = tmp_striker
			non_striker_notout = tmp_notout



		if inn == 1 :
			global first_inn_score
			first_inn_score = total_runs

        # If it is second innings and if the team has chased down the target
		if inn == 2 and total_runs > first_inn_score :
			break


	num_of_overs_played = str(int((i+1)/6)) + "." + str((i+1)%6)
	return total_runs, tot_wickets, str(total_runs)+"/"+str(tot_wickets)+" Overs : "+ num_of_overs_played


# MAIN
first_innings_score, wickets1, formatted_score1 = innings(team1_bat_order, team2_bow_order, 1)
print ("Team 1 Score : " + formatted_score1)

second_innings_score, wickets2, formatted_score2 = innings(team2_bat_order, team1_bow_order, 2)
print ("Team 2 Score : " + formatted_score2)

if first_innings_score > second_innings_score :
	print ("Team 1 wins!")
	print ("By "+str(first_innings_score - second_innings_score)+" runs")
elif second_innings_score > first_innings_score :
	print ("Team 2 wins!")
	print ("By "+ str(10 - wickets2) +" wickets")
else :
	print ("Match Tied.")
