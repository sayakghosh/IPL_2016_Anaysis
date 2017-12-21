import urllib.request
from bs4 import BeautifulSoup
import re
import csv

stats=csv.writer(open('stats-bat.csv','w'))
stats.writerow(['country_id','country_name','player_id','player_name','runs_made','average','SR','sixes','fours','fifties','hundreds','not_out','batinnings','matches'])

def get_country_id():
	root_url='http://www.espncricinfo.com/indian-premier-league-2016/content/squad/index.html?object=968923'
	root_response=urllib.request.urlopen(root_url)
	root_html=root_response.read().decode('utf-8')
	root_soup=BeautifulSoup(root_html, "html.parser")
	country_links= root_soup.findAll('a',href=re.compile('indian-premier-league-2016/content/squad/'))
	for i in country_links:
		country_name=i.text
		country_id=i.get('href').split('/')[-1].split('.')[0]
		print (country_id ,'--',country_name)
		#if country_id == "index": continue

		get_country_squad(str(country_id),str(country_name))

def get_country_squad(country_id,country_name):
	url='http://www.espncricinfo.com/indian-premier-league-2016/content/squad/'+str(country_id)+'.html'
	response = urllib.request.urlopen(url)
	html = response.read().decode('utf-8')
	soup = BeautifulSoup(html, "html.parser")
	image_links=soup.findAll('a', href=re.compile('/indian-premier-league-2016/content/player/'))
	for i in image_links:
		if i.text:
			player_id=i.get('href').split('/')[-1].split('.')[0]
			player_name=i.text.strip()
			statsf(country_id,country_name,player_id,player_name)


def statsf(country_id,country_name,player_id,player_name):
	print (player_name)
	#print ('Bowling ...')
	#(bowlinnings,balls,wickets,runsgiven,fourw,fivew) = stat_scraper(country_id,country_name,player_id,player_name,'bowling')
	print ('Batting ...')
	(runs_made,ave,sr,sixes,fours,fifties,hundreds,not_out,batinnings,matches) = stat_scraper(country_id,country_name,player_id,player_name,'batting')
	stats.writerow([country_id,country_name,player_id,player_name,runs_made,ave,sr,sixes,fours,fifties,hundreds,not_out,batinnings,matches])
	print ('Done')

def stat_scraper(country_id,country_name,player_id,player_name,action):
	year=2016
	if action == "batting":
		url='http://www.espncricinfo.com/indian-premier-league-2016/content/player/'+str(player_id)+'.html#bataves'
	else:
		url='http://www.espncricinfo.com/indian-premier-league-2016/content/player/'+str(player_id)+'.html#bowlaves'
	response = urllib.request.urlopen(url)
	html = response.read().decode('utf-8')
	soup = BeautifulSoup(html, "html.parser")
	array=(4,6,8,12,11,10,9,3,2,1)
	try:
		year_data = soup.findAll(text='T20s')[0].findParents("tr")[0].findAll("td")
	except IndexError:
		pass
	results=[]
	for i in array:
		try:
				results.append(float(year_data[i].get_text()))
		except:
				results.append(0)
	print (action,'----',results)
	return results

get_country_id()
