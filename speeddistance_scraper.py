import requests
import json

import psycopg2
from psycopg2.extensions import AsIs

conn = psycopg2.connect("dbname=nbadata user=postgres")
cur = conn.cursor()

headers = {'Cookie' : 'ASP.NET_SessionId=bf4bdsf5vqkauv3hquiydwzj; globalUserOrderId=Id=; bSID=Id=d4dc678a-49f5-42de-8635-e6d783c55f50; s_fuid=17691613676678691303211417123336815876; s_pers=%20s_last_team%3DPhiladelphia%252076ers%7C1503958884858%3B%20productnum%3D2%7C1475015130930%3B; rr_rcs=eF4FwbENgDAMBMCGil1ewrHfSTZgDewEiYIOmJ-7ZXlzN0Y6taMFHTZbwGNMJMmwPOJ0W-_vucYm4g1itVhRUaldQAXkB673EiE; s_cc=true; s_fid=188D52E3F63005CA-2C913EDBDE190504; s_sq=nbag-n-76ers%3D%2526pid%253Dwww.nba.com%25253A%25252Fsixers%25252Fschedule%2526pidt%253D1%2526oid%253Dhttp%25253A%25252F%25252Fwww.nba.com%25252Fsixers%25252Fschedule%252523game-block-overview%2526ot%253DA', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'}

players_url = 'http://stats.nba.com/stats/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=2015-16'


url = 'http://stats.nba.com/stats/playerdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=PerGame&Period=0&PlayerID=201166&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision='

# r = requests.get(url, headers=headers)
# json_response = json.loads(r.text)
# print(json_response['resultSets'])


# tracking_types = ['CatchShoot', 'Defense', 'Drives', 'Passing', 'PullUpShot', 'Rebounding', 'Efficiency', 'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch']


# tracking_type = 'Possessions'
# league_tracking_url = 'http://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=%s&Season=2013-14&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=' % tracking_type

# r = requests.get(league_tracking_url, headers=headers)
# json_response = json.loads(r.text)

# rows = json_response['resultSets'][0]['rowSet']

# for row in rows:
#     indices = [0, 1] + range(9, 21)
#     parsed_row = [row[i] for i in indices] + ['2013-14']
#     cur.execute('INSERT INTO players_possessions (player_id, player_name, touches, front_ct_touches, time_of_poss, avg_sec_per_touch, avg_drib_per_touch, pts_per_touch, elbow_touches, post_touches, paint_touches, pts_per_elbow_touch, pts_per_post_touch, pts_per_paint_touch, season) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', tuple(parsed_row))
#     conn.commit()

# cur.close()
# conn.close()


tracking_type = 'SpeedDistance'
league_tracking_url = 'http://stats.nba.com/stats/leaguedashptstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOrTeam=Player&PlayerPosition=&PtMeasureType=%s&Season=2013-14&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=' % tracking_type

r = requests.get(league_tracking_url, headers=headers)
json_response = json.loads(r.text)
print(json_response['resultSets'][0]['headers'])
print(len(json_response['resultSets'][0]['headers']))
rows = json_response['resultSets'][0]['rowSet']
schema = json_response['resultSets'][0]['headers']

for row in rows:
    # print(row)
    indices = [0, 1] + range(9, 22)
    parsed_row = [row[i] for i in indices] + ['2013-14']
    # relevant_schema = str([str(schema[i]).lower() for i in indices]).replace('[', '(').replace(']', ')').replace('\'', '')
    relevant_schema = [AsIs(str(schema[i]).lower()) for i in indices] + [AsIs('season')]
    # print(parsed_row)
    # print(len(relevant_schema), len(parsed_row))
    cur.execute('INSERT INTO players_efficiency (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', tuple(relevant_schema + parsed_row))
    conn.commit()

cur.close()
conn.close()

