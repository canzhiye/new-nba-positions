import requests
import json
import datetime

import psycopg2
from psycopg2 import DataError, IntegrityError

conn = psycopg2.connect("dbname=nbadata user=postgres")
cur = conn.cursor()

# 2014, 2015 seasons
# headers = {
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
#     'Host': 'data.nba.com',
#     'Accept-Encoding': 'gzip, deflate, sdch',
#     'Accept-Language': 'en-US,en;q=0.8',
#     'Accept': '*/*',
#     'Origin': 'http://www.nba.com'
# }

# date = datetime.datetime(2014, 10, 28)

# for i in range(582):
#     date_string = date.strftime('%Y%m%d')
#     url = 'http://data.nba.net/data/10s/prod/v1/%s/scoreboard.json' % date_string
#     r = requests.get(url, headers=headers)
#     json_response = r.json()
#     # json_response = json.loads(r.text)
#     games = json_response['games']

#     for game in games:
#         if str(game['seasonStageId']) == '2':
#             game_id = game['gameId']

#             home_id = game['hTeam']['teamId']
#             home_abr = game['hTeam']['triCode']
#             try:
#                 home_score = int(game['hTeam']['score'])
#             except Exception as e:
#                 home_score = 0

#             away_id = game['vTeam']['teamId']
#             away_abr = game['vTeam']['triCode']
#             try:
#                 away_score = int(game['vTeam']['score'])
#             except Exception as e:
#                 away_score = 0

#             try:
#                 cur.execute('INSERT INTO games (game_id, date, home_id, home_abr, home_score, away_id, away_abr, away_score) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);', (game_id, date_string, home_id, home_abr, home_score, away_id, away_abr, away_score))
#                 conn.commit()
#             except IntegrityError as e:
#                 conn.rollback()
#                 print(e)
#             except DataError as e:
#                 print(home_score, away_score)
#                 print(e)

#     date += datetime.timedelta(days=1)
#     print(date)

# 2013 season
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
    'Host': 'stats.nba.com',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Accept': '*/*',
    'Origin': 'http://www.nba.com'
}

url = 'http://stats.nba.com/stats/leaguegamelog?Counter=1000&DateFrom=&DateTo=&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season=2013-14&SeasonType=Regular+Season&Sorter=PTS'
r = requests.get(url, headers=headers)
json_response = r.json()
games = json_response['resultSets'][0]['rowSet']
print(games)

for game in games:
    # create all the rows
    # game_id = game[4]
    # date = game[5]

    # try:
    #     cur.execute('INSERT INTO games (game_id, date) VALUES (%s, %s);', (game_id, date))
    #     conn.commit()
    # except IntegrityError as e:
    #     conn.rollback()

    game_id = game[4]
    matchup = game[6]

    if '@' in matchup:
        teams = matchup.split('@')
        # print(str(game[1]), teams[0])
        if str(game[2]) in teams[0]:
            cur.execute('UPDATE games SET away_id=(%s), away_abr=(%s), away_score=(%s) WHERE game_id=(%s);', (game[1], game[2], game[26], game_id))
        else:
            cur.execute('UPDATE games SET home_id=(%s), home_abr=(%s), home_score=(%s) WHERE game_id=(%s);', (game[1], game[2], game[26], game_id))

    elif 'vs' in matchup:
        teams = matchup.split('vs')
        if str(game[2]) in teams[1]:
            cur.execute('UPDATE games SET away_id=(%s), away_abr=(%s), away_score=(%s) WHERE game_id=(%s);', (game[1], game[2], game[26], game_id))
        else:
            cur.execute('UPDATE games SET home_id=(%s), home_abr=(%s), home_score=(%s) WHERE game_id=(%s);', (game[1], game[2], game[26], game_id))
    conn.commit()

cur.close()
conn.close()
