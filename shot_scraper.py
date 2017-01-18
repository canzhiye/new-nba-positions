import requests
import json

from multiprocessing import Pool

import psycopg2

headers = {'Cookie' : 'ASP.NET_SessionId=bf4bdsf5vqkauv3hquiydwzj; globalUserOrderId=Id=; bSID=Id=d4dc678a-49f5-42de-8635-e6d783c55f50; s_fuid=17691613676678691303211417123336815876; s_pers=%20s_last_team%3DPhiladelphia%252076ers%7C1503958884858%3B%20productnum%3D2%7C1475015130930%3B; rr_rcs=eF4FwbENgDAMBMCGil1ewrHfSTZgDewEiYIOmJ-7ZXlzN0Y6taMFHTZbwGNMJMmwPOJ0W-_vucYm4g1itVhRUaldQAXkB673EiE; s_cc=true; s_fid=188D52E3F63005CA-2C913EDBDE190504; s_sq=nbag-n-76ers%3D%2526pid%253Dwww.nba.com%25253A%25252Fsixers%25252Fschedule%2526pidt%253D1%2526oid%253Dhttp%25253A%25252F%25252Fwww.nba.com%25252Fsixers%25252Fschedule%252523game-block-overview%2526ot%253DA', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'}


team_ids = [1610612737, 1610612738, 1610612739, 1610612740, 1610612741, 1610612742, 1610612743, 1610612744, 1610612745, 1610612746, 1610612747, 1610612748, 1610612749, 1610612750, 1610612751, 1610612752, 1610612753, 1610612754, 1610612755, 1610612756, 1610612757, 1610612758, 1610612759, 1610612760, 1610612761, 1610612762, 1610612763, 1610612764, 1610612765, 1610612766]

seasons = ['2016-17', '2015-16', '2014-15', '2013-14', '2012-13', '2011-12', '2010-11', '2009-10', '2008-09', '2007-08', '2006-07', '2005-06', '2004-05', '2003-04', '2002-03', '2001-02']


def index_season(season):
    # team_ids = [1610612737]
    conn = psycopg2.connect(host='nbadata.cgwazkifkz49.us-west-2.rds.amazonaws.com', user='nbadata', password='2016TtP!', dbname='nbadata', port=5432)
    cur = conn.cursor()

    teams_to_games = {}

    for team_id in team_ids:
        url = 'http://stats.nba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=00&Season=%s&SeasonType=Regular+Season&TeamID=%d' % (season, team_id)

        r = requests.get(url, headers=headers)
        json_response = json.loads(r.text)

        games = json_response['resultSets'][0]['rowSet']

        game_ids = [game[1] for game in games]
        teams_to_games[team_id] = game_ids

        # game_id = '0021501227'
        # team_id = '1610612744'

        for game_id in game_ids:
            shot_tracker_url= 'http://stats.nba.com/stats/shotchartdetail?CFID=&CFPARAMS=&ContextFilter=&ContextMeasure=FGA&DateFrom=&DateTo=&GameID=%s&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID=0&Position=&RookieYear=&Season=%s&SeasonSegment=&SeasonType=Regular+Season&TeamID=%s&VsConference=&VsDivision=&PlayerPosition=' % (game_id, season, team_id)

            r = requests.get(shot_tracker_url, headers=headers)
            json_response = json.loads(r.text)

            col_headers = json_response['resultSets'][0]['headers']
            shots = json_response['resultSets'][0]['rowSet']

            # print(json_response['resultSets'][0]['headers'])

            # shots = shots[:5]
            for shot in shots:
                s = shot[:len(shot)-3]
                try:
                    cur.execute('INSERT INTO shots (grid_type, game_id, game_event_id, player_id, player_name, team_id, team_name, period, minutes_remaining, seconds_remaining, event_type, action_type, shot_type, shot_zone_basic, shot_zone_area, shot_zone_range, shot_distance, loc_x, loc_y, shot_attempted_flag, shot_made_flag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', tuple(s))
                    conn.commit()
                    print(s)
                except psycopg2.IntegrityError as e:
                    conn.rollback()
                    print(e)

            # try:
            #     cur.close()
            #     conn.close()
            # except psycopg2.InterfaceError as e:
            #     print(e)
            # cur.close()
            # conn.close()

p = Pool(16)
p.map(index_season, seasons)


# tuple(n if i == k else 1 for i in range(m))
# for team_id in teams_to_games.keys():
#     game_ids = teams_to_games[team_id]
#     for game_id in game_ids:
#         shot_tracker_url= 'http://stats.nba.com/stats/shotchartdetail?CFID=&CFPARAMS=&ContextFilter=&ContextMeasure=FGA&DateFrom=&DateTo=&GameID=%d&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID=0&Position=&RookieYear=&Season=2015-16&SeasonSegment=&SeasonType=Regular+Season&TeamID=%d&VsConference=&VsDivision=' % (game_id, team_id)

# ["Shot Chart Detail","0021501227",420,2571,"Leandro Barbosa",1610612744,"Golden State Warriors",4,7,32,"Missed Shot","Running Jump Shot","2PT Field Goal","In The Paint (Non-RA)","Center(C)","Less Than 8 ft.",5,-45,31,1,0]




