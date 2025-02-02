# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 17:56:22 2024

@author: Oliver Just-De Bleser
"""

import requests
import psycopg2 as psql
from dotenv import load_dotenv
import os
import time 

def create_match_table():
    '''
    Creates postgresql table for match data if the table doesn't exist

    Returns
    -------
    None.

    '''
    with psql.connect(database = 'pagila',
               user = os.getenv("sql_user"),
               host = os.getenv("host"),
               password = os.getenv("sql_password"),
               port=5432) as conn:
        with conn.cursor() as cur:
            creation_slq = '''
                    CREATE TABLE IF NOT EXISTS student.ojdb_matches (
                        match_id BIGINT PRIMARY KEY,
                        rank SMALLINT,
                        radiant_wins BOOLEAN NOT NULL
                    );
                    '''
            # run query
            cur.execute(creation_slq)
            conn.commit()
            

def create_hero_picks_table():
    '''
    Creates postgresql table for hero picks data if the table doesn't exist

    Returns
    -------
    None.

    '''
    with psql.connect(database = 'pagila',
               user = os.getenv("sql_user"),
               host = os.getenv("host"),
               password = os.getenv("sql_password"),
               port=5432) as conn:
        with conn.cursor() as cur:
            creation_slq = '''
            CREATE TABLE IF NOT EXISTS student.ojdb_hero_picks (
                match_id BIGINT REFERENCES student.ojdb_matches(match_id),
                hero_id SMALLINT,
                team SMALLINT,
                facet SMALLINT,
                items SMALLINT[] NOT NULL CHECK (array_length(items, 1) = 6),
                backpack SMALLINT[] NOT NULL CHECK (array_length(backpack, 1) = 3),
                neutral_item SMALLINT,
                kills SMALLINT,
                deaths SMALLINT,
                assists SMALLINT,
                gold_per_min SMALLINT,
                xp_per_min SMALLINT,
                level SMALLINT,
                net_worth INT,
                aghanims_scepter BOOLEAN,
                aghanims_shard BOOLEAN,
                moonshard BOOLEAN,
                hero_damage INT,
                tower_damage INT,
                hero_healing INT,
                PRIMARY KEY(match_id, hero_id)
            );
            '''
            cur.execute(creation_slq)
            conn.commit()


def get_matches():
    '''
    Makes get requests to the OpenDota API first to get 100 random public
    matches in each rank distribution given. Then queries the match API to 
    get more information regariding heroes, items purchased, damage done etc.

    Returns
    -------
    None.

    '''
    create_match_table()
    create_hero_picks_table()
    rank_distributions = ((10,15), # herald
                          (20,25), # guardian
                          (30,35), # crusader
                          (40,45), # archon
                          (50,55), # legend
                          (60,65), # ancient
                          (70,75), # divine
                          (80,85)) # immortal
    
    with psql.connect(database = 'pagila',
                user = os.getenv("sql_user"),
                host = os.getenv("host"),
                password = os.getenv("sql_password"),
                port=5432) as conn:
        with conn.cursor() as cur:
            for rank_dist in rank_distributions:
                responce = requests.get(
                    f"https://api.opendota.com/api/publicMatches?min_rank={rank_dist[0]}&max_rank={rank_dist[1]}"
                )
                limit = 0
                while responce.status_code !=200 and limit < 60:
                    time.sleep(1)
                    responce = requests.get(
                        f"https://api.opendota.com/api/publicMatches?min_rank={rank_dist[0]}&max_rank={rank_dist[1]}"
                    )
                    limit +=1
                try:
                    matches = responce.json()
                    insert_match_query = """
                                        INSERT INTO student.ojdb_matches (match_id, avg_rank, radiant_wins)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (match_id) DO NOTHING;
                                        """
                    insert_player_query = """
                                        INSERT INTO student.ojdb_hero_picks (match_id, hero_id, team, facet, items, backpack, neutral_item,
                                        kills, deaths, assists, gold_per_min, xp_per_min, level, net_worth, aghanims_scepter, aghanims_shard,
                                        moonshard, hero_damage, tower_damage, hero_healing)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (match_id, hero_id) DO NOTHING;
                                        """
                    
                    for match in matches:
                        match_values = (
                            match['match_id'],
                            match['avg_rank_tier'],
                            match['radiant_win']           
                            )
                        cur.execute(insert_match_query, match_values)
                        match_responce = requests.get(
                            f"https://api.opendota.com/api/matches/{match['match_id']}"
                        )
                        limit = 0
                        while match_responce.status_code != 200 and limit < 60:
                            time.sleep(0)
                            match_responce = requests.get(
                                f"https://api.opendota.com/api/matches/{match['match_id']}"
                            )
                            limit += 1
                        match_data = match_responce.json()
                        player_data = match_data['players']
                        for player in player_data:
                            player_values = (
                                match['match_id'],
                                player['hero_id'],
                                player['team_number'],
                                player['hero_variant'],
                                [player[f'item_{i}'] for i in range(6)],
                                [player[f'backpack_{i}'] for i in range(3)],
                                player['item_neutral'],
                                player['kills'],
                                player['deaths'],
                                player['assists'],
                                player['gold_per_min'],
                                player['xp_per_min'],
                                player['level'],
                                player['net_worth'],
                                bool(player['aghanims_scepter']),
                                bool(player['aghanims_shard']),
                                bool(player['moonshard']),
                                player['hero_damage'],
                                player['tower_damage'],
                                player['hero_healing']
                            )
                            cur.execute(insert_player_query, player_values)
                        conn.commit()
                except:
                    pass

def create_hero_bench_table():
    '''
    Creates postgresql table for hero picks data if the table doesn't exist

    Returns
    -------
    None.

    '''
    with psql.connect(database = 'pagila',
               user = os.getenv("sql_user"),
               host = os.getenv("host"),
               password = os.getenv("sql_password"),
               port=5432) as conn:
        with conn.cursor() as cur:
            creation_slq = '''
            CREATE TABLE IF NOT EXISTS student.ojdb_hero_benchmark (
                hero_id SMALLINT,
                avg_gpm INT,
                avg_xpm INT,
                PRIMARY KEY(hero_id)
            );
            '''
            cur.execute(creation_slq)
            conn.commit()
    
def get_hero_benchmarks():
    create_hero_bench_table()
    hero_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
                36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
                52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
                68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83,
                84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
                100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
                113, 114, 119, 120, 121, 123, 126, 128, 129, 135, 136, 137, 138]
    
    with psql.connect(database = 'pagila',
                user = os.getenv("sql_user"),
                host = os.getenv("host"),
                password = os.getenv("sql_password"),
                port=5432) as conn:
        insert_bench_query = """
                            INSERT INTO student.ojdb_hero_benchmark (hero_id, avg_gpm, avg_xpm)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (hero_id) DO UPDATE SET
                                avg_gpm = EXCLUDED.avg_gpm,
                                avg_xpm = EXCLUDED.avg_xpm;
                            """
        with conn.cursor() as cur:
            for hero_id in hero_ids:
                responce = requests.get(f"https://api.opendota.com/api/benchmarks?hero_id={hero_id}")
                limit = 0
                while responce.status_code != 200 and limit < 60:
                    time.sleep(1)
                    responce = requests.get(f"https://api.opendota.com/api/benchmarks?hero_id={hero_id}")
                    limit += 1
                try:
                    benchmark_data = responce.json()
                    benchmark_values = (
                        hero_id,
                        benchmark_data['result']['gold_per_min'][4]['value'], # gpm
                        benchmark_data['result']['xp_per_min'][4]['value'] #xpm
                        )
                    
                    cur.execute(insert_bench_query, benchmark_values)
                except:
                    pass
            conn.commit()
        
                
if __name__ == '__main__':
    load_dotenv()
    get_matches()
    get_hero_benchmarks()
    