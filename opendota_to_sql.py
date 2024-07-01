# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 17:56:22 2024

@author: Oliver Just-De Bleser
"""

import requests
from datetime import datetime
import psycopg2 as psql
from dotenv import load_dotenv, dotenv_values
import os

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
                try:
                    matches = requests.get(
                        f"https://api.opendota.com/api/publicMatches?min_rank={rank_dist[0]}&max_rank={rank_dist[1]}"
                    ).json()
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
                        match_data = requests.get(
                            f"https://api.opendota.com/api/matches/{match['match_id']}"
                        ).json()
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
                
if __name__ == '__main__':
    load_dotenv()
    get_matches()
        
    