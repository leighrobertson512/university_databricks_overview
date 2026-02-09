# Databricks notebook source
catalog = "university_learning" 
schema = "world_cup"
volume_name = "world_cup_data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DDL Statements for World Cup Data
# MAGIC -- Generated from CSV files in volume
# MAGIC -- Total tables: 27
# MAGIC
# MAGIC
# MAGIC -- Table: award_winners
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.award_winners (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   award_id STRING,
# MAGIC   award_name STRING,
# MAGIC   shared INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, player_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: awards
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.awards (
# MAGIC   key_id INT,
# MAGIC   award_id STRING,
# MAGIC   award_name STRING,
# MAGIC   award_description STRING,
# MAGIC   year_introduced INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (year_introduced, key_id, award_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: bookings
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.bookings (
# MAGIC   key_id INT,
# MAGIC   booking_id STRING,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   minute_label STRING,
# MAGIC   minute_regulation INT,
# MAGIC   minute_stoppage INT,
# MAGIC   match_period STRING,
# MAGIC   yellow_card INT,
# MAGIC   red_card INT,
# MAGIC   second_yellow_card INT,
# MAGIC   sending_off INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: confederations
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.confederations (
# MAGIC   key_id INT,
# MAGIC   confederation_id STRING,
# MAGIC   confederation_name STRING,
# MAGIC   confederation_code STRING,
# MAGIC   confederation_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (key_id, confederation_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: goals
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.goals (
# MAGIC   key_id INT,
# MAGIC   goal_id STRING,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   player_team_id STRING,
# MAGIC   player_team_name STRING,
# MAGIC   player_team_code STRING,
# MAGIC   minute_label STRING,
# MAGIC   minute_regulation INT,
# MAGIC   minute_stoppage INT,
# MAGIC   match_period STRING,
# MAGIC   own_goal INT,
# MAGIC   penalty INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: group_standings
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.group_standings (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   stage_number INT,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   position INT,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   played INT,
# MAGIC   wins INT,
# MAGIC   draws INT,
# MAGIC   losses INT,
# MAGIC   goals_for INT,
# MAGIC   goals_against INT,
# MAGIC   goal_difference INT,
# MAGIC   points INT,
# MAGIC   advanced INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: groups
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.groups (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   stage_number INT,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   count_teams INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: host_countries
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.host_countries (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   performance STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: manager_appearances
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.manager_appearances (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   manager_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   country_name STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: manager_appointments
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.manager_appointments (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   manager_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   country_name STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, manager_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: managers
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.managers (
# MAGIC   key_id INT,
# MAGIC   manager_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   female INT,
# MAGIC   country_name STRING,
# MAGIC   manager_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (manager_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: matches
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.matches (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   group_stage INT,
# MAGIC   knockout_stage INT,
# MAGIC   replayed INT,
# MAGIC   replay INT,
# MAGIC   match_date DATE,
# MAGIC   match_time TIMESTAMP,
# MAGIC   stadium_id STRING,
# MAGIC   stadium_name STRING,
# MAGIC   city_name STRING,
# MAGIC   country_name STRING,
# MAGIC   home_team_id STRING,
# MAGIC   home_team_name STRING,
# MAGIC   home_team_code STRING,
# MAGIC   away_team_id STRING,
# MAGIC   away_team_name STRING,
# MAGIC   away_team_code STRING,
# MAGIC   score STRING,
# MAGIC   home_team_score INT,
# MAGIC   away_team_score INT,
# MAGIC   home_team_score_margin INT,
# MAGIC   away_team_score_margin INT,
# MAGIC   extra_time INT,
# MAGIC   penalty_shootout INT,
# MAGIC   score_penalties STRING,
# MAGIC   home_team_score_penalties INT,
# MAGIC   away_team_score_penalties INT,
# MAGIC   result STRING,
# MAGIC   home_team_win INT,
# MAGIC   away_team_win INT,
# MAGIC   draw INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, home_team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: penalty_kicks
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.penalty_kicks (
# MAGIC   key_id INT,
# MAGIC   penalty_kick_id STRING,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   converted INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: player_appearances
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.player_appearances (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   position_name STRING,
# MAGIC   position_code STRING,
# MAGIC   starter INT,
# MAGIC   substitute INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: players
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.players (
# MAGIC   key_id INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   birth_date STRING,
# MAGIC   female INT,
# MAGIC   goal_keeper INT,
# MAGIC   defender INT,
# MAGIC   midfielder INT,
# MAGIC   forward INT,
# MAGIC   count_tournaments INT,
# MAGIC   list_tournaments STRING,
# MAGIC   player_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (player_id, birth_date, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: qualified_teams
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.qualified_teams (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   count_matches INT,
# MAGIC   performance STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: referee_appearances
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.referee_appearances (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   referee_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   country_name STRING,
# MAGIC   confederation_id STRING,
# MAGIC   confederation_name STRING,
# MAGIC   confederation_code STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, referee_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: referee_appointments
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.referee_appointments (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   referee_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   country_name STRING,
# MAGIC   confederation_id STRING,
# MAGIC   confederation_name STRING,
# MAGIC   confederation_code STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, referee_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: referees
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.referees (
# MAGIC   key_id INT,
# MAGIC   referee_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   female INT,
# MAGIC   country_name STRING,
# MAGIC   confederation_id STRING,
# MAGIC   confederation_name STRING,
# MAGIC   confederation_code STRING,
# MAGIC   referee_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (referee_id, key_id, confederation_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: squads
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.squads (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   position_name STRING,
# MAGIC   position_code STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, player_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: stadiums
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.stadiums (
# MAGIC   key_id INT,
# MAGIC   stadium_id STRING,
# MAGIC   stadium_name STRING,
# MAGIC   city_name STRING,
# MAGIC   country_name STRING,
# MAGIC   stadium_capacity INT,
# MAGIC   stadium_wikipedia_link STRING,
# MAGIC   city_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (key_id, stadium_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: substitutions
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.substitutions (
# MAGIC   key_id INT,
# MAGIC   substitution_id STRING,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   match_date DATE,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   player_id STRING,
# MAGIC   family_name STRING,
# MAGIC   given_name STRING,
# MAGIC   shirt_number INT,
# MAGIC   minute_label STRING,
# MAGIC   minute_regulation INT,
# MAGIC   minute_stoppage INT,
# MAGIC   match_period STRING,
# MAGIC   going_off INT,
# MAGIC   coming_on INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: team_appearances
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.team_appearances (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   match_id STRING,
# MAGIC   match_name STRING,
# MAGIC   stage_name STRING,
# MAGIC   group_name STRING,
# MAGIC   group_stage INT,
# MAGIC   knockout_stage INT,
# MAGIC   replayed INT,
# MAGIC   replay INT,
# MAGIC   match_date DATE,
# MAGIC   match_time TIMESTAMP,
# MAGIC   stadium_id STRING,
# MAGIC   stadium_name STRING,
# MAGIC   city_name STRING,
# MAGIC   country_name STRING,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   opponent_id STRING,
# MAGIC   opponent_name STRING,
# MAGIC   opponent_code STRING,
# MAGIC   home_team INT,
# MAGIC   away_team INT,
# MAGIC   goals_for INT,
# MAGIC   goals_against INT,
# MAGIC   goal_differential INT,
# MAGIC   extra_time INT,
# MAGIC   penalty_shootout INT,
# MAGIC   penalties_for INT,
# MAGIC   penalties_against INT,
# MAGIC   result STRING,
# MAGIC   win INT,
# MAGIC   lose INT,
# MAGIC   draw INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, match_id, team_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: teams
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.teams (
# MAGIC   key_id INT,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   mens_team INT,
# MAGIC   womens_team INT,
# MAGIC   federation_name STRING,
# MAGIC   region_name STRING,
# MAGIC   confederation_id STRING,
# MAGIC   confederation_name STRING,
# MAGIC   confederation_code STRING,
# MAGIC   mens_team_wikipedia_link STRING,
# MAGIC   womens_team_wikipedia_link STRING,
# MAGIC   federation_wikipedia_link STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (team_id, key_id, confederation_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: tournament_stages
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.tournament_stages (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   stage_number INT,
# MAGIC   stage_name STRING,
# MAGIC   group_stage INT,
# MAGIC   knockout_stage INT,
# MAGIC   unbalanced_groups INT,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   count_matches INT,
# MAGIC   count_teams INT,
# MAGIC   count_scheduled INT,
# MAGIC   count_replays INT,
# MAGIC   count_playoffs INT,
# MAGIC   count_walkovers INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, start_date, end_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: tournament_standings
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.tournament_standings (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   position INT,
# MAGIC   team_id STRING,
# MAGIC   team_name STRING,
# MAGIC   team_code STRING,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, team_id, key_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Table: tournaments
# MAGIC CREATE TABLE IF NOT EXISTS university_learning.world_cup.tournaments (
# MAGIC   key_id INT,
# MAGIC   tournament_id STRING,
# MAGIC   tournament_name STRING,
# MAGIC   year INT,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   host_country STRING,
# MAGIC   winner STRING,
# MAGIC   host_won INT,
# MAGIC   count_teams INT,
# MAGIC   group_stage INT,
# MAGIC   second_group_stage INT,
# MAGIC   final_round INT,
# MAGIC   round_of_16 INT,
# MAGIC   quarter_finals INT,
# MAGIC   semi_finals INT,
# MAGIC   third_place_match INT,
# MAGIC   final INT,
# MAGIC   audit_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (tournament_id, year, start_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

"""
1. Steps create free account with Databricks
2. Clone git URL locally 
3. Run setup -> create statements
4. Write tables to Databricks 
5. Run command to add relationships 
"""

# COMMAND ----------

# DBTITLE 1,Write CSVs to Iceberg tables
from pyspark.sql.functions import lit, current_timestamp
# Read all CSV files into dataframes and write to Iceberg tables
world_cup_data_path = '/Volumes/university_learning/world_cup/world_cup_data/'
files = dbutils.fs.ls(world_cup_data_path)
csv_files = [file for file in files if file.name.endswith('.csv')]

# Dictionary to store dataframes
dataframes = {}

for file in csv_files:
    table_name = file.name.replace('.csv', '')
    file_path = world_cup_data_path + file.name
    table_full_name = f"university_learning.world_cup.{table_name}"
    
    print(f"Reading {file.name}...")
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Add audit_update_ts column
    df_to_write = df.withColumn('audit_update_ts', current_timestamp())
    
    # Write to Iceberg table
    print(f" Truncating and writing to {table_full_name}...")
    #for the sake of the demo we will be doing blind appends and hence will truncate the table before to prevent duplications
    spark.sql(f"TRUNCATE TABLE {table_full_name}")

    df_to_write.write.mode("append").saveAsTable(table_full_name)
    
    # Store original df for DDL generation
    dataframes[table_name] = df
    
    row_count = df.count()
    print(f"  ✓ {table_name}: {row_count} rows written\n")

print(f"\nSuccessfully loaded and wrote {len(dataframes)} tables")

# COMMAND ----------

