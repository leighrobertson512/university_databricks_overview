# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS university_learning.world_cup_agents;

# COMMAND ----------

# MAGIC %md
# MAGIC ## World Cup Database Schema - Primary and Foreign Key Relationships
# MAGIC
# MAGIC ### Core Dimension Tables (Primary Keys)
# MAGIC 1. **tournaments** - `tournament_id` (PK)
# MAGIC 2. **teams** - `team_id` (PK)
# MAGIC 3. **players** - `player_id` (PK)
# MAGIC 4. **managers** - `manager_id` (PK)
# MAGIC 5. **referees** - `referee_id` (PK)
# MAGIC 6. **stadiums** - `stadium_id` (PK)
# MAGIC 7. **awards** - `award_id` (PK)
# MAGIC 8. **confederations** - `confederation_id` (PK)
# MAGIC 9. **matches** - `match_id` (PK)
# MAGIC
# MAGIC ### Fact/Bridge Tables (Foreign Keys)
# MAGIC * **goals, bookings, substitutions, penalty_kicks** → tournament_id, match_id, team_id, player_id
# MAGIC * **matches** → tournament_id, stadium_id, home_team_id, away_team_id (both reference teams)
# MAGIC * **squads** → tournament_id, team_id, player_id
# MAGIC * **qualified_teams, host_countries** → tournament_id, team_id
# MAGIC * **award_winners** → tournament_id, award_id, player_id, team_id
# MAGIC * **manager_appointments, manager_appearances** → tournament_id, team_id, manager_id
# MAGIC * **referee_appointments, referee_appearances** → tournament_id, referee_id, confederation_id
# MAGIC * **teams, referees** → confederation_id
# MAGIC * **player_appearances, team_appearances** → tournament_id, match_id, team_id
# MAGIC * **groups, group_standings, tournament_stages, tournament_standings** → tournament_id
# MAGIC
# MAGIC **Note:** Unity Catalog supports informational constraints (PRIMARY KEY, FOREIGN KEY) that are NOT enforced but provide metadata for query optimization and BI tools.

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Primary Keys
# MAGIC %sql
# MAGIC -- Primary Key Constraints
# MAGIC -- Note: These are informational constraints in Unity Catalog (not enforced)
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.tournaments 
# MAGIC ADD CONSTRAINT pk_tournaments PRIMARY KEY(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.teams 
# MAGIC ADD CONSTRAINT pk_teams PRIMARY KEY(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.players 
# MAGIC ADD CONSTRAINT pk_players PRIMARY KEY(player_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.managers 
# MAGIC ADD CONSTRAINT pk_managers PRIMARY KEY(manager_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referees 
# MAGIC ADD CONSTRAINT pk_referees PRIMARY KEY(referee_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.stadiums 
# MAGIC ADD CONSTRAINT pk_stadiums PRIMARY KEY(stadium_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.awards 
# MAGIC ADD CONSTRAINT pk_awards PRIMARY KEY(award_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.confederations 
# MAGIC ADD CONSTRAINT pk_confederations PRIMARY KEY(confederation_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.matches 
# MAGIC ADD CONSTRAINT pk_matches PRIMARY KEY(match_id);

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Foreign Keys - Part 1
# MAGIC %sql
# MAGIC -- Foreign Key Constraints - Matches Table
# MAGIC ALTER TABLE university_learning.world_cup.matches 
# MAGIC ADD CONSTRAINT fk_matches_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.matches 
# MAGIC ADD CONSTRAINT fk_matches_stadium FOREIGN KEY(stadium_id) REFERENCES university_learning.world_cup.stadiums(stadium_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.matches 
# MAGIC ADD CONSTRAINT fk_matches_home_team FOREIGN KEY(home_team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.matches 
# MAGIC ADD CONSTRAINT fk_matches_away_team FOREIGN KEY(away_team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Teams Table
# MAGIC ALTER TABLE university_learning.world_cup.teams 
# MAGIC ADD CONSTRAINT fk_teams_confederation FOREIGN KEY(confederation_id) REFERENCES university_learning.world_cup.confederations(confederation_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Referees Table
# MAGIC ALTER TABLE university_learning.world_cup.referees 
# MAGIC ADD CONSTRAINT fk_referees_confederation FOREIGN KEY(confederation_id) REFERENCES university_learning.world_cup.confederations(confederation_id);

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Foreign Keys - Part 2 (Goals, Bookings, Substitutions)
# MAGIC %sql
# MAGIC -- Foreign Key Constraints - Goals Table
# MAGIC ALTER TABLE university_learning.world_cup.goals 
# MAGIC ADD CONSTRAINT fk_goals_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.goals 
# MAGIC ADD CONSTRAINT fk_goals_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.goals 
# MAGIC ADD CONSTRAINT fk_goals_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.goals 
# MAGIC ADD CONSTRAINT fk_goals_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Bookings Table
# MAGIC ALTER TABLE university_learning.world_cup.bookings 
# MAGIC ADD CONSTRAINT fk_bookings_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.bookings 
# MAGIC ADD CONSTRAINT fk_bookings_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.bookings 
# MAGIC ADD CONSTRAINT fk_bookings_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.bookings 
# MAGIC ADD CONSTRAINT fk_bookings_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Substitutions Table
# MAGIC ALTER TABLE university_learning.world_cup.substitutions 
# MAGIC ADD CONSTRAINT fk_substitutions_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.substitutions 
# MAGIC ADD CONSTRAINT fk_substitutions_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.substitutions 
# MAGIC ADD CONSTRAINT fk_substitutions_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.substitutions 
# MAGIC ADD CONSTRAINT fk_substitutions_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Foreign Keys - Part 3 (Penalty Kicks, Squads, Appearances)
# MAGIC %sql
# MAGIC -- Foreign Key Constraints - Penalty Kicks Table
# MAGIC ALTER TABLE university_learning.world_cup.penalty_kicks 
# MAGIC ADD CONSTRAINT fk_penalty_kicks_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.penalty_kicks 
# MAGIC ADD CONSTRAINT fk_penalty_kicks_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.penalty_kicks 
# MAGIC ADD CONSTRAINT fk_penalty_kicks_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.penalty_kicks 
# MAGIC ADD CONSTRAINT fk_penalty_kicks_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Squads Table
# MAGIC ALTER TABLE university_learning.world_cup.squads 
# MAGIC ADD CONSTRAINT fk_squads_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.squads 
# MAGIC ADD CONSTRAINT fk_squads_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.squads 
# MAGIC ADD CONSTRAINT fk_squads_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Player Appearances Table
# MAGIC ALTER TABLE university_learning.world_cup.player_appearances 
# MAGIC ADD CONSTRAINT fk_player_appearances_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.player_appearances 
# MAGIC ADD CONSTRAINT fk_player_appearances_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.player_appearances 
# MAGIC ADD CONSTRAINT fk_player_appearances_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.player_appearances 
# MAGIC ADD CONSTRAINT fk_player_appearances_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Team Appearances Table
# MAGIC ALTER TABLE university_learning.world_cup.team_appearances 
# MAGIC ADD CONSTRAINT fk_team_appearances_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.team_appearances 
# MAGIC ADD CONSTRAINT fk_team_appearances_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.team_appearances 
# MAGIC ADD CONSTRAINT fk_team_appearances_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.team_appearances 
# MAGIC ADD CONSTRAINT fk_team_appearances_stadium FOREIGN KEY(stadium_id) REFERENCES university_learning.world_cup.stadiums(stadium_id);

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Foreign Keys - Part 4 (Managers, Referees, Awards)
# MAGIC %sql
# MAGIC -- Foreign Key Constraints - Manager Appointments Table
# MAGIC ALTER TABLE university_learning.world_cup.manager_appointments 
# MAGIC ADD CONSTRAINT fk_manager_appointments_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.manager_appointments 
# MAGIC ADD CONSTRAINT fk_manager_appointments_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.manager_appointments 
# MAGIC ADD CONSTRAINT fk_manager_appointments_manager FOREIGN KEY(manager_id) REFERENCES university_learning.world_cup.managers(manager_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Manager Appearances Table
# MAGIC ALTER TABLE university_learning.world_cup.manager_appearances 
# MAGIC ADD CONSTRAINT fk_manager_appearances_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.manager_appearances 
# MAGIC ADD CONSTRAINT fk_manager_appearances_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.manager_appearances 
# MAGIC ADD CONSTRAINT fk_manager_appearances_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.manager_appearances 
# MAGIC ADD CONSTRAINT fk_manager_appearances_manager FOREIGN KEY(manager_id) REFERENCES university_learning.world_cup.managers(manager_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Referee Appointments Table
# MAGIC ALTER TABLE university_learning.world_cup.referee_appointments 
# MAGIC ADD CONSTRAINT fk_referee_appointments_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referee_appointments 
# MAGIC ADD CONSTRAINT fk_referee_appointments_referee FOREIGN KEY(referee_id) REFERENCES university_learning.world_cup.referees(referee_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referee_appointments 
# MAGIC ADD CONSTRAINT fk_referee_appointments_confederation FOREIGN KEY(confederation_id) REFERENCES university_learning.world_cup.confederations(confederation_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Referee Appearances Table
# MAGIC ALTER TABLE university_learning.world_cup.referee_appearances 
# MAGIC ADD CONSTRAINT fk_referee_appearances_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referee_appearances 
# MAGIC ADD CONSTRAINT fk_referee_appearances_match FOREIGN KEY(match_id) REFERENCES university_learning.world_cup.matches(match_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referee_appearances 
# MAGIC ADD CONSTRAINT fk_referee_appearances_referee FOREIGN KEY(referee_id) REFERENCES university_learning.world_cup.referees(referee_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.referee_appearances 
# MAGIC ADD CONSTRAINT fk_referee_appearances_confederation FOREIGN KEY(confederation_id) REFERENCES university_learning.world_cup.confederations(confederation_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Award Winners Table
# MAGIC ALTER TABLE university_learning.world_cup.award_winners 
# MAGIC ADD CONSTRAINT fk_award_winners_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.award_winners 
# MAGIC ADD CONSTRAINT fk_award_winners_award FOREIGN KEY(award_id) REFERENCES university_learning.world_cup.awards(award_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.award_winners 
# MAGIC ADD CONSTRAINT fk_award_winners_player FOREIGN KEY(player_id) REFERENCES university_learning.world_cup.players(player_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.award_winners 
# MAGIC ADD CONSTRAINT fk_award_winners_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);

# COMMAND ----------

# DBTITLE 1,ALTER TABLE Statements for Foreign Keys - Part 5 (Tournament-related tables)
# MAGIC %sql
# MAGIC -- Foreign Key Constraints - Qualified Teams Table
# MAGIC ALTER TABLE university_learning.world_cup.qualified_teams 
# MAGIC ADD CONSTRAINT fk_qualified_teams_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.qualified_teams 
# MAGIC ADD CONSTRAINT fk_qualified_teams_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Host Countries Table
# MAGIC ALTER TABLE university_learning.world_cup.host_countries 
# MAGIC ADD CONSTRAINT fk_host_countries_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.host_countries 
# MAGIC ADD CONSTRAINT fk_host_countries_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Groups Table
# MAGIC ALTER TABLE university_learning.world_cup.groups 
# MAGIC ADD CONSTRAINT fk_groups_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Group Standings Table
# MAGIC ALTER TABLE university_learning.world_cup.group_standings 
# MAGIC ADD CONSTRAINT fk_group_standings_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.group_standings 
# MAGIC ADD CONSTRAINT fk_group_standings_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Tournament Stages Table
# MAGIC ALTER TABLE university_learning.world_cup.tournament_stages 
# MAGIC ADD CONSTRAINT fk_tournament_stages_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC -- Foreign Key Constraints - Tournament Standings Table
# MAGIC ALTER TABLE university_learning.world_cup.tournament_standings 
# MAGIC ADD CONSTRAINT fk_tournament_standings_tournament FOREIGN KEY(tournament_id) REFERENCES university_learning.world_cup.tournaments(tournament_id);
# MAGIC
# MAGIC ALTER TABLE university_learning.world_cup.tournament_standings 
# MAGIC ADD CONSTRAINT fk_tournament_standings_team FOREIGN KEY(team_id) REFERENCES university_learning.world_cup.teams(team_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## World Cup Database Schema - Primary and Foreign Keys Reference
# MAGIC
# MAGIC ### Note on Iceberg Tables
# MAGIC Since these are **managed Iceberg tables**, they may not support traditional ALTER TABLE constraint syntax. This document serves as a reference for the logical relationships in the schema.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Primary Keys
# MAGIC
# MAGIC | Table | Primary Key Column |
# MAGIC |-------|-------------------|
# MAGIC | `tournaments` | `tournament_id` |
# MAGIC | `teams` | `team_id` |
# MAGIC | `players` | `player_id` |
# MAGIC | `managers` | `manager_id` |
# MAGIC | `referees` | `referee_id` |
# MAGIC | `stadiums` | `stadium_id` |
# MAGIC | `awards` | `award_id` |
# MAGIC | `confederations` | `confederation_id` |
# MAGIC | `matches` | `match_id` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Foreign Key Relationships
# MAGIC
# MAGIC #### Matches Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `stadium_id` → `stadiums(stadium_id)`
# MAGIC * `home_team_id` → `teams(team_id)`
# MAGIC * `away_team_id` → `teams(team_id)`
# MAGIC
# MAGIC #### Teams Table
# MAGIC * `confederation_id` → `confederations(confederation_id)`
# MAGIC
# MAGIC #### Referees Table
# MAGIC * `confederation_id` → `confederations(confederation_id)`
# MAGIC
# MAGIC #### Goals Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Bookings Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Substitutions Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Penalty Kicks Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Squads Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Player Appearances Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC
# MAGIC #### Team Appearances Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `stadium_id` → `stadiums(stadium_id)`
# MAGIC
# MAGIC #### Manager Appointments Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `manager_id` → `managers(manager_id)`
# MAGIC
# MAGIC #### Manager Appearances Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC * `manager_id` → `managers(manager_id)`
# MAGIC
# MAGIC #### Referee Appointments Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `referee_id` → `referees(referee_id)`
# MAGIC * `confederation_id` → `confederations(confederation_id)`
# MAGIC
# MAGIC #### Referee Appearances Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `match_id` → `matches(match_id)`
# MAGIC * `referee_id` → `referees(referee_id)`
# MAGIC * `confederation_id` → `confederations(confederation_id)`
# MAGIC
# MAGIC #### Award Winners Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `award_id` → `awards(award_id)`
# MAGIC * `player_id` → `players(player_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC
# MAGIC #### Qualified Teams Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC
# MAGIC #### Host Countries Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC
# MAGIC #### Groups Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC
# MAGIC #### Group Standings Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`
# MAGIC
# MAGIC #### Tournament Stages Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC
# MAGIC #### Tournament Standings Table
# MAGIC * `tournament_id` → `tournaments(tournament_id)`
# MAGIC * `team_id` → `teams(team_id)`