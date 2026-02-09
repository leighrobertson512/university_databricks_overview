-- ============================================================================
-- World Cup Dashboard Queries
-- ============================================================================
-- This file contains SQL queries for building a comprehensive World Cup dashboard
-- Catalog: university_learning
-- Schema: world_cup
-- ============================================================================

-- ============================================================================
-- 1. TOURNAMENT OVERVIEW & KPI METRICS
-- ============================================================================

-- Overall Tournament Statistics
SELECT 
    COUNT(DISTINCT tournaments.tournament_id) AS total_tournaments,
    MIN(tournaments.year) AS first_tournament_year,
    MAX(tournaments.year) AS latest_tournament_year,
    COUNT(DISTINCT qualified_teams.team_id) AS total_unique_teams,
    COUNT(DISTINCT squads.player_id) AS total_unique_players,
    COUNT(DISTINCT matches.match_id) AS total_matches,
    SUM(matches.home_team_score + matches.away_team_score) AS total_goals_scored,
    ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) AS avg_goals_per_match
FROM university_learning.world_cup.tournaments tournaments
LEFT JOIN university_learning.world_cup.matches matches ON tournaments.tournament_id = matches.tournament_id
LEFT JOIN university_learning.world_cup.qualified_teams qualified_teams ON tournaments.tournament_id = qualified_teams.tournament_id
LEFT JOIN university_learning.world_cup.squads squads ON tournaments.tournament_id = squads.tournament_id;

-- Tournament Summary by Year
SELECT 
    tournaments.tournament_id,
    tournaments.tournament_name,
    tournaments.year,
    tournaments.start_date,
    tournaments.end_date,
    tournaments.host_country,
    tournaments.winner,
    tournaments.count_teams,
    COUNT(DISTINCT matches.match_id) AS total_matches,
    SUM(matches.home_team_score + matches.away_team_score) AS total_goals,
    ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) AS avg_goals_per_match
FROM university_learning.world_cup.tournaments tournaments
LEFT JOIN university_learning.world_cup.matches matches ON tournaments.tournament_id = matches.tournament_id
GROUP BY tournaments.tournament_id, tournaments.tournament_name, tournaments.year, tournaments.start_date, tournaments.end_date, tournaments.host_country, tournaments.winner, tournaments.count_teams
ORDER BY tournaments.year DESC;

-- ============================================================================
-- 2. TOP SCORING PLAYERS
-- ============================================================================

-- Top 20 All-Time Goal Scorers
SELECT 
    players.player_id,
    players.family_name,
    players.given_name,
    COUNT(DISTINCT goals.goal_id) AS total_goals,
    COUNT(DISTINCT goals.tournament_id) AS tournaments_played,
    COUNT(DISTINCT goals.match_id) AS matches_played,
    SUM(CASE WHEN goals.own_goal = 1 THEN 1 ELSE 0 END) AS own_goals,
    SUM(CASE WHEN goals.penalty = 1 THEN 1 ELSE 0 END) AS penalty_goals,
    COUNT(DISTINCT goals.team_id) AS teams_represented
FROM university_learning.world_cup.players players
INNER JOIN university_learning.world_cup.goals goals ON players.player_id = goals.player_id
WHERE goals.own_goal = 0 OR goals.own_goal IS NULL
GROUP BY players.player_id, players.family_name, players.given_name
ORDER BY total_goals DESC
LIMIT 20;

-- Top Goal Scorers by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    players.family_name,
    players.given_name,
    teams.team_name,
    COUNT(goals.goal_id) AS goals_scored,
    SUM(CASE WHEN goals.penalty = 1 THEN 1 ELSE 0 END) AS penalty_goals
FROM university_learning.world_cup.goals goals
INNER JOIN university_learning.world_cup.tournaments tournaments ON goals.tournament_id = tournaments.tournament_id
INNER JOIN university_learning.world_cup.players players ON goals.player_id = players.player_id
INNER JOIN university_learning.world_cup.teams teams ON goals.team_id = teams.team_id
WHERE (goals.own_goal = 0 OR goals.own_goal IS NULL)
GROUP BY tournaments.tournament_name, tournaments.year, players.family_name, players.given_name, teams.team_name
QUALIFY ROW_NUMBER() OVER (PARTITION BY tournaments.tournament_id ORDER BY COUNT(goals.goal_id) DESC) <= 5
ORDER BY tournaments.year DESC, goals_scored DESC;

-- ============================================================================
-- 3. TEAM PERFORMANCE METRICS
-- ============================================================================

-- All-Time Team Performance Summary
SELECT 
    teams.team_id,
    teams.team_name,
    teams.team_code,
    confederations.confederation_name,
    COUNT(DISTINCT qualified_teams.tournament_id) AS tournaments_qualified,
    COUNT(DISTINCT team_appearances.match_id) AS total_matches,
    SUM(team_appearances.win) AS total_wins,
    SUM(team_appearances.draw) AS total_draws,
    SUM(team_appearances.lose) AS total_losses,
    SUM(team_appearances.goals_for) AS total_goals_for,
    SUM(team_appearances.goals_against) AS total_goals_against,
    SUM(team_appearances.goals_for) - SUM(team_appearances.goals_against) AS goal_difference,
    ROUND(SUM(team_appearances.win) * 3.0 + SUM(team_appearances.draw), 2) AS total_points,
    ROUND(AVG(team_appearances.goals_for), 2) AS avg_goals_for_per_match,
    ROUND(AVG(team_appearances.goals_against), 2) AS avg_goals_against_per_match
FROM university_learning.world_cup.teams teams
LEFT JOIN university_learning.world_cup.confederations confederations ON teams.confederation_id = confederations.confederation_id
LEFT JOIN university_learning.world_cup.qualified_teams qualified_teams ON teams.team_id = qualified_teams.team_id
LEFT JOIN university_learning.world_cup.team_appearances team_appearances ON teams.team_id = team_appearances.team_id
GROUP BY teams.team_id, teams.team_name, teams.team_code, confederations.confederation_name
HAVING COUNT(DISTINCT qualified_teams.tournament_id) > 0
ORDER BY total_points DESC, goal_difference DESC
LIMIT 50;

-- Team Performance by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    tournament_standings.team_name,
    tournament_standings.team_code,
    tournament_standings.position AS final_position,
    COUNT(DISTINCT team_appearances.match_id) AS matches_played,
    SUM(team_appearances.win) AS wins,
    SUM(team_appearances.draw) AS draws,
    SUM(team_appearances.lose) AS losses,
    SUM(team_appearances.goals_for) AS goals_for,
    SUM(team_appearances.goals_against) AS goals_against,
    SUM(team_appearances.goals_for) - SUM(team_appearances.goals_against) AS goal_difference
FROM university_learning.world_cup.tournament_standings tournament_standings
INNER JOIN university_learning.world_cup.tournaments tournaments ON tournament_standings.tournament_id = tournaments.tournament_id
LEFT JOIN university_learning.world_cup.team_appearances team_appearances ON tournament_standings.tournament_id = team_appearances.tournament_id AND tournament_standings.team_id = team_appearances.team_id
GROUP BY tournaments.tournament_name, tournaments.year, tournament_standings.team_name, tournament_standings.team_code, tournament_standings.position
ORDER BY tournaments.year DESC, tournament_standings.position;

-- Most Successful Teams (Tournament Winners)
SELECT 
    teams.team_name,
    teams.team_code,
    COUNT(DISTINCT tournaments.tournament_id) AS championships_won,
    STRING_AGG(DISTINCT CAST(tournaments.year AS STRING), ', ' ORDER BY CAST(tournaments.year AS STRING)) AS winning_years
FROM university_learning.world_cup.teams teams
INNER JOIN university_learning.world_cup.tournaments tournaments ON teams.team_name = tournaments.winner
GROUP BY teams.team_name, teams.team_code
ORDER BY championships_won DESC;

-- ============================================================================
-- 4. MATCH STATISTICS
-- ============================================================================

-- High-Scoring Matches
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    matches.match_name,
    matches.match_date,
    matches.stage_name,
    matches.home_team_name,
    matches.away_team_name,
    matches.home_team_score,
    matches.away_team_score,
    matches.home_team_score + matches.away_team_score AS total_goals,
    matches.stadium_name,
    matches.city_name,
    matches.country_name
FROM university_learning.world_cup.matches matches
INNER JOIN university_learning.world_cup.tournaments tournaments ON matches.tournament_id = tournaments.tournament_id
WHERE matches.home_team_score + matches.away_team_score >= 6
ORDER BY total_goals DESC, tournaments.year DESC
LIMIT 50;

-- Match Statistics Summary
SELECT 
    COUNT(*) AS total_matches,
    SUM(CASE WHEN matches.home_team_win = 1 THEN 1 ELSE 0 END) AS home_wins,
    SUM(CASE WHEN matches.away_team_win = 1 THEN 1 ELSE 0 END) AS away_wins,
    SUM(CASE WHEN matches.draw = 1 THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN matches.extra_time = 1 THEN 1 ELSE 0 END) AS matches_with_extra_time,
    SUM(CASE WHEN matches.penalty_shootout = 1 THEN 1 ELSE 0 END) AS penalty_shootouts,
    ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) AS avg_goals_per_match,
    MAX(matches.home_team_score + matches.away_team_score) AS highest_scoring_match
FROM university_learning.world_cup.matches matches;

-- ============================================================================
-- 5. REFEREE STATISTICS
-- ============================================================================

-- Most Active Referees
SELECT 
    referees.referee_id,
    referees.family_name,
    referees.given_name,
    referees.country_name,
    confederations.confederation_name,
    COUNT(DISTINCT referee_appearances.match_id) AS matches_refereed,
    COUNT(DISTINCT referee_appearances.tournament_id) AS tournaments_participated
FROM university_learning.world_cup.referees referees
LEFT JOIN university_learning.world_cup.confederations confederations ON referees.confederation_id = confederations.confederation_id
LEFT JOIN university_learning.world_cup.referee_appearances referee_appearances ON referees.referee_id = referee_appearances.referee_id
GROUP BY referees.referee_id, referees.family_name, referees.given_name, referees.country_name, confederations.confederation_name
ORDER BY matches_refereed DESC
LIMIT 30;

-- Referee Appointments by Confederation
SELECT 
    confederations.confederation_name,
    COUNT(DISTINCT referee_appearances.referee_id) AS unique_referees,
    COUNT(referee_appearances.match_id) AS total_match_appointments
FROM university_learning.world_cup.referee_appearances referee_appearances
INNER JOIN university_learning.world_cup.confederations confederations ON referee_appearances.confederation_id = confederations.confederation_id
GROUP BY confederations.confederation_name
ORDER BY total_match_appointments DESC;

-- ============================================================================
-- 6. MANAGER STATISTICS
-- ============================================================================

-- Most Successful Managers
SELECT 
    managers.manager_id,
    managers.family_name,
    managers.given_name,
    managers.country_name,
    COUNT(DISTINCT manager_appearances.tournament_id) AS tournaments_managed,
    COUNT(DISTINCT manager_appearances.team_id) AS teams_managed,
    COUNT(DISTINCT manager_appearances.match_id) AS matches_managed,
    COUNT(DISTINCT CASE WHEN tournament_standings.position = 1 THEN tournament_standings.tournament_id END) AS championships_won
FROM university_learning.world_cup.managers managers
LEFT JOIN university_learning.world_cup.manager_appearances manager_appearances ON managers.manager_id = manager_appearances.manager_id
LEFT JOIN university_learning.world_cup.tournament_standings tournament_standings ON manager_appearances.tournament_id = tournament_standings.tournament_id AND manager_appearances.team_id = tournament_standings.team_id
GROUP BY managers.manager_id, managers.family_name, managers.given_name, managers.country_name
HAVING COUNT(DISTINCT manager_appearances.tournament_id) > 0
ORDER BY championships_won DESC, tournaments_managed DESC
LIMIT 30;

-- Manager Performance by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    managers.family_name || ', ' || managers.given_name AS manager_name,
    teams.team_name,
    tournament_standings.position AS final_position,
    COUNT(DISTINCT manager_appearances.match_id) AS matches_managed,
    SUM(CASE WHEN team_appearances.win = 1 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN team_appearances.draw = 1 THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN team_appearances.lose = 1 THEN 1 ELSE 0 END) AS losses
FROM university_learning.world_cup.manager_appearances manager_appearances
INNER JOIN university_learning.world_cup.tournaments tournaments ON manager_appearances.tournament_id = tournaments.tournament_id
INNER JOIN university_learning.world_cup.managers managers ON manager_appearances.manager_id = managers.manager_id
INNER JOIN university_learning.world_cup.teams teams ON manager_appearances.team_id = teams.team_id
LEFT JOIN university_learning.world_cup.tournament_standings tournament_standings ON manager_appearances.tournament_id = tournament_standings.tournament_id AND manager_appearances.team_id = tournament_standings.team_id
LEFT JOIN university_learning.world_cup.team_appearances team_appearances ON manager_appearances.tournament_id = team_appearances.tournament_id AND manager_appearances.team_id = team_appearances.team_id AND manager_appearances.match_id = team_appearances.match_id
GROUP BY tournaments.tournament_name, tournaments.year, managers.family_name, managers.given_name, teams.team_name, tournament_standings.position
ORDER BY tournaments.year DESC, tournament_standings.position;

-- ============================================================================
-- 7. STADIUM USAGE
-- ============================================================================

-- Stadium Usage Statistics
SELECT 
    stadiums.stadium_id,
    stadiums.stadium_name,
    stadiums.city_name,
    stadiums.country_name,
    stadiums.stadium_capacity,
    COUNT(DISTINCT matches.match_id) AS total_matches_hosted,
    COUNT(DISTINCT matches.tournament_id) AS tournaments_hosted,
    SUM(matches.home_team_score + matches.away_team_score) AS total_goals_scored,
    ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) AS avg_goals_per_match
FROM university_learning.world_cup.stadiums stadiums
LEFT JOIN university_learning.world_cup.matches matches ON stadiums.stadium_id = matches.stadium_id
GROUP BY stadiums.stadium_id, stadiums.stadium_name, stadiums.city_name, stadiums.country_name, stadiums.stadium_capacity
HAVING COUNT(DISTINCT matches.match_id) > 0
ORDER BY total_matches_hosted DESC;

-- Stadiums by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    stadiums.stadium_name,
    stadiums.city_name,
    stadiums.country_name,
    COUNT(matches.match_id) AS matches_hosted
FROM university_learning.world_cup.matches matches
INNER JOIN university_learning.world_cup.tournaments tournaments ON matches.tournament_id = tournaments.tournament_id
INNER JOIN university_learning.world_cup.stadiums stadiums ON matches.stadium_id = stadiums.stadium_id
GROUP BY tournaments.tournament_name, tournaments.year, stadiums.stadium_name, stadiums.city_name, stadiums.country_name
ORDER BY tournaments.year DESC, matches_hosted DESC;

-- ============================================================================
-- 8. AWARD WINNERS
-- ============================================================================

-- Award Winners Summary
SELECT 
    award_winners.tournament_name,
    tournaments.year,
    award_winners.award_name,
    award_winners.family_name || ', ' || award_winners.given_name AS player_name,
    award_winners.team_name,
    award_winners.shared
FROM university_learning.world_cup.award_winners award_winners
INNER JOIN university_learning.world_cup.tournaments tournaments ON award_winners.tournament_id = tournaments.tournament_id
ORDER BY tournaments.year DESC, award_winners.award_name;

-- Players with Most Awards
SELECT 
    players.player_id,
    players.family_name,
    players.given_name,
    COUNT(DISTINCT award_winners.award_id) AS unique_awards,
    COUNT(award_winners.award_id) AS total_awards,
    STRING_AGG(DISTINCT award_winners.award_name, ', ' ORDER BY award_winners.award_name) AS awards_won
FROM university_learning.world_cup.players players
INNER JOIN university_learning.world_cup.award_winners award_winners ON players.player_id = award_winners.player_id
GROUP BY players.player_id, players.family_name, players.given_name
ORDER BY total_awards DESC
LIMIT 20;

-- ============================================================================
-- 9. HISTORICAL TRENDS
-- ============================================================================

-- Goals Per Tournament Trend
SELECT 
    tournaments.year,
    tournaments.tournament_name,
    COUNT(DISTINCT matches.match_id) AS total_matches,
    SUM(matches.home_team_score + matches.away_team_score) AS total_goals,
    ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) AS avg_goals_per_match,
    MAX(matches.home_team_score + matches.away_team_score) AS highest_scoring_match
FROM university_learning.world_cup.tournaments tournaments
INNER JOIN university_learning.world_cup.matches matches ON tournaments.tournament_id = matches.tournament_id
GROUP BY tournaments.year, tournaments.tournament_name
ORDER BY tournaments.year;

-- Teams Participating Over Time
SELECT 
    tournaments.year,
    tournaments.tournament_name,
    COUNT(DISTINCT qualified_teams.team_id) AS number_of_teams
FROM university_learning.world_cup.tournaments tournaments
INNER JOIN university_learning.world_cup.qualified_teams qualified_teams ON tournaments.tournament_id = qualified_teams.tournament_id
GROUP BY tournaments.year, tournaments.tournament_name
ORDER BY tournaments.year;

-- ============================================================================
-- 10. CONFEDERATION PERFORMANCE
-- ============================================================================

-- Confederation Performance Summary
SELECT 
    confederations.confederation_name,
    confederations.confederation_code,
    COUNT(DISTINCT teams.team_id) AS total_teams,
    COUNT(DISTINCT qualified_teams.tournament_id) AS tournaments_participated,
    COUNT(DISTINCT CASE WHEN tournament_standings.position = 1 THEN tournament_standings.tournament_id END) AS championships_won,
    COUNT(DISTINCT team_appearances.match_id) AS total_matches,
    SUM(team_appearances.win) AS total_wins,
    SUM(team_appearances.draw) AS total_draws,
    SUM(team_appearances.lose) AS total_losses,
    SUM(team_appearances.goals_for) AS total_goals_for,
    SUM(team_appearances.goals_against) AS total_goals_against
FROM university_learning.world_cup.confederations confederations
LEFT JOIN university_learning.world_cup.teams teams ON confederations.confederation_id = teams.confederation_id
LEFT JOIN university_learning.world_cup.qualified_teams qualified_teams ON teams.team_id = qualified_teams.team_id
LEFT JOIN university_learning.world_cup.tournament_standings tournament_standings ON teams.team_id = tournament_standings.team_id
LEFT JOIN university_learning.world_cup.team_appearances team_appearances ON teams.team_id = team_appearances.team_id
GROUP BY confederations.confederation_name, confederations.confederation_code
ORDER BY championships_won DESC, total_wins DESC;

-- ============================================================================
-- 11. PLAYER APPEARANCES & STATISTICS
-- ============================================================================

-- Most Capped Players
SELECT 
    players.player_id,
    players.family_name,
    players.given_name,
    COUNT(DISTINCT player_appearances.tournament_id) AS tournaments_played,
    COUNT(DISTINCT player_appearances.match_id) AS total_appearances,
    SUM(player_appearances.starter) AS starts,
    SUM(player_appearances.substitute) AS substitute_appearances,
    COUNT(DISTINCT player_appearances.team_id) AS teams_represented
FROM university_learning.world_cup.players players
INNER JOIN university_learning.world_cup.player_appearances player_appearances ON players.player_id = player_appearances.player_id
GROUP BY players.player_id, players.family_name, players.given_name
ORDER BY total_appearances DESC
LIMIT 50;

-- Player Statistics by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    players.family_name || ', ' || players.given_name AS player_name,
    teams.team_name,
    COUNT(DISTINCT player_appearances.match_id) AS appearances,
    SUM(player_appearances.starter) AS starts,
    COUNT(DISTINCT goals.goal_id) AS goals_scored,
    COUNT(DISTINCT bookings.booking_id) AS bookings
FROM university_learning.world_cup.player_appearances player_appearances
INNER JOIN university_learning.world_cup.tournaments tournaments ON player_appearances.tournament_id = tournaments.tournament_id
INNER JOIN university_learning.world_cup.players players ON player_appearances.player_id = players.player_id
INNER JOIN university_learning.world_cup.teams teams ON player_appearances.team_id = teams.team_id
LEFT JOIN university_learning.world_cup.goals goals ON player_appearances.tournament_id = goals.tournament_id AND player_appearances.match_id = goals.match_id AND player_appearances.player_id = goals.player_id AND (goals.own_goal = 0 OR goals.own_goal IS NULL)
LEFT JOIN university_learning.world_cup.bookings bookings ON player_appearances.tournament_id = bookings.tournament_id AND player_appearances.match_id = bookings.match_id AND player_appearances.player_id = bookings.player_id
GROUP BY tournaments.tournament_name, tournaments.year, players.family_name, players.given_name, teams.team_name
ORDER BY tournaments.year DESC, goals_scored DESC, appearances DESC;

-- ============================================================================
-- 12. GROUP STAGE ANALYSIS
-- ============================================================================

-- Group Stage Standings Summary
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    group_standings.group_name,
    group_standings.team_name,
    group_standings.position,
    group_standings.played,
    group_standings.wins,
    group_standings.draws,
    group_standings.losses,
    group_standings.goals_for,
    group_standings.goals_against,
    group_standings.goal_difference,
    group_standings.points,
    group_standings.advanced
FROM university_learning.world_cup.group_standings group_standings
INNER JOIN university_learning.world_cup.tournaments tournaments ON group_standings.tournament_id = tournaments.tournament_id
ORDER BY tournaments.year DESC, group_standings.group_name, group_standings.position;

-- Group Stage Statistics
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    COUNT(DISTINCT group_standings.group_name) AS number_of_groups,
    COUNT(DISTINCT group_standings.team_id) AS teams_in_groups,
    ROUND(AVG(group_standings.points), 2) AS avg_points_per_team,
    ROUND(AVG(group_standings.goals_for), 2) AS avg_goals_for_per_team,
    ROUND(AVG(group_standings.goal_difference), 2) AS avg_goal_difference
FROM university_learning.world_cup.group_standings group_standings
INNER JOIN university_learning.world_cup.tournaments tournaments ON group_standings.tournament_id = tournaments.tournament_id
GROUP BY tournaments.tournament_name, tournaments.year
ORDER BY tournaments.year DESC;

-- ============================================================================
-- 13. KNOCKOUT STAGE ANALYSIS
-- ============================================================================

-- Knockout Stage Matches
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    matches.match_name,
    matches.stage_name,
    matches.match_date,
    matches.home_team_name,
    matches.away_team_name,
    matches.home_team_score,
    matches.away_team_score,
    CASE 
        WHEN matches.home_team_win = 1 THEN matches.home_team_name
        WHEN matches.away_team_win = 1 THEN matches.away_team_name
        ELSE 'Draw'
    END AS winner,
    matches.extra_time,
    matches.penalty_shootout,
    matches.home_team_score_penalties,
    matches.away_team_score_penalties
FROM university_learning.world_cup.matches matches
INNER JOIN university_learning.world_cup.tournaments tournaments ON matches.tournament_id = tournaments.tournament_id
WHERE matches.knockout_stage = 1
ORDER BY tournaments.year DESC, 
    CASE matches.stage_name
        WHEN 'Final' THEN 1
        WHEN 'Third place' THEN 2
        WHEN 'Semi-finals' THEN 3
        WHEN 'Quarter-finals' THEN 4
        WHEN 'Round of 16' THEN 5
        ELSE 6
    END,
    matches.match_date;

-- Penalty Shootout Statistics
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    COUNT(DISTINCT penalty_kicks.match_id) AS penalty_shootouts,
    COUNT(DISTINCT penalty_kicks.player_id) AS players_taking_penalties,
    SUM(penalty_kicks.converted) AS penalties_converted,
    COUNT(penalty_kicks.penalty_kick_id) AS total_penalties_taken,
    ROUND(SUM(penalty_kicks.converted) * 100.0 / COUNT(penalty_kicks.penalty_kick_id), 2) AS conversion_rate_pct
FROM university_learning.world_cup.penalty_kicks penalty_kicks
INNER JOIN university_learning.world_cup.tournaments tournaments ON penalty_kicks.tournament_id = tournaments.tournament_id
GROUP BY tournaments.tournament_name, tournaments.year
ORDER BY tournaments.year DESC;

-- ============================================================================
-- 14. BOOKINGS & DISCIPLINE
-- ============================================================================

-- Team Discipline Statistics
SELECT 
    teams.team_name,
    teams.team_code,
    COUNT(DISTINCT bookings.tournament_id) AS tournaments,
    COUNT(DISTINCT bookings.match_id) AS matches,
    SUM(bookings.yellow_card) AS total_yellow_cards,
    SUM(bookings.red_card) AS total_red_cards,
    SUM(bookings.second_yellow_card) AS second_yellow_cards,
    SUM(bookings.sending_off) AS total_sendings_off,
    ROUND(AVG(bookings.yellow_card + bookings.red_card), 2) AS avg_cards_per_match
FROM university_learning.world_cup.teams teams
INNER JOIN university_learning.world_cup.bookings bookings ON teams.team_id = bookings.team_id
GROUP BY teams.team_name, teams.team_code
ORDER BY total_yellow_cards + total_red_cards DESC
LIMIT 30;

-- Most Booked Players
SELECT 
    players.family_name || ', ' || players.given_name AS player_name,
    COUNT(DISTINCT bookings.tournament_id) AS tournaments,
    COUNT(DISTINCT bookings.match_id) AS matches,
    SUM(bookings.yellow_card) AS yellow_cards,
    SUM(bookings.red_card) AS red_cards,
    SUM(bookings.second_yellow_card) AS second_yellow_cards,
    SUM(bookings.sending_off) AS sendings_off
FROM university_learning.world_cup.players players
INNER JOIN university_learning.world_cup.bookings bookings ON players.player_id = bookings.player_id
GROUP BY players.family_name, players.given_name
ORDER BY yellow_cards + red_cards DESC
LIMIT 30;

-- ============================================================================
-- 15. SUBSTITUTIONS ANALYSIS
-- ============================================================================

-- Substitution Statistics by Tournament
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    COUNT(DISTINCT substitutions.match_id) AS matches_with_substitutions,
    COUNT(substitutions.substitution_id) AS total_substitutions,
    COUNT(DISTINCT substitutions.player_id) AS unique_players_substituted,
    COUNT(DISTINCT substitutions.team_id) AS teams_making_substitutions
FROM university_learning.world_cup.substitutions substitutions
INNER JOIN university_learning.world_cup.tournaments tournaments ON substitutions.tournament_id = tournaments.tournament_id
GROUP BY tournaments.tournament_name, tournaments.year
ORDER BY tournaments.year DESC;

-- Most Substituted Players
SELECT 
    players.family_name || ', ' || players.given_name AS player_name,
    COUNT(DISTINCT substitutions.tournament_id) AS tournaments,
    SUM(substitutions.going_off) AS times_substituted_off,
    SUM(substitutions.coming_on) AS times_substituted_on
FROM university_learning.world_cup.players players
INNER JOIN university_learning.world_cup.substitutions substitutions ON players.player_id = substitutions.player_id
GROUP BY players.family_name, players.given_name
ORDER BY times_substituted_off + times_substituted_on DESC
LIMIT 30;

-- ============================================================================
-- 16. HOST COUNTRY PERFORMANCE
-- ============================================================================

-- Host Country Performance
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    host_countries.team_name AS host_country,
    host_countries.performance AS host_performance,
    tournament_standings.position AS final_position,
    COUNT(DISTINCT team_appearances.match_id) AS matches_played,
    SUM(team_appearances.win) AS wins,
    SUM(team_appearances.draw) AS draws,
    SUM(team_appearances.lose) AS losses
FROM university_learning.world_cup.host_countries host_countries
INNER JOIN university_learning.world_cup.tournaments tournaments ON host_countries.tournament_id = tournaments.tournament_id
LEFT JOIN university_learning.world_cup.tournament_standings tournament_standings ON host_countries.tournament_id = tournament_standings.tournament_id AND host_countries.team_id = tournament_standings.team_id
LEFT JOIN university_learning.world_cup.team_appearances team_appearances ON host_countries.tournament_id = team_appearances.tournament_id AND host_countries.team_id = team_appearances.team_id
GROUP BY tournaments.tournament_name, tournaments.year, host_countries.team_name, host_countries.performance, tournament_standings.position
ORDER BY tournaments.year DESC;

-- ============================================================================
-- 17. RECENT TOURNAMENT DASHBOARD (Latest Tournament Focus)
-- ============================================================================

-- Latest Tournament Overview
SELECT 
    tournaments.tournament_name,
    tournaments.year,
    tournaments.host_country,
    tournaments.winner,
    tournaments.count_teams,
    (SELECT COUNT(DISTINCT matches.match_id) FROM university_learning.world_cup.matches matches WHERE matches.tournament_id = tournaments.tournament_id) AS total_matches,
    (SELECT SUM(matches.home_team_score + matches.away_team_score) FROM university_learning.world_cup.matches matches WHERE matches.tournament_id = tournaments.tournament_id) AS total_goals,
    (SELECT ROUND(AVG(matches.home_team_score + matches.away_team_score), 2) FROM university_learning.world_cup.matches matches WHERE matches.tournament_id = tournaments.tournament_id) AS avg_goals_per_match
FROM university_learning.world_cup.tournaments tournaments
ORDER BY tournaments.year DESC
LIMIT 1;

-- Latest Tournament Top Scorers
SELECT 
    players.family_name || ', ' || players.given_name AS player_name,
    teams.team_name,
    COUNT(goals.goal_id) AS goals_scored,
    SUM(CASE WHEN goals.penalty = 1 THEN 1 ELSE 0 END) AS penalty_goals
FROM university_learning.world_cup.goals goals
INNER JOIN university_learning.world_cup.tournaments tournaments ON goals.tournament_id = tournaments.tournament_id
INNER JOIN university_learning.world_cup.players players ON goals.player_id = players.player_id
INNER JOIN university_learning.world_cup.teams teams ON goals.team_id = teams.team_id
WHERE (goals.own_goal = 0 OR goals.own_goal IS NULL)
    AND tournaments.year = (SELECT MAX(tournaments.year) FROM university_learning.world_cup.tournaments tournaments)
GROUP BY players.family_name, players.given_name, teams.team_name
ORDER BY goals_scored DESC
LIMIT 10;

