

# NCAA Basketball Dataset



## Examples
This example shows some basic NCAA Men's Basketball game analysis, using the public
Bigquery datasets, equivalent to [this lab](https://www.cloudskillsboost.google/focuses/624?parent=catalog)


## Find out which events happen most often?

Lab
```sql
#standardSQL
SELECT
  event_type,
  COUNT(*) AS event_count
FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr`
GROUP BY 1
ORDER BY event_count DESC;
```

Preql
```sql  
select 
    game_event.type,
    game_event.count
order by 
game_event.count
    desc;
```

### Games with the most 3 points made?

Lab
```sql
#standardSQL
#most three points made
SELECT
  scheduled_date,
  name,
  market,
  alias,
  three_points_att,
  three_points_made,
  three_points_pct,
  opp_name,
  opp_market,
  opp_alias,
  opp_three_points_att,
  opp_three_points_made,
  opp_three_points_pct,
  (three_points_made + opp_three_points_made) AS total_threes
FROM `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr`
WHERE season > 2010
ORDER BY total_threes DESC
LIMIT 5;

```


PreQL
```sql


SELECT
  game.id,
  game.scheduled_date,
  game.season,
  game.home_team.name,
  game.home_market,
  game.home_alias,
  game.home_three_points_att,
  game.home_three_points_made,
  game.home_three_points_pct,
  game.away_team.name,
  game.away_market,
  game.away_alias,
  game.away_three_points_att,
  game.away_three_points_made,
  game.away_three_points_pct,
  game.total_three_points_made
where game.season > 2010

ORDER BY 
    game.total_three_points_made desc
LIMIT 5
;

```