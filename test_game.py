import nfldb
from nfldb.db import Tx
from nfldb.update import *

thisDB = nfldb.connect()
update_game_schedules(thisDB)
update_games(thisDB, batch_size=16)
with Tx(thisDB) as cursor:
    update_season_state(cursor)
    update_players(cursor, 30)
queryObject = nfldb.Query(thisDB)

queryObject.game(season_year=2012, season_type='Regular')
for pp in queryObject.sort('rushing_yds').limit(5).as_aggregate():
    print(pp.player, pp.rushing_yds)

# simulatedGame = game_from_id(Tx(thisDB), "2009080950")
with Tx(thisDB) as cursor:
    gamesInProgress = games_in_progress(cursor)
    simulatedGame = game_from_id(cursor, "2009080950")
    print(simulatedGame)