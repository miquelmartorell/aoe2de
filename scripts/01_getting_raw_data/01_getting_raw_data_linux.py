#!/usr/bin/env python
# coding: utf-8

## LIBRERÍAS UTILIZADAS.

import requests
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td
import glob
import time

print("Bienvenido a la función escrita en Python para obtener datos de partidas clasificatorias del Age Of Empires II Definitive Edition a través de la API de aoe2.net.")
print("Esta función se encarga de obtener dos archivos CSV para cada día.")
print("El primer archivo, matches_raw_data, contiene información básica sobre las partidas clasificatorias.")
print("El segundo archivo, players_raw_data, contiene información básica sobre los jugadores que han participado en las partidas clasificatorias.")

## CONEXIÓN API AOE2.NET | matches.

# Definición de las columnas para el dataframe matches (partidas). 
matches_columns = ['match_id', 'lobby_id', 'match_uuid', 'version', 'name', 'num_players', 'num_slots', 'average_rating',
                    'cheats', 'full_tech_tree', 'ending_age', 'expansion', 'game_type', 'has_custom_content', 'has_password',
                    'lock_speed', 'lock_teams', 'map_size', 'map_type', 'pop', 'ranked', 'leaderboard_id', 'rating_type', 'resources',
                    'rms','scenario', 'server', 'shared_exploration', 'speed', 'starting_age', 'team_together', 'team_positions',
                    'treaty_length', 'turbo', 'victory', 'victory_time', 'visibility', 'opened', 'started', 'finished', 'players']

# Definición de las columnas para el dataframe players (jugadores). 
players_columns = ["profile_id", "steam_id", "name", "clan", "country", "slot", "slot_type", "rating",
                    "rating_change", "games", "wins", "streak", "drops", "color", "team", "civ", "won"]


# ## Función para obtener las partidas a través de la api de aoe2.net.
# 
# ### matches_api(parámetros)
# 
# #### Parámetros:
# 
# * game: Juego del cual queremos obtener las partidas.
#     * Game (Age of Empires 2:Definitive Edition=aoe2de)
# * start_date: Fecha de inicio en formato dd-mm-aaaa.
# * end_date: Fecha de fin (el día siguiente al día que quieres obtener). Es decir, el día marcado como fecha de fin no obtendremos ningún registro.
# * time_sleep: Número de segundos que deben transcurrir entre consultas a la API de aoe2.net
# * daily_tranche: Número de segundos que habrá entre cada tramo diario para obtener las 1.000 primeras partidas a partir de ese momento.
# 
# #### Devolución:
# Devuelve dataframes de partidas y jugadores por día guardados en archivos .csv
# 
# Cada archivo .csv del directorio matches contendrá las partidas ranked (clasificatorias) jugadas en un día.  
# Cada archivo .csv del directorio players contendrá los jugadores que han participado en las partidas ranked (clasificatorias) jugadas en un día.  
# Nota: Se tarda una media de 120 minutos para obtener todos los datos de partidas de un día.

# Función para obtener las partidas a través de la api de aoe2.net.
def matches_api(game, start_date, end_date, path_matches, path_players, time_sleep, tranche_seconds):
    
    # Conversión del formato string de la fecha de inicio y de la fecha fin en integer(timestamp GMT+0). 
    string_date = " 00:00:00 +0000"
    ##  Fecha de inicio.
    start_date = start_date + string_date
    start_date_dt = dt.strptime(start_date, "%d-%m-%Y %H:%M:%S %z")
    start = int(dt.timestamp(start_date_dt))
    ##  Fecha de fin.
    end_date = end_date + string_date
    end_date_dt = dt.strptime(end_date, "%d-%m-%Y %H:%M:%S %z")
    end = int(dt.timestamp(end_date_dt))
    
    # Definición del rango de días entre el intervalo de la fecha de inicio y la fecha de fin.
    days = range(start, end, 86400) #86400segundos = 60segundos * 60minutos * 24horas
    
    # Bucle para recorrer todos los días del rango.
    for day in days:
        ## Creación de los dataframes matches y players.
        matches = pd.DataFrame(columns=matches_columns)
        players = pd.DataFrame(columns=players_columns)
        ## Definición de los tramos diarios. Por defecto, el valor es de 60 segundos (1 minuto).
        ## Se trata de una restricción de la propia API, ya que por cada petición sólo podemos obtener, como máximo, 1.000 registros de partidas.
        daily_tranches = range(day, day+86400, int(tranche_seconds))
        short_date = dt.utcfromtimestamp(day).strftime("%d-%m-%Y")
        ## creación de archivo logfile.
        ### path log.
        year_date = str(short_date).split("-")[2]
        month_date = str(short_date).split("-")[1] 
        short_date_log = dt.utcfromtimestamp(day).strftime("%Y-%m-%d")
        path_log = "/home/mms959tfm/data_aoe2_de/logs/01_getting_raw_data/" + year_date + "/" + month_date + "/"
        logfile_getting_raw_data = open(path_log + "/log_getting_raw_data_" + str(short_date_log) + ".txt", "a")
        logfile_getting_raw_data.write(
            "DATE: " + str(short_date) + "\n"
        )
        print("Importando partidas clasificatorias del día " + short_date)
        
        # Bucle para recorrer todos los tramos diarios.
        for daily_tranche in daily_tranches:
            success = False
            while not success:
                try:
                    date = dt.utcfromtimestamp(daily_tranche).strftime("%d-%m-%Y %H:%M:%S")
                    # Petición a  la api de aoe2.net
                    request = requests.get("https://aoe2.net/api/matches?game=" + game + "&count=1000&since=" + str(daily_tranche))
                    # Recepción de los datos de las partidas en formato json.
                    json = request.json()
                    # Creación del dataframe matches_to_append a partir de los datos recibidos json.
                    matches_to_append = pd.DataFrame(json)
                    # Filtración de partidas: Sólo mantener partidas ranked (clasificatorias).
                    matches_to_append = matches_to_append[matches_to_append["ranked"]==True]
                    # Mostrar por pantalla las partidas ranked encontradas por tramo diario.
                    print(date, "Partidas clasificatorias encontradas: " + str(matches_to_append.shape[0]))
                    # Adjuntar los datos del dataframe matches_to_append al dataframe matches.
                    matches = matches.append(matches_to_append)
                    # Actualizar archivo logfile.
                    logfile_getting_raw_data.write(
                        date + " RANKED MATCHES FOUND: " + str(matches_to_append.shape[0]) + "\n"
                    )
                    # Pausamos x segundos entre petición y petición a la API.
                    time.sleep(int(time_sleep))
                    success = True
                except Exception as error:
                    # Actualizar archivo logfile.
                    logfile_getting_raw_data.write(
                        date + " ERROR: " + str(error) + "\n"
                    )
                    # Pausamos x segundos entre petición y petición a la API.
                    time.sleep(int(time_sleep))
            
        # En éste punto, el algoritmo ha terminado de recorrer todo el día.
        
        # Obtención de la fecha en formato corto dd-mm-aaaa.
        short_date = dt.utcfromtimestamp(daily_tranche).strftime("%d-%m-%Y")
        # Separación de la lista players del dataframe matches para incluirla en el dataframe players.
        ## Eliminar todas las columnas del dataframe matches excepto match_id y players. Guardar resultado en variable players_raw.
        players_raw = matches.drop(matches.iloc[:, 1:40], inplace = False, axis = 1)
        ## Establecer match_id como índice.
        players_raw = players_raw.set_index("match_id")
        players_raw.sort_index(inplace=True)
        ## Aplicar pd.Series a la columna players de la nueva variable players_raw_2. Descomponemos la variable players en 8 columnas.
        players_raw_2 = players_raw["players"].apply(pd.Series)
        ## El número máximo de jugadores que pueden participar en una partida de AoEDE2 son de 8 jugadores.
        num_players = 8
        ## Bucle para recorrer cada columna. Aplicación nuevamente de pd.Series para su descomposición.
        for player in range(num_players):
            ### Creación dataframe players_to_append a partir de players_raw_2.
            players_to_append = players_raw_2[player].apply(pd.Series)
            players_to_append.reset_index(inplace=True)
            ### Si estamos en la columna 2 o superior, se nos crear una columna adicional 0. Debemos eliminarla.
            if player>=2:
                #### Eliminar columna 0.
                del players_to_append[0]
                #### Reordenar columnas dataframe players_to_append.
                players_to_append = players_to_append[["match_id", "profile_id", "steam_id", "name", "clan", "country", "slot", "slot_type", "rating",
                                                        "rating_change", "games", "wins", "streak", "drops", "color", "team", "civ", "won"]]
            ### Adjuntar los datos del dataframe players_to_append al dataframe players.
            players = players.append(players_to_append)
            ### Reordenar columnas dataframe players.
            players = players[["match_id", "profile_id", "steam_id", "name", "clan", "country", "slot", "slot_type", "rating",
                                "rating_change", "games", "wins", "streak", "drops", "color", "team", "civ", "won"]]
        ## Ordenar valores dataframe players por los valores de la columna match_id.
        players.sort_values(by=["match_id"], inplace=True)
        ## Restablecer/eliminar índices del dataframe players.
        players.reset_index(drop=True, inplace=True)
        ## Eliminar todas las filas que contengan NaN en todas sus columnas excepto la columna match_id. 
        players.dropna(thresh=2, inplace=True)
        ## Eliminar columna players del dataframe matches.
        matches.drop(["players"], axis=1, inplace=True)

        # Mostrar por pantalla el resumen del día de las partidas.
        print("Resumen partidas clasificatorias del día " + short_date)
        print("Partidas clasificatorias encontradas: " + str(matches.shape[0]))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write("\n" +
            "SUMMARY RANKED MATCHES " + str(short_date) + "\n" +
            "RANKED MATCHES FOUND: " + str(matches.shape[0]) + "\n"
        )
        ## Comprobación de posibles partidas duplicadas en el datagrame matches.
        duplicated_matches = matches.shape[0] - len(matches["match_id"].unique())
        print("Partidas clasificatorias duplicadas: " + str(duplicated_matches))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write(
            "DUPLICATED RANKED MATCHES FOUND: " + str(duplicated_matches) + "\n"
        )
        if duplicated_matches != 0:
            ## Eliminar partidas duplicadas del datframe matches.
            matches.drop_duplicates(inplace=True)
        print("Total partidas clasificatorias: " + str(matches.shape[0]))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write(
            "TOTAL RANKED MATCHES: " + str(matches.shape[0]) + "\n"
        )

        # Mostrar por pantalla el resumen del día de los jugadores que han participado en las partidas.
        print("Jugadores de partidas clasificatorias encontrados: " + str(players.shape[0]))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write(
            "RANKED PLAYERS FOUND: " + str(players.shape[0]) + "\n"
        )
        ## Comprobación de posibles jugadores duplicados en el datagrame players.
        duplicated_players = players[players.duplicated() ==1].shape[0]
        print("Jugadores de partidas clasificatorias duplicados: " + str(duplicated_players))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write(
            "DUPLICATED RANKED PLAYERS FOUND: " + str(duplicated_players) + "\n"
        )
        if duplicated_players != 0:
            ## Eliminar jugadores duplicados del dataframe players (en una misma partida). Esto no significa que un jugador no pueda aparecer muchas veces,
            ## sino que en cada partida sólamente puede aparecer una única vez.
            players.drop_duplicates(inplace=True)
        print("Total jugadores de partidas clasificatorias: " + str(players.shape[0]))
        ## Actualizar archivo logfile.
        logfile_getting_raw_data.write(
            "TOTAL RANKED PLAYERS: " + str(players.shape[0]) + "\n"
        )
        
        # Renombrar columna name del dataframe matches por match_name.
        matches.rename(columns={"name": "match_name"}, inplace=True)
        # Renombrar columna name del dataframe players por player_name.
        players.rename(columns={"name": "player_name"}, inplace=True)
        
        # Guardar partidas ranked diarias en archivo csv.
        short_date = dt.utcfromtimestamp(day).strftime("%Y-%m-%d")
        matches.to_csv(path_matches + "matches_raw_data_" + str(short_date) + ".csv", header=True, index=False)
        ## Mostrar por pantalla que el archivo se ha generado correctamente.
        print("Archivo matches_raw_data_" + str(short_date) + ".csv generado correctamente.")
        # Guardar jugadores que han participado en partidas ranked diarias en archivo csv.
        players.to_csv(path_players + "players_raw_data_" + str(short_date) + ".csv", header=True, index=False)
        ## Mostrar por pantalla que el archivo se ha generado correctamente.
        print("Archivo players_raw_data_" + str(short_date) + ".csv generado correctamente.")
        print("--------------------------------------------------------------------------------------------------------------------")
        


# In[ ]:


# Pasar como parámetros:
#   - game: Juego del cual queremos obtener las partidas.
#   - start_date: Fecha de inicio en formato dd-mm-aaaa.
#   - end_date: Fecha de fin (el día siguiente al día final que quieres obtener). Es decir, el día marcado como fecha de fin no obtendremos ningún registro.
#   - time_sleep: Número de segundos entre peticiones a la api de aoe2.net.
#   - daily_tranche: Número de segundos que habrá entre cada tramo diario para obtener las 1.000 primeras partidas a partir de ese momento.
#   Cada archivo .csv del directorio matches contendrá las partidas ranked (clasificatorias) jugadas en un día.
#   Cada archivo .csv del directorio players contendrá los jugadores que han participado en las partidas ranked (clasificatorias) jugadas en un día.

# print("Por favor, introducir a continuación la información requerida: \n")
# start_date = input("Introducir la fecha de inicio en formato dd-mm-aaaa: \n")
# end_date = input("Introducir la fecha de fin en formato dd-mm-aaaa (el día siguiente al día final que quieres obtener): \n")
# time_sleep = input("Introducir el número de segundos que deben transcurrir entre consultas a la API de aoe2.net (pausa): \n")
# tranche_seconds = input("Introducir el número de segundos que habrá entre cada tramo diario para obtener las 1.000 primeras partidas a partir de ese momento: \n")

game = "aoe2de"
start_date = dt.today() - td(days=1)
start_date = start_date.strftime ('%d-%m-%Y')
end_date = dt.today().strftime ('%d-%m-%Y')
start_date = str(start_date)
end_date = str(end_date)
path_matches = "/home/mms959tfm/data_aoe2_de/csv/raw_data/matches_raw_data/"
path_players = "/home/mms959tfm/data_aoe2_de/csv/raw_data/players_raw_data/"
time_sleep = 1
tranche_seconds = 300
matches_api(game, start_date, end_date, path_matches, path_players, time_sleep, tranche_seconds)

