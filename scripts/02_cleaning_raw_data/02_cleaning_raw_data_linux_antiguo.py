#!/usr/bin/env python
# coding: utf-8

### LIBRERIAS UTILIZADAS.

import requests
import pandas as pd
import numpy as np
from datetime import datetime as dt
import glob, sys, os, time, shutil

# Dar tiempo suficiente a que se generen los dos archivos csv (matches_raw_data y players_raw_data).
time.sleep(30)

# Directorios acceso archivos matches y players.
path_raw_matches = "/home/mms959tfm/data_aoe2_de/csv/raw_data/matches_raw_data/"
path_raw_players = "/home/mms959tfm/data_aoe2_de/csv/raw_data/players_raw_data/"

# Función para obtener los strings a través de la api de aoe2.net.
def strings_api(game, language):
    url_strings = "https://aoe2.net/api/strings?game=" + game + "&language=" + language + ""
    strings = requests.get(url_strings).json()
    keys_strings = sorted(strings.keys())
    return keys_strings, strings

keys_strings = strings_api("aoe2de", "en")[0]
strings = strings_api("aoe2de", "en")[1]
map_size = pd.DataFrame(strings["map_size"])
map_type = pd.DataFrame(strings["map_type"])
rating_type = pd.DataFrame(strings["rating_type"])

# Creación del string de la variable color.
data_color = {"id": [1, 2, 3, 4, 5, 6, 7, 8], 
                "string": ["blue", "red", "green", "yellow", "cyan", "purple", "orange", "grey"]}
color = pd.DataFrame(data=data_color)

### CARGA DE DATOS DIARIOS BRUTOS.

files_matches = glob.glob(path_raw_matches + "*.csv")
files_matches = sorted(files_matches)
files_players = glob.glob(path_raw_players + "*.csv")
files_players = sorted(files_players)

for file_matches in files_matches:
    
    ########## matches ##########
    
    # Creación del dataframe matches.
    matches = pd.read_csv(file_matches, index_col=None, header=0)
    
    # Cargar el csv de player correspondiente al fichero matches.
    namefile_matches = file_matches.split("/")[7]
    file_players = file_matches.replace("matches_raw_data", "players_raw_data")
    namefile_players = file_players.split("/")[7]
    final_path_players = path_raw_players + namefile_players
    print(namefile_matches, " ; " ,namefile_players)
    
    # Creación del dataframe players.
    players = pd.read_csv(final_path_players, index_col=None, header=0)
    
    # Columnas a eliminar del dataframe matches.
    matches_columns_to_drop  = ['lobby_id', 'match_uuid', 'match_name', 'num_slots', 'average_rating', 'cheats',
                                'full_tech_tree', 'ending_age', 'expansion', 'game_type', 'has_custom_content',
                                'has_password', 'leaderboard_id', 'lock_speed', 'lock_teams', 'pop', 'ranked',
                                'resources', 'rms', 'scenario', 'shared_exploration', 'speed', 'starting_age',
                                'team_together', 'team_positions', 'treaty_length', 'turbo', 'victory',
                                'victory_time', 'visibility', 'opened']
    # Eliminación de las columnas.
    matches.drop(columns=matches_columns_to_drop, inplace=True)
    
    # Variables temporales. Estas incluyen la duración de la partida en segundos y en minutos,
    # así como el momento de inicio y de fin de la partida en formato datetime.
    matches["duration"] = matches["finished"] - matches["started"]
    matches["started_date"] = pd.to_datetime(matches["started"], unit="s")
    matches["finished_date"] = pd.to_datetime(matches["finished"], unit="s")
    matches["duration_minutes"] = round(matches["duration"]/60)
    
    # Eliminación de partidas duplicadas en matches.
    matches.drop_duplicates(keep="first", inplace=True)
    
    # Guardar número de partidas iniciales.
    initial_matches = matches.shape[0]
    
    # Filtración de las partidas que realmente han empezado en el día natural.
    # Obtener día inicial en formato string.
    date = namefile_matches.split("_")[3].split(".")[0]
    hour = " 00:00:00 +0000"
    started_date = date + hour
    # Conversión a timestamp. Vigilar la zona horaria.
    started_date_dt = dt.strptime(started_date, "%Y-%m-%d %H:%M:%S %z")
    started_date_dt = int(dt.timestamp(started_date_dt))
    # Al timestamp inicial debemos sumarle 86400 segundos para obtener el timestamp del día siguiente.
    finished_date_dt = started_date_dt + 86400
    # Conversión del timestamp del día siguiente a string.
    finished_date = dt.utcfromtimestamp(finished_date_dt).strftime("%Y-%m-%d %H:%M:%S")
    # Obtener de nuevo el día inicial en formato string, ya sin la zona horaria. 
    hour = " 00:00:00"
    started_date  = date + hour
    # Ahora ya se puede filtrar correctamente.
    matches = matches[(matches["started_date"]>=started_date)&(matches["started_date"]<finished_date)]
    
    # Sustitución de las llaves ID por sus respectivos Strings en el dataframe matches.
    ## map_size.
    matches = pd.merge(left=matches , right=map_size, how="left", left_on="map_size", right_on="id")
    matches["map_size"] = matches["string"]
    matches.drop(["string", "id"], axis=1, inplace=True)
    ## map_type.
    matches = pd.merge(left=matches , right=map_type, how="left", left_on="map_type", right_on="id")
    matches["map_type"] = matches["string"]
    matches.drop(["string", "id"], axis=1, inplace=True)
    ## rating_type.
    matches = pd.merge(left=matches , right=rating_type, how="left", left_on="rating_type", right_on="id")
    matches["rating_type"] = matches["string"]
    matches.drop(["string", "id"], axis=1, inplace=True)
    # Inferir variable version desde noviembre 2019 hasta marzo 2020, ya que era una variable que no se recopilaba.
    indexes = matches[(matches["started_date"]>="2019-12-04 00:00:00") &
                        (matches["started_date"]<"2019-12-17 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 33315
    indexes = matches[(matches["started_date"]>="2019-12-17 00:00:00") &
                        (matches["started_date"]<"2019-12-20 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 34055
    indexes = matches[(matches["started_date"]>="2019-12-20 00:00:00") &
                        (matches["started_date"]<"2020-01-13 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 34223        
    indexes = matches[(matches["started_date"]>="2020-01-13 00:00:00") &
                        (matches["started_date"]<"2020-01-21 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 34397
    indexes = matches[(matches["started_date"]>="2020-01-21 00:00:00") &
                        (matches["started_date"]<"2020-01-23 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 34699
    indexes = matches[(matches["started_date"]>="2020-01-23 00:00:00") &
                        (matches["started_date"]<"2020-02-13 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 34793
    indexes = matches[(matches["started_date"]>="2020-02-13 00:00:00") &
                        (matches["started_date"]<"2020-02-27 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 35209
    indexes = matches[(matches["started_date"]>="2020-02-27 00:00:00") &
                        (matches["started_date"]<"2020-03-30 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 35584
    indexes = matches[(matches["started_date"]>="2020-03-30 00:00:00") &
                        (matches["started_date"]<"2020-04-29 00:00:00") &
                        (matches["version"].isna()==True)].index
    matches.loc[indexes, "version"] = 36202        
    
    # Eliminación de partidas donde versión es NaN.
    rows_to_drop = matches[matches["version"].isna()==True].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas donde num_players es impar.
    ## Eliminación de  partidas donde el número de jugadores es 0, 1, 3, 5, 7 o NaN, 
    ## ya que las partidas sólo pueden ser 1v1, 2v2, 3v3 o 4v4, es decir, 2, 4, 6 ó 8.
    rows_to_drop = matches[(matches["num_players"]==0) |
                            (matches["num_players"]==1) |
                            (matches["num_players"]==3) |
                            (matches["num_players"]==5) |
                            (matches["num_players"]==7) |
                            (matches["num_players"].isna()==True)].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas donde el tamaño del mapa de la partida es NaN o "Small (3 player)", ya que es para la modalidad Deatmatch (Combate Total). 
    rows_to_drop = matches[(matches["map_size"].isna()==True ) |
                           (["map_size"]=="Small (3 player)")].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas donde el mapa de la partida es NaN.
    rows_to_drop = matches[matches["map_type"].isna()==True].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas donde rating_type no sea Random Map o bien sean NaN.
    ## Eliminamos la modalidad de "Combate Total", al ser muy poco representativa, apenas un 2% de las partidas clasificatorias jugadas pertenecen a esta modalidad.
    ## Tambien eliminamos los NaN.
    rows_to_drop = matches[(matches["rating_type"].isna()==True) | (matches["rating_type"].str.contains("Death"))].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas donde la finalización de la partida es NaN.
    rows_to_drop = matches[matches["duration"].isna()==True].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas que tienen una duración negativa, es decir, donde el timestamp finished es inferior al timestamp started.
    rows_to_drop = matches[matches["duration"]<0].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de partidas que tienen una duración superior a las 4 horas.
    rows_to_drop = matches[matches["duration_minutes"]>240].index
    matches.drop(index=rows_to_drop, inplace=True)
    
    # Rellenar NaN de la variable server por la palabra "unknown".
    matches["server"].fillna("unknown", inplace=True)
    
    # Conversión de variables a int64.
    matches["match_id"] = matches["match_id"].astype("int64")
    matches["version"] = matches["version"].astype("int64")
    matches["num_players"] = matches["num_players"].astype("int64")
    matches["started"] = matches["started"].astype("int64")
    matches["finished"] = matches["finished"].astype("int64")
    matches["duration"] = matches["duration"].astype("int64")
    matches["duration_minutes"] = matches["duration_minutes"].astype("int64")
    
    ########## players ##########
    # Columnas a eliminar del dataframe players.
    players_columns_to_drop  = ['clan', 'slot_type', 'games', 'wins', 'streak', 'drops']
    # Eliminación de las columnas.
    players.drop(columns=players_columns_to_drop, inplace=True)
    
    # Eliminación de jugadores duplicados en players.
    players.drop_duplicates(keep="first", inplace=True)
    
    # Sustitución de las llaves ID por sus respectivos Strings en el dataframe players.
    matches.reset_index(inplace=True, drop=True)
    day = str(matches.loc[0, "started_date"])
    day = dt.strptime(day, "%Y-%m-%d %H:%M:%S")
    date_civs_02 = "2021-01-26 00:00:00"
    date_civs_02 = dt.strptime(date_civs_02, "%Y-%m-%d %H:%M:%S")
    if day < date_civs_02:
    # Civs antigüas 01:
        civ = pd.read_csv("/home/mms959tfm/data_aoe2_de/civs/civs_01.csv")
    else:
    # Nuevas civs 02:
        civ = pd.read_csv("/home/mms959tfm/data_aoe2_de/civs/civs_02.csv")
    ## civ.
    players = pd.merge(left=players , right=civ, how="left", left_on="civ", right_on="id")
    players["civ"] = players["string"]
    players.drop(["string", "id"], axis=1, inplace=True)
    ## color.
    players = pd.merge(left=players , right=color, how="left", left_on="color", right_on="id")
    players["color"] = players["string"]
    players.drop(["string", "id"], axis=1, inplace=True)
    
    # Comprobación si en una misma partida tenemos algún jugador o jugadores con la variable won NaN
    # y algún jugador o jugadores con la variable rellenada con True/False.
    # Eliminación de estas partidas.
    matches_nan_won = players[players["won"].isna()==True]
    matches_notnan_won = players[players["won"].isna()==False]
    matches_nan_won_list = matches_nan_won["match_id"].unique().tolist()
    matches_notnan_won_list = matches_notnan_won["match_id"].unique().tolist()
    matches_to_review = set(matches_nan_won_list).intersection(matches_notnan_won_list)
    for match in matches_to_review:
        rows_to_drop_players = players[players["match_id"]==match].index
        players.drop(index=rows_to_drop_players, inplace=True)
          
    # Eliminación de jugadores donde desconocemos el ganador y el perdedor de la partida.
    rows_to_drop = players[players["won"].isna()==True].index
    players.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de jugadores donde profile_id es NaN.
    rows_to_drop = players[players["profile_id"].isna()==True].index
    players.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de jugadores donde desconocemos la civilización del jugador.
    rows_to_drop = players[players["civ"].isna()==True].index
    players.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de jugadores donde desconocemos el equipo al que pertenece el jugador.
    rows_to_drop = players[players["team"].isna()==True].index
    players.drop(index=rows_to_drop, inplace=True)
    
    # Eliminación de jugadores donde el team es 0, -1, 3 ó 4.
    matches_to_drop = players["match_id"][(players["team"]==0) | (players["team"]==-1) |
                        (players["team"]==3) | (players["team"]==4)]
    rows_to_drop = []
    for match in matches_to_drop:
        rows_to_append = players[players["match_id"]==match].index.tolist()
        rows_to_drop.extend(rows_to_append)
    players.drop(index=rows_to_drop, inplace=True)
    
    # Conversión de variables a int64.
    players["match_id"] = players["match_id"].astype("int64")
    players["profile_id"] = players["profile_id"].astype("int64")
    players["slot"] = players["slot"].astype("int64")
    players["team"] = players["team"].astype("int64")
    
    # Conversión de variables a bool.
    players["won"] = players["won"].astype("bool")
    
    ########## merge ##########
    
    # Finalmente, después de la limpieza de datos, agrupar datos con merge y
    # creamos matches y players definitivos (con los datos verificados correctos.)
    df = players.merge(matches, how="inner", left_on="match_id", right_on="match_id")
    matches = df.iloc[:, [0,12,13,14,15,16,17,18,19,20,21,22,23]]
    players = df.iloc[:,0:12]
    
    # Eliminación de partidas duplicadas en matches.
    matches.drop_duplicates(subset="match_id", keep="first", inplace=True, ignore_index=True)
    
    # Guardar número de partidas finales.
    final_matches = matches.shape[0]
    
    # Check para comprobar la integridad de los datos de cada dataframe.
    if (matches.iloc[:, 0].unique().tolist())!=(players.iloc[:, 0].unique().tolist()):
        sys.exit("Se ha producido un error en el proceso de limpieza de datos. Por favor, revise")
    
    ########## logs ##########
    ## Dates.
    started_date = started_date.split()[0]
    year_date = started_date.split()[0].split("-")[0]
    month_date = started_date.split()[0].split("-")[1]
    
    ## NaN.
    matches_nan = matches.isna().sum()
    players_nan = players.isna().sum()
    
    ## Porcentaje de partidas que mantenemos después de la limpieza.
    percentage_matches = round(final_matches / initial_matches * 100, 2)
    
    ## matches.
    matches_version = matches["version"].value_counts(normalize=True, dropna=False)
    matches_num_players = matches["num_players"].value_counts(normalize=True, dropna=False)
    matches_map_size = matches["map_size"].value_counts(normalize=True, dropna=False)
    matches_map_type = matches["map_type"].value_counts(normalize=True, dropna=False)
    matches_rating_type = matches["rating_type"].value_counts(normalize=True, dropna=False)
    matches_server = matches["server"].value_counts(normalize=True, dropna=False)
    
    ## players.
    players_slot = players["slot"].value_counts(normalize=True, dropna=False)
    players_rating_change = players["rating_change"].value_counts(normalize=True, dropna=False)
    players_color = players["color"].value_counts(normalize=True, dropna=False)
    players_team = players["team"].value_counts(normalize=True, dropna=False)
    players_civ = players["civ"].value_counts(normalize=True, dropna=False)
    players_won = players["won"].value_counts(normalize=True, dropna=False)
    
    ## Creación de archivo logfile.
    ### path log.
    path_log = "/home/mms959tfm/data_aoe2_de/logs/02_cleaning_raw_data/" + year_date + "/" + month_date + "/"
    
    ## logfile.
    logfile_cleaning_raw_data = open(path_log + "/log_cleaning_raw_data_" + str(started_date) + ".txt", "a")
    logfile_cleaning_raw_data.write(
        "DATE: " + str(started_date) + "\n" + "\n" +
        "FILE MATCHES: " + str(namefile_matches) + "\n" +
        "FILE PLAYERS: " + str(namefile_players) + "\n" +        
        "FIRST MATCH DATE: " + str(min(matches["started_date"])) + "\n" +
        "LAST MATCH DATE: " + str(max(matches["started_date"])) + "\n" +
        "INITIAL MATCHES: "+ str(initial_matches) + "\n" +
        "FINAL MATCHES: "+ str(final_matches) + "\n" +
        "PERCENTAGE MATCHES: "+ str(percentage_matches) + "\n" + "\n" +
        "MATCHES NAN: "  + "\n" + str(matches_nan) + "\n" + "\n" +
        "PLAYERS NAN: "  + "\n" + str(players_nan) + "\n" + "\n" +
        "VERSION: "  + "\n" + str(matches_version) + "\n" + "\n" +
        "NUM PLAYERS: "  + "\n" + str(matches_num_players) + "\n" + "\n" +
        "MAP SIZE: "  + "\n" + str(matches_map_size) + "\n" + "\n" +
        "MAP TYPE: "  + "\n" + str(matches_map_type) + "\n" + "\n" +
        "RATING TYPE: "  + "\n" + str(matches_rating_type) + "\n" + "\n" +
        "SERVER: "  + "\n" + str(matches_server) + "\n" + "\n" +
        "SLOT: "  + "\n" + str(players_slot) + "\n" + "\n" +
        "RATING CHANGE: "  + "\n" + str(players_rating_change) + "\n" + "\n" +
        "COLOR: "  + "\n" + str(players_color) + "\n" + "\n" +
        "TEAM: "  + "\n" + str(players_team) + "\n" + "\n" +
        "CIV: "  + "\n" + str(players_civ) + "\n" + "\n" +
        "WON: "  + "\n" + str(players_won) + "\n" + "\n"
    )
    
    ## Guardar datos en archivos csv.
    path_save_clean_matches = "/home/mms959tfm/data_aoe2_de/csv/clean_data/matches_clean_data/" 
    matches.to_csv(path_or_buf= path_save_clean_matches + "/matches_clean_data_" + str(started_date) + ".csv", header=True, index=False)
    path_save_clean_players = "/home/mms959tfm/data_aoe2_de/csv/clean_data/players_clean_data/"   
    players.to_csv(path_or_buf= path_save_clean_players + "/players_clean_data_" + str(started_date) + ".csv", header=True, index=False)

    ## Mover archivos raw data a su directorio final.
    shutil.move(path_raw_matches + namefile_matches, path_raw_matches + str(year_date) + "/" + str(month_date) + "/")
    shutil.move(path_raw_players + namefile_players, path_raw_players + str(year_date) + "/" + str(month_date) + "/")
    
