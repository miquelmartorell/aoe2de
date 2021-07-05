#!/bin/bash

# Script para inicializar script python 03_csv_to_sql_linux para generar archivo sql y mover archivo csv a su directorio final.

# Rutas archivos limpios csv.
matches=/home/mms959tfm/data_aoe2_de/csv/clean_data/matches_clean_data/*.csv
players=/home/mms959tfm/data_aoe2_de/csv/clean_data/players_clean_data/*.csv

# Bucle para recorrer todos los archivos matches_clean_data.csv
for match in $matches
do
	filename=$(basename $match .csv)
	date=$(echo $filename| cut -d'_' -f 4)
	year=$(echo $date| cut -d'-' -f 1)
	month=$(echo $date| cut -d'-' -f 2)
	day=$(echo $date| cut -d'-' -f 3)
	# Generación archivo log.
	echo $filename
	echo "MATCHES" >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	echo "Generando archivo $filename.sql" >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	# Generación archivo matches_clean_data.sql
	python3 /home/mms959tfm/data_aoe2_de/scripts/03_generating_clean_sql_data/03_csv_to_sql_linux.py -t matches $match > /home/mms959tfm/data_aoe2_de/sql/matches_data/$filename.sql
	echo "Archivo $filename.sql generado correctamente." >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	# Mover archivo matches_clean_data.csv a su directorio final.
	mv /home/mms959tfm/data_aoe2_de/csv/clean_data/matches_clean_data/$filename.csv /home/mms959tfm/data_aoe2_de/csv/clean_data/matches_clean_data/$year/$month/
	echo "Archivo $filename.csv movido a su directorio final correctamente." >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
done

# Bucle para recorrer todos los archivos players_clean_data.csv
for player in $players
do
	filename=$(basename $player .csv)
	date=$(echo $filename| cut -d'_' -f 4)
	year=$(echo $date| cut -d'-' -f 1)
	month=$(echo $date| cut -d'-' -f 2)
	day=$(echo $date| cut -d'-' -f 3)
	# Generación archivo log.
	echo $filename
	echo "PLAYERS" >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	echo "Generando archivo $filename.sql" >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	# Generación archivo players_clean_data.sql
	python3 /home/mms959tfm/data_aoe2_de/scripts/03_generating_clean_sql_data/03_csv_to_sql_linux.py -t players $player > /home/mms959tfm/data_aoe2_de/sql/players_data/$filename.sql
	echo "Archivo $filename.sql generado correctamente." >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
	# Mover archivo players_clean_data.csv a su directorio final.
	mv /home/mms959tfm/data_aoe2_de/csv/clean_data/players_clean_data/$filename.csv /home/mms959tfm/data_aoe2_de/csv/clean_data/players_clean_data/$year/$month/
	echo "Archivo $filename.csv movido a su directorio final correctamente." >> /home/mms959tfm/data_aoe2_de/logs/03_generating_clean_sql_data/$year/$month/log_generating_clean_sql_data_$year-$month-$day.txt
done
