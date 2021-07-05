#!/bin/bash

# Script para importar archivo sql a la base de datos aoe2de alojada en mariaDB y mover archivo sql a su directorio final.

# Rutas archivos sql.
matches=/home/mms959tfm/data_aoe2_de/sql/matches_data/*.sql
players=/home/mms959tfm/data_aoe2_de/sql/players_data/*.sql

# Bucle para recorrer todos los archivos matches_clean_data.sql
for match in $matches
do
	filename=$(basename $match .sql)
	date=$(echo $filename| cut -d'_' -f 4)
	year=$(echo $date| cut -d'-' -f 1)
	month=$(echo $date| cut -d'-' -f 2)
	day=$(echo $date| cut -d'-' -f 3)
	# Generación archivo log.
	echo "MATCHES" >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	echo "Importando archivo $filename.sql a mariaDB." >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	# Importación archivo sql a mariaDB.
	echo $filename
	mysql -u mms -pagedb aoe2de < $match |& tee -a /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	echo "Archivo $filename.sql importado correctamente." >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	# Mover archivo matches_clean_data.sql a su directorio final.
	mv /home/mms959tfm/data_aoe2_de/sql/matches_data/$filename.sql /home/mms959tfm/data_aoe2_de/sql/matches_data/$year/$month/
	echo "Archivo $filename.sql movido a su directorio final correctamente." >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
done

# Bucle para recorrer todos los archivos players_clean_data.sql
for player in $players
do
	filename=$(basename $player .sql)
	date=$(echo $filename| cut -d'_' -f 4)
	year=$(echo $date| cut -d'-' -f 1)
	month=$(echo $date| cut -d'-' -f 2)
	day=$(echo $date| cut -d'-' -f 3)
	# Generación archivo log.
	echo "PLAYERS" >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	echo "Importando archivo $filename.sql a mariaDB" >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	# Importación archivo sql a mariaDB.
	echo $filename
	mysql -u mms -pagedb aoe2de < $player |& tee -a /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	echo "Archivo $filename.sql importado correctamente." >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
	# Mover archivo players_clean_data.sql a su directorio final.
	mv /home/mms959tfm/data_aoe2_de/sql/players_data/$filename.sql /home/mms959tfm/data_aoe2_de/sql/players_data/$year/$month/
	echo "Archivo $filename.sql movido a su directorio final correctamente." >> /home/mms959tfm/data_aoe2_de/logs/04_importing_sql_files_into_mariadb/$year/$month/log_importing_sql_files_into_mariadb_$year-$month-$day.txt
done
