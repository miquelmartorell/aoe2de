-- Age Of Empires II Definitive Edition Database Schema
-- Version 1.0
-- Copyright (c) 2021.

SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

DROP SCHEMA IF EXISTS aoe2de;
CREATE SCHEMA aoe2de;
USE aoe2de;

--
-- Table structure for table `matches`
--

CREATE TABLE matches (
  match_id BIGINT UNSIGNED NOT NULL UNIQUE,
  version SMALLINT UNSIGNED NOT NULL,
  num_players SMALLINT NOT NULL,
  map_size VARCHAR(1024) NOT NULL,
  map_type VARCHAR(1024) NOT NULL,
  rating_type VARCHAR(1024) NOT NULL,
  server VARCHAR(1024) NOT NULL,
  started BIGINT NOT NULL,
  finished BIGINT NOT NULL,
  duration BIGINT NOT NULL,
  started_date DATETIME NOT NULL,
  finished_date DATETIME NOT NULL,
  duration_minutes BIGINT NOT NULL,
  PRIMARY KEY (match_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `players`
--

CREATE TABLE players (
  match_id BIGINT UNSIGNED NOT NULL,
  profile_id BIGINT UNSIGNED NOT NULL,
  steam_id BIGINT UNSIGNED NULL,
  player_name VARCHAR(1024) NULL,
  country VARCHAR(1024) NULL,
  slot SMALLINT NULL,
  rating INT NULL,
  rating_change INT NULL,
  color VARCHAR(1024) NULL,
  team SMALLINT NOT NULL,
  civ VARCHAR(1024) NOT NULL,
  won TINYINT(1) NOT NULL,
  PRIMARY KEY (match_id, profile_id),
  UNIQUE KEY (match_id, profile_id),
  CONSTRAINT fk_players_matches FOREIGN KEY (match_id) REFERENCES matches (match_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `player`
--

/*CREATE TABLE player (
  profile_id BIGINT UNSIGNED NOT NULL,
  steam_id DOUBLE UNSIGNED NULL,
  player_name VARCHAR(255) NULL,
  country VARCHAR(255) NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id),
  UNIQUE KEY (profile_id, steam_id, player_name, country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/

