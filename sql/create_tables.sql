-- Create tables used by this library/scripts. See README.md for details.
CREATE TABLE `files` (
  id               INT(11)       UNSIGNED AUTO_INCREMENT,
  filename         VARCHAR(128)  NOT NULL,
  impressiontype   VARCHAR(15)   NOT NULL,
  timestamp        TIMESTAMP     NOT NULL,
  directory        VARCHAR(256)  NOT NULL,
  sample_rate      INT(2)        UNSIGNED DEFAULT NULL,
  status           ENUM('processing', 'consumed') NOT NULL,
  consumed_events  INT           UNSIGNED DEFAULT NULL,
  ignored_events   INT           UNSIGNED DEFAULT NULL,
  invalid_events   INT           UNSIGNED DEFAULT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (filename),
  INDEX timestamp_index (timestamp),
  INDEX status_index (status)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `project` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  project   VARCHAR(128)  NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (project)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `language` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  iso_code  VARCHAR(24)   NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (iso_code)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `country` (
  id        SMALLINT(3)   UNSIGNED AUTO_INCREMENT,
  iso_code  VARCHAR(8)    NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY (iso_code)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `bannerimpressions` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  banner          VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  campaign        VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  count           MEDIUMINT(11) DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY (timestamp, banner, campaign, project_id, language_id, country_id),
  INDEX (timestamp),
  INDEX (banner),
  INDEX (campaign),
  INDEX (project_id),
  INDEX (language_id),
  INDEX (country_id)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `files_and_bannerimpressions` (
  id                  INT(11)       UNSIGNED AUTO_INCREMENT,
  file_id             INT(11)       UNSIGNED NOT NULL,
  banner_imp_cell_id  INT(11)       UNSIGNED NOT NULL,
  count               MEDIUMINT(11) DEFAULT 0,

  PRIMARY KEY (id),
  UNIQUE KEY (file_id, banner_imp_cell_id),
  INDEX file_id_index (file_id),
  INDEX banner_imp_cell_id_index (banner_imp_cell_id)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `landingpageimpression_raw` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_medium      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_key         VARCHAR(128)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  landingpage     VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  project_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  language_id     SMALLINT(3)   UNSIGNED DEFAULT NULL,
  country_id      SMALLINT(3)   UNSIGNED DEFAULT NULL,
  file_id         INT(11)       UNSIGNED NOT NULL,

  PRIMARY KEY (id),
  INDEX (timestamp),
  INDEX (utm_source),
  INDEX (utm_campaign),
  INDEX (utm_medium),
  INDEX (utm_key),
  INDEX (landingpage),
  INDEX (project_id),
  INDEX (language_id),
  INDEX (country_id),
  INDEX file_id_index (file_id)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;

CREATE TABLE `donatewiki_unique` (
  id              INT(11)       UNSIGNED AUTO_INCREMENT,
  timestamp       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
  utm_source      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  utm_campaign    VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  contact_id      VARCHAR(255)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  link_id         VARCHAR(128)  CHARACTER SET utf8 DEFAULT '' NOT NULL,
  file_id         INT(11)       UNSIGNED NOT NULL,

  PRIMARY KEY (id),
  UNIQUE KEY utm_source (utm_source, contact_id),
  INDEX file_id_index (file_id)
) DEFAULT CHARACTER SET = utf8 ENGINE = InnoDB;