
CREATE TABLE TBL(cache_fielduse) (
  `row_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `scope_id` int(10) unsigned NOT NULL,
  `field_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `scope_id` (`scope_id`,`field_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(cache_scopes) (
  `scope_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `digest` char(32),
  PRIMARY KEY (`scope_id`),
  UNIQUE KEY `digest` (`digest`)
) ENGINE=InnoDB;


CREATE TABLE TBL(dbr_fields) (
  `field_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `table_id` int(10) unsigned NOT NULL,
  `name` varchar(250) NOT NULL,
  `data_type` tinyint(3) unsigned NOT NULL,
  `is_nullable` tinyint(1),
  `is_signed` tinyint(1),
  `max_value` int(10) unsigned NOT NULL,
  `decimal_digits` int(10) unsigned,
  `display_name` varchar(250),
  `is_pkey` tinyint(1) DEFAULT '0',
  `index_type` tinyint(1),
  `trans_id` tinyint(3) unsigned,
  `regex` varchar(250),
  `default_val` varchar(250),
  PRIMARY KEY (`field_id`),
  KEY `table_id` (`table_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(dbr_instances) (
  `instance_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema_id` int(10) NOT NULL,
  `handle` varchar(50) NOT NULL,
  `class` varchar(50) NOT NULL COMMENT 'query, master, etc...',
  `dbname` varchar(250),
  `prefix` varchar(250),
  `username` varchar(250),
  `password` varchar(250),
  `host` varchar(250),
  `dbfile` varchar(250),
  `module` varchar(50) NOT NULL COMMENT 'Which DB Module to use',
  `readonly` tinyint(1),
  `tag` varchar(255),
  PRIMARY KEY (`instance_id`),
  KEY `schema_id` (`schema_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(dbr_relationships) (
  `relationship_id` int(11) NOT NULL AUTO_INCREMENT,
  `from_name` varchar(45) NOT NULL COMMENT 'reverse name of this relationship',
  `from_table_id` int(10) NOT NULL,
  `from_field_id` int(10) NOT NULL,
  `to_name` varchar(45) NOT NULL COMMENT 'forward name of this relationship',
  `to_table_id` int(10) NOT NULL,
  `to_field_id` int(10) NOT NULL,
  `type` tinyint(3) NOT NULL,
  PRIMARY KEY (`relationship_id`),
  KEY `from_table_id` (`from_table_id`),
  KEY `to_table_id` (`to_table_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(dbr_schemas) (
  `schema_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `handle` varchar(50),
  `display_name` varchar(50),
  PRIMARY KEY (`schema_id`),
  UNIQUE KEY `handle` (`handle`)
) ENGINE=InnoDB;


CREATE TABLE TBL(dbr_tables) (
  `table_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema_id` int(10) unsigned NOT NULL,
  `name` varchar(250) NOT NULL,
  `display_name` varchar(250),
  `is_cachable` tinyint(1) NOT NULL,
  PRIMARY KEY (`table_id`),
  KEY `schema_id` (`schema_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(indexes) (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `refinement_of_id` int(10) unsigned,
  `field_id` int(10) unsigned NOT NULL,
  `prefix_length` int(10) unsigned,
  `is_unique` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
);


CREATE TABLE TBL(enum) (
  `enum_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `handle` varchar(250) COMMENT 'ideally a unique key',
  `name` varchar(250),
  `override_id` int(10) unsigned,
  PRIMARY KEY (`enum_id`),
  KEY `handle` (`handle`)
) ENGINE=InnoDB;


CREATE TABLE TBL(enum_legacy_map) (
  `row_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `context` varchar(250),
  `field` varchar(250),
  `enum_id` int(10) unsigned NOT NULL,
  `sortval` int(11),
  PRIMARY KEY (`row_id`)
) ENGINE=InnoDB;


CREATE TABLE TBL(enum_map) (
  `row_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `field_id` int(10) unsigned NOT NULL,
  `enum_id` int(10) unsigned NOT NULL,
  `sortval` int(11),
  PRIMARY KEY (`row_id`),
  KEY `field_id` (`field_id`)
) ENGINE=InnoDB;
