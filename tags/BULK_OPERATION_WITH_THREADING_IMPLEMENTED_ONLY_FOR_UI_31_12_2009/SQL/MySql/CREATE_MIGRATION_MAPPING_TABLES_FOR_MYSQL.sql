CREATE TABLE MIGRATION_MAPPING (
	identifier bigint(20) NOT NULL auto_increment,          
	object_classname varchar(255) default NULL,             
	old_id bigint(20) default NULL,                         
	new_id bigint(20) default NULL,
	PRIMARY KEY (identifier)                           
);

CREATE TABLE MIGRATION_EXCEPTION
(
	identifier bigint(20) NOT NULL auto_increment,
	classname varchar(255) default NULL,
	sandboxid bigint(20) default NULL,
	message varchar(4000) default NULL,
	stackTrace varchar(4000) default NULL,
	PRIMARY KEY (identifier)
);