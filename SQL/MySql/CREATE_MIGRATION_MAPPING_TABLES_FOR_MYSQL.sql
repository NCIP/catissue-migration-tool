/*L
   Copyright Washington University in St. Louis
   Copyright SemanticBits
   Copyright Persistent Systems
   Copyright Krishagni

   Distributed under the OSI-approved BSD 3-Clause License.
   See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
L*/

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