CREATE TABLE `catissue_migration_mapping` (                 
                              `identifier` bigint(20) NOT NULL auto_increment,          
                              `object_classname` varchar(255) default NULL,             
                              `old_id` bigint(20) default NULL,                         
                              `new_id` bigint(20) default NULL,                         
                              PRIMARY KEY  (`identifier`)                               
                            )
CREATE TABLE `catissue_migration_exception` (  
                                `identifier` bigint(20) NOT NULL auto_increment,         
                                `classname` varchar(255) default NULL,       
                                `sandboxid` bigint(20) default NULL,      
                                `message` varchar(500) default NULL,         
                                PRIMARY KEY  (`identifier`)                  
                              )