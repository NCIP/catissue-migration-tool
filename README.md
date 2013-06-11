Welcome to the caTissue Migration Tool Project!
=====================================

The Migration Tool is a java application, based on the API and developed within the caTissue project. The migration involves extracting input legacy data into an interim caTissue staging database and then running the caTissue Migration Tool, which will validate and load the staging data into the target caTissue instance.

The migration tool provides a two-fold benefit of both pushing data from a staging instance of caTissue into a production / target instance as well as performing data validation that allows identification of bad data that needs to be cleansed such as duplicate records, type mismatch, value/domain mismatch, etc. Once bad data is corrected, the Migration Tool can be re-run to migrate just those remaining records. This tool is also effective for handling/managing the security rules present in the common security module (CSM). One important constraint to consider with this method is the requirement to set up two instances of caTissue, an initial staging instances and final target/production instance.

caTissue Migration Tool is an Open Source project written in Java.

The caTissue Migration Tool is distributed under the BSD 3-Clause License.
Please see the NOTICE and LICENSE files for details.

You will find more details about the caTissue Migration Tool in the following links:
 * [caTissue Tools Wiki] (https://wiki.nci.nih.gov/display/caTissue/caTissue+Tools+Wiki)
 * [caTissue Wiki] (https://wiki.nci.nih.gov/display/caTissuedoc/caTissue+Documentation+Wiki)
 * [Issue Tracker] (https://bugzilla.wustl.edu/bugzilla/)
 * [Code Repository] (https://github.com/NCIP/catissue-migration-tool)
 * [Migrating legacy data] (https://wiki.nci.nih.gov/display/TBPTKC/Migrating+Legacy+Data+into+caTissue+-+Methods+and+Case+Studies#MigratingLegacyDataintocaTissue-MethodsandCaseStudies-Method2ConsiderationsforUtilizingthecaTissueMigrationTool)

Please join us in further developing and improving the caTissue Migration Tool.

## Required Software to build
* Java 1.6
* Ant 1.7.x
