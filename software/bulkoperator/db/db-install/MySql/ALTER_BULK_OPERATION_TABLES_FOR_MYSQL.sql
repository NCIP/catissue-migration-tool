/*L
   Copyright Washington University in St. Louis
   Copyright SemanticBits
   Copyright Persistent Systems
   Copyright Krishagni

   Distributed under the OSI-approved BSD 3-Clause License.
   See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
L*/

ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE_TEMP VARCHAR(15000));
UPDATE catissue_bulk_operation set CSV_TEMPLATE_TEMP=CSV_TEMPLATE
alter TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE;
ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE VARCHAR(15000));
UPDATE catissue_bulk_operation set CSV_TEMPLATE=CSV_TEMPLATE_TEMP;
ALTER TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE_TEMP;
commit;