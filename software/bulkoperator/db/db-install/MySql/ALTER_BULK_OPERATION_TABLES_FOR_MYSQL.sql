ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE_TEMP LONGTEXT );
UPDATE catissue_bulk_operation set CSV_TEMPLATE_TEMP=CSV_TEMPLATE;
alter TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE;
ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE LONGTEXT);
UPDATE catissue_bulk_operation set CSV_TEMPLATE=CSV_TEMPLATE_TEMP;
ALTER TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE_TEMP;
commit;

ALTER TABLE  catissue_bulk_operation ADD(XML_TEMPALTE_TEMP LONGTEXT );
UPDATE catissue_bulk_operation set XML_TEMPALTE_TEMP=XML_TEMPALTE;
alter TABLE  catissue_bulk_operation DROP column  XML_TEMPALTE;
ALTER TABLE  catissue_bulk_operation ADD(XML_TEMPALTE LONGTEXT);
UPDATE catissue_bulk_operation set XML_TEMPALTE=XML_TEMPALTE_TEMP;
ALTER TABLE  catissue_bulk_operation DROP column  XML_TEMPALTE_TEMP;
commit;