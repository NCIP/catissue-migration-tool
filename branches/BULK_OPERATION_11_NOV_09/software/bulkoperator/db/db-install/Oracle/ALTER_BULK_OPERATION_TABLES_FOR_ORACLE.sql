ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE_TEMP CLOB);
UPDATE catissue_bulk_operation set CSV_TEMPLATE_TEMP=CSV_TEMPLATE;
ALTER TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE;
ALTER TABLE  catissue_bulk_operation ADD(CSV_TEMPLATE CLOB);
UPDATE catissue_bulk_operation set CSV_TEMPLATE=CSV_TEMPLATE_TEMP;
ALTER TABLE  catissue_bulk_operation DROP column  CSV_TEMPLATE_TEMP;
commit;