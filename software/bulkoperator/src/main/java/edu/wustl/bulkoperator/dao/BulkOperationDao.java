package edu.wustl.bulkoperator.dao;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;
import edu.wustl.bulkoperator.bizlogic.BulkOperationBizLogic;
import edu.wustl.bulkoperator.metadata.BulkOperationTemplate;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.DaoUtil;
import edu.wustl.common.beans.NameValueBean;
import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.query.generator.ColumnValueBean;

public class BulkOperationDao {
	
	private static final Logger logger = Logger.getLogger(BulkOperationBizLogic.class);

	private JDBCDAO jdbcDao;
	
	private static final String GET_TEMPL_DETAILS_SQL = 
			"SELECT " +
			"	DROPDOWN_NAME, OPERATION, XML_TEMPALTE, CSV_TEMPLATE " +
			"FROM " +
			"	CATISSUE_BULK_OPERATION " +
			"WHERE " +
			"	DROPDOWN_NAME = ?";
	
	private static final String GET_DROPDOWN_SQL= 
			"SELECT " +
			"	DROPDOWN_NAME " +
			"FROM" +
			"	CATISSUE_BULK_OPERATION";
	
	private static final String GET_TEMPLATE_SQL= 
			"SELECT " +
			"	OPERATION " +
			"FROM" +
			"	CATISSUE_BULK_OPERATION " +
			"WHERE " +
			"	OPERATION = ?";
	
	
	private static final String INSERT_TEMPLATE_ORA_SQL = 
			"INSERT INTO " +
			"	CATISSUE_BULK_OPERATION " +
			"(IDENTIFIER, OPERATION, DROPDOWN_NAME, CSV_TEMPLATE, XML_TEMPALTE) " +
			"	VALUES (CATISSUE_BULK_OPERATION_SEQ.NEXTVAL, ?, ?, ?, ?) ";
	
	private static final String INSERT_TEMPLATE_MYSQL_SQL = 
			"INSERT INTO " +
			"	CATISSUE_BULK_OPERATION " +
			"(IDENTIFIER, OPERATION, DROPDOWN_NAME, CSV_TEMPLATE, XML_TEMPALTE) " +
			"	VALUES (default, ?, ?, ?, ?) ";
	
	
	private static final String UPDATE_TEMPLATE = 
			"UPDATE " +
			"	CATISSUE_BULK_OPERATION " +
			"SET  " +
			"	CSV_TEMPLATE = ?, XML_TEMPALTE = ? " +
			"WHERE " +
			"	DROPDOWN_NAME= ? ";

	
	public BulkOperationDao(JDBCDAO jdbcDao) {
		this.jdbcDao = jdbcDao;
	}
	
	public static BulkOperationTemplate getTemplateDetails(String templateName) throws BulkOperationException {
		BulkOperationTemplate template = null;
		
		List<List<Object>> templateDetails = DaoUtil.executeSQLQuery(GET_TEMPL_DETAILS_SQL, templateName);
		if (templateDetails != null && !templateDetails.isEmpty()) {
			List<Object> templateDetailRow = templateDetails.get(0);
			template = new BulkOperationTemplate();
			template.setTemplateName((String)templateDetailRow.get(0));
			template.setOperationName((String)templateDetailRow.get(1));
			template.setXmlTemplate(DaoUtil.getString((Clob)templateDetailRow.get(2)));
			template.setCsvTemplate(DaoUtil.getString((Clob)templateDetailRow.get(3)));
		}
		
		return template;
	}
	
	
	public static boolean doesTemplateExists(String templateName) throws BulkOperationException {
		List<List<Object>> templateDetails = DaoUtil.executeSQLQuery(GET_TEMPLATE_SQL, templateName);
		if (templateDetails != null && !templateDetails.isEmpty()) {
			return true;
		}
		return false;
	}
	
	
	public static List<NameValueBean> getTemplateNameDropDownList() throws BulkOperationException, ApplicationException {
		List<NameValueBean> bulkOperationList = new ArrayList<NameValueBean>();
		JDBCDAO jdbcDao = null;
		
		try {
			jdbcDao = DaoUtil.getJdbcDao();
			List<List<Object>> list = DaoUtil.executeSQLQuery(GET_DROPDOWN_SQL, null);
//			List list = jdbcDao.executeQuery(GET_DROPDOWN_SQL);
			if(!list.isEmpty())	{
				Iterator iterator = list.iterator();
				while(iterator.hasNext()) {
					List innerList = (List)iterator.next();
					String innerString = (String)innerList.get(0);
					bulkOperationList.add(new NameValueBean(innerString, innerString));
				}
			}
		} catch (Exception exp) {
			logger.error(exp.getMessage(), exp);
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.dropdown");
			throw new BulkOperationException(errorKey, exp, "");
		} finally {
			DaoUtil.closeJdbcDao(jdbcDao);
		}
		
		return bulkOperationList;
	}
	
	public static File getCSVFile(String dropdownName) {
		File csvFile = null;
		
		try {
		BulkOperationTemplate boTemplate = getTemplateDetails(dropdownName);
		csvFile = writeCSVFile(boTemplate.getCsvTemplate(), dropdownName);
		} catch(Exception e) {
			logger.error("Error in retrieving the csvTemplate");
			throw new RuntimeException("Error in retrieving the csvTemplate",e);
		}
		return csvFile;
	}

	private static File writeCSVFile(String commaSeparatedString, String dropdownName) throws Exception {
		CSVWriter writer = null;
		File csvFile = null;
		try {
			String csvFileName = dropdownName + ".csv";
			csvFile = new File(csvFileName);
			csvFile.createNewFile();
			writer = new CSVWriter(new FileWriter(csvFileName), ',');
			String[] stringArray = commaSeparatedString.split(",");
			writer.writeNext(stringArray);
		} catch (IOException exp) {
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.writing");
			throw new BulkOperationException(errorkey, exp, "");
		} finally {
			writer.close();
		}
		return csvFile;
	}
	
	
	public void uploadTemplate(BulkOperationTemplate boTemplate, String dbType) {
		try {
			
			if(doesTemplateExists(boTemplate.getTemplateName())) {
				updateTemplate(boTemplate);
			} else {
				insertTemplate(boTemplate, dbType);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error occured in persisting the template",e);
		}
	}

	
	private void insertTemplate(BulkOperationTemplate template, String dbType) 
	throws Exception {
		List<ColumnValueBean> params = new ArrayList<ColumnValueBean>();

		try {
			params.add(new ColumnValueBean(template.getOperationName()));
			params.add(new ColumnValueBean(template.getTemplateName()));
			params.add(new ColumnValueBean(template.getCsvTemplate()));
			params.add(new ColumnValueBean(template.getXmlTemplate()));
			
			if (dbType == null) {
				if (BulkOperationConstants.ORACLE_DATABASE.equalsIgnoreCase(BulkOperationUtility.getDatabaseType())) {
					jdbcDao.executeUpdate(INSERT_TEMPLATE_ORA_SQL, params);
				} else if (BulkOperationConstants.MYSQL_DATABASE.equalsIgnoreCase(BulkOperationUtility.getDatabaseType())) {
					jdbcDao.executeUpdate(INSERT_TEMPLATE_MYSQL_SQL, params);
				}
			} else if (BulkOperationConstants.ORACLE_DATABASE.equalsIgnoreCase(dbType)) {
				jdbcDao.executeUpdate(INSERT_TEMPLATE_ORA_SQL, params);
			} else if (BulkOperationConstants.MYSQL_DATABASE.equalsIgnoreCase(dbType)) {
				jdbcDao.executeUpdate(INSERT_TEMPLATE_MYSQL_SQL, params);
			}
			logger.info("Template is Inserted: " + template.getTemplateName());
		} catch (Exception e) {
			throw new RuntimeException("Error inserting bulk operation template", e);
		} 
	}
	
	private void updateTemplate(BulkOperationTemplate template) 
	throws Exception {
		List<ColumnValueBean> params = new ArrayList<ColumnValueBean>();

		try {
			params.add(new ColumnValueBean(template.getCsvTemplate()));
			params.add(new ColumnValueBean(template.getXmlTemplate()));
			params.add(new ColumnValueBean(template.getTemplateName()));

			jdbcDao.executeUpdate(UPDATE_TEMPLATE, params);
			logger.info("Template is Updated: " + template.getTemplateName());
		} catch (Exception e) {
			throw new RuntimeException("Error updating bulk operation template", e);
		} 
	}
}
