/**
 *
 */

package edu.wustl.bulkoperator.templateImport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.digester3.Digester;
import org.apache.commons.digester3.binder.DigesterLoader;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.BulkOperationTemplateParser;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.common.util.logger.LoggerConfig;
import edu.wustl.dao.exception.DAOException;

public abstract class AbstractImportBulkOperation {

	protected static final Logger logger = Logger.getCommonLogger(ImportBulkOperationTemplate.class);

	static {
		LoggerConfig.configureLogger(System.getProperty("user.dir") + "/BulkOperations/conf");
	}


	protected boolean isTemplateExist(String operationName, String operationNameFromDB,
			String dropdownName, String dropdownNameFromDB) throws BulkOperationException {
		boolean flag = false;
		if (operationNameFromDB.equals(operationName) & dropdownNameFromDB.equals(dropdownName))
		{
			flag = true;
		}
		else if (operationNameFromDB.equals(operationName)
				& !dropdownNameFromDB.equals(dropdownName))
		{
			logger.debug("Cannot insert the template as "
					+ "same Operation Name already exists in the database.");
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.matching.operation.name");

			throw new BulkOperationException(errorkey, null, "");
		}
		else if (!operationNameFromDB.equals(operationName)
				& dropdownNameFromDB.equals(dropdownName))
		{
			logger.debug("Cannot insert template as "
					+ "same DropDown Name already exists in the database.");
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.matching.dropdown.name");
			throw new BulkOperationException(errorkey, null, "");
		}
		return flag;

	}

	/**
	 * Get XML Template File Data.
	 * @param xmlFile String.
	 * @return String.
	 * @throws Exception Exception.
	 */
	protected String getXMLTemplateFileData(String xmlFile) throws BulkOperationException
	{
		StringWriter xmlFormatData = new StringWriter();
		try
		{
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			InputStream inputStream = new FileInputStream(new File(xmlFile));
			org.w3c.dom.Document doc = documentBuilderFactory.newDocumentBuilder().parse(
					inputStream);
			Transformer serializer = TransformerFactory.newInstance().newTransformer();
			serializer.transform(new DOMSource(doc), new StreamResult(xmlFormatData));
		}
		catch (FileNotFoundException fnfExp)
		{
			logger.debug("XML File Not Found at the specified path.", fnfExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.xml.file.not.found");
			throw new BulkOperationException(errorkey, fnfExp, "");
		}
		catch (IOException ioExp)
		{
			logger.debug("Error in reading the XML File.", ioExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.xml.file.reading");
			throw new BulkOperationException(errorkey, ioExp, "");
		}
		catch (Exception exp)
		{
			logger.debug("Error in encoding XML file to data stream.", exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		
		return xmlFormatData.toString();
	}

	/**
	 * Get CSV Template File Data.
	 * @param csvFile String.
	 * @return String.
	 * @throws Exception Exception.
	 */
	protected String getCSVTemplateFileData(String csvFile) throws BulkOperationException
	{
		CSVReader reader = null;
		List<String[]> list = null;
		StringBuffer commaSeparatedString = new StringBuffer();
		try
		{
			reader = new CSVReader(new FileReader(csvFile));
			list = reader.readAll();
			reader.close();
			Iterator<String[]> iterator = list.iterator();
			if (iterator.hasNext())
			{
				String string[] = iterator.next();
				int rowDataLength = string.length;
				for (int i = 0; i < rowDataLength; i++)
				{
					commaSeparatedString.append(string[i]);
					if (i < rowDataLength - 1)
					{
						commaSeparatedString.append(BulkOperationConstants.SINGLE_COMMA);
					}
				}
			}
		}
		catch (FileNotFoundException fnfExpp)
		{
			logger.debug("CSV File Not Found at the specified path.", fnfExpp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.not.found");
			throw new BulkOperationException(errorkey, fnfExpp, "");
		}
		catch (IOException ioExpp)
		{
			logger.debug("Error in reading the CSV File.", ioExpp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.reading");
			throw new BulkOperationException(errorkey, ioExpp, "");
		}
		finally
		{
			try
			{
				if (reader != null)
				{
					reader.close();
				}
			}
			catch (IOException ioExpp)
			{
				logger.debug("Error while closing the reader.", ioExpp);
			}
		}
		return commaSeparatedString.toString();
	}

	protected void importTemplates(String operationName, String dropdownName, String csvFile,
			String xmlFile) {
		try {
			
			logger.info("operationName : "+operationName);
			logger.info("dropdownName  : "+dropdownName);
			logger.info("csvFile	   : "+csvFile);
			logger.info("xmlFile 	   : "+xmlFile);

			validateXml(xmlFile);
			saveTemplateInDatabase(operationName, dropdownName, csvFile, xmlFile);
			
		} catch (BulkOperationException exp) {
			logger.info("------------------------ERROR:BulkOperationException--------------------------------\n");
			logger.info(exp.getCause().getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		} catch (SQLException exp) {
			logger.info("------------------------ERROR:SQLException--------------------------------\n");
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		} catch (DAOException exp) {
			logger.info("------------------------ERROR:DAOException--------------------------------\n");
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		} catch (Exception exp) {
			logger.info("------------------------ERROR:Exception--------------------------------\n");
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		}
	}

	protected void saveTemplateInDatabase(String operationName, String dropdownName,
			String csvFile, String xmlFile) throws DAOException, BulkOperationException,
			SQLException, IOException {
		boolean flag = checkAddOrEditTemplateCase(operationName, dropdownName);
		if (flag) {
			editTemplate(operationName, dropdownName, csvFile, xmlFile);
		}
		else {
			addTemplate(operationName, dropdownName, csvFile, xmlFile);
		}
	}

	private void validateXml(String xmlFile) throws Exception {
	
		try {
			BulkOperation.fromXml(getXMLTemplateFileData(xmlFile), true);
			
		} catch (Exception e) {
			 logger.debug(e.getMessage());
			 ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.xml.template");
			 throw new BulkOperationException(errorkey, e, e.getMessage());
		}
	}
		
	protected abstract boolean checkAddOrEditTemplateCase(String operationName, String dropdownName)
			throws DAOException, BulkOperationException;

	protected abstract void editTemplate(String operationName, String dropdownName, String csvFile,
			String xmlFile) throws DAOException, BulkOperationException, IOException, SQLException;

	protected abstract void addTemplate(String operationName, String dropdownName, String csvFile,
			String xmlFile) throws DAOException, BulkOperationException, IOException, SQLException;

}
