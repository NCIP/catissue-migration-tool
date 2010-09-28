
package edu.wustl.bulkoperator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.dao.DBManagerImpl;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.metadata.BulkOperationMetadataUtil;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.DataList;
import edu.wustl.bulkoperator.util.DataReader;
import edu.wustl.bulkoperator.validator.TemplateValidator;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.common.util.logger.LoggerConfig;

/**
 * Import Bulk Operation from UI back end target.
 * @author sagar_baldwa
 *
 */
public class ImportBulkOperationTemplate
{

	/**
	 * logger Logger - Generic logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(ImportBulkOperationTemplate.class);

	/**
	 * logger Logger - Generic logger.
	 */
	static
	{
		try
		{
			LoggerConfig.configureLogger(System.getProperty("user.dir") + "/BulkOperations/conf");
			ErrorKey.init("~");
		}
		catch (IOException exp)
		{
			logger.error(exp.getMessage(), exp);
		}
	}

	/**
	 * Import Bulk Operation Template.
	 * @param operationName String.
	 * @param dropdownName String.
	 * @param csvFilePath String.
	 * @param xmlFilePath String.
	 * @throws Exception Exception.
	 */
	public ImportBulkOperationTemplate(String operationName, String dropdownName, String csvFile,
			String xmlFile) throws Exception
	{
		validate(operationName, dropdownName, csvFile, xmlFile);
	}

	/**
	 * Main method.
	 * @param args Array of Strings.
	 */
	public static void main(String[] args)
	{
		try
		{
			/*System.setProperty("config.dir", "clinportal-properties");
			String operationName = "STIR_3monthFollowupSurvey";
			String dropdownName = "STIR_3monthFollowupSurvey";
			String csvFile = "D:/E/Bulk Data Files/NEC_maternal_data 2.0.csv";
			String xmlFile = "D:/E/Bulk/NEC_maternal_data 2.xml";*/
			//validateParameters(args);
			String operationName = args[0];
			String dropdownName = args[1];
			String csvFile = args[2];
			String xmlFile = args[3];
			validate(operationName, dropdownName, csvFile, xmlFile);
		}
		catch (BulkOperationException exp)
		{

			logger.info("------------------------ERROR:BulkOperationException--------------------------------\n");
			logger.debug(exp.getMessage(), exp);
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		}
		catch (SQLException exp)
		{
			logger.info("------------------------ERROR:SQLException--------------------------------\n");
			logger.debug(exp.getMessage(), exp);
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		}
		catch (Exception exp)
		{
			logger.info("------------------------ERROR:Exception--------------------------------\n");
			logger.debug(exp.getMessage(), exp);
			logger.info(exp.getMessage() + "\n");
			logger.info("------------------------ERROR:--------------------------------");
		}
	}

	/**
	 * @param operationName
	 * @param dropdownName
	 * @param csvFile
	 * @param xmlFile
	 * @throws FileNotFoundException
	 * @throws BulkOperationException
	 * @throws Exception
	 */
	private static void validate(String operationName, String dropdownName, String csvFile,
			String xmlFile) throws BulkOperationException, SQLException, IOException
	{
		DataList dataList = null;
		try
		{
			FileInputStream inputStream = new FileInputStream(csvFile);
			Properties properties = new Properties();
			properties.put("inputStream", inputStream);
			dataList = DataReader.getNewDataReaderInstance(properties).readData();
		}
		catch (FileNotFoundException fnfExpp)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.not.found");
			throw new BulkOperationException(errorkey, fnfExpp, "");

		}
		catch (Exception exp)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.incorrect.csv.file");
			throw new BulkOperationException(errorkey, exp, "");
		}
		BulkOperationMetaData metaData = null;
		try
		{
			String configDirectory = System.getProperties().getProperty(BulkOperationConstants.CONFIG_DIR);
			metaData = new BulkOperationMetadataUtil().unmarshall(xmlFile,
					"./" + configDirectory + "/mapping.xml");
		}
		catch (BulkOperationException exp)
		{
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
		}
		Collection<BulkOperationClass> classList = metaData.getBulkOperationClass();
		if (classList == null)
		{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.no.templates.loaded.message");
			throw new BulkOperationException(errorkey, null, "");
		}
		else
		{
			Iterator<BulkOperationClass> iterator = classList.iterator();
			if (iterator.hasNext())
			{
				BulkOperationClass bulkOperationClass = iterator.next();
				TemplateValidator templateValidator = new TemplateValidator();
				Set<String> errorList = templateValidator.validateXmlAndCsv(bulkOperationClass,
						operationName, dataList);
				if (errorList.isEmpty())
				{
					String csvFileData = getCSVTemplateFileData(csvFile);
					String xmlFileData = getXMLTemplateFileData(xmlFile);
					saveTemplateInDatabase(operationName, dropdownName, csvFileData, xmlFileData);
				}
				else
				{
					logger.info("----------------------ERROR:errorList-------------------------");
					for (String error : errorList)
					{
						logger.info(error);
					}
					logger.info("----------------------ERROR-------------------------");
				}
			}
		}
	}

	/**
	 * Add Template In Database.
	 * @param operationName String.
	 * @param dropdownName String.
	 * @param csvFileData String.
	 * @param xmlFileData String.
	 * @throws Exception Exception.
	 */
	private static void saveTemplateInDatabase(String operationName, String dropdownName,
			String csvFileData, String xmlFileData) throws BulkOperationException, SQLException
	{
		Connection connection = null;
		try
		{
			connection = DBManagerImpl.getConnection();
			boolean flag = checkAddOrEditTemplateCase(connection, operationName, dropdownName);
			if (flag)
			{
				editTemplate(connection, operationName, dropdownName, csvFileData, xmlFileData);
			}
			else
			{
				addTemplate(connection, operationName, dropdownName, csvFileData, xmlFileData);
			}
		}
		catch (BulkOperationException exp)
		{
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
		}
		finally
		{
			if (connection != null)
			{
				connection.close();
			}
		}
	}

	/**
	 * Edit Template.
	 * @param connection Connection.
	 * @param operationName String.
	 * @param dropdownName String.
	 * @param csvFileData String.
	 * @param xmlFileData String.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws SQLException SQLException.
	 */
	private static void editTemplate(Connection connection, String operationName,
			String dropdownName, String csvFileData, String xmlFileData)
			throws BulkOperationException, SQLException
	{
		PreparedStatement preparedStatement = null;
		String databaseType = null;
		try
		{
			databaseType = BulkOperationUtility.getDatabaseType();
			if (BulkOperationConstants.ORACLE_DATABASE.equalsIgnoreCase(databaseType))
			{
				String query = "update catissue_bulk_operation set OPERATION = ?, "
						+ "CSV_TEMPLATE = ?, XML_TEMPALTE = ?,  DROPDOWN_NAME = ? "
						+ "where OPERATION = ? or DROPDOWN_NAME= ? ";
				preparedStatement = connection.prepareStatement(query);
				preparedStatement.setString(1, operationName);
				preparedStatement.setString(2, csvFileData);
				StringReader reader = new StringReader(xmlFileData);
				preparedStatement.setCharacterStream(3, reader, xmlFileData.length());
				preparedStatement.setString(4, dropdownName);
				preparedStatement.setString(5, operationName);
				preparedStatement.setString(6, dropdownName);
			}
			else if (BulkOperationConstants.MYSQL_DATABASE.equalsIgnoreCase(databaseType))
			{
				String query = "update catissue_bulk_operation set OPERATION = ?, "
						+ "CSV_TEMPLATE = ?, XML_TEMPALTE = ?,  DROPDOWN_NAME = ? "
						+ "where OPERATION = ? or DROPDOWN_NAME= ? ";
				preparedStatement = connection.prepareStatement(query);
				preparedStatement.setString(1, operationName);
				preparedStatement.setString(2, csvFileData);
				preparedStatement.setString(3, xmlFileData);
				preparedStatement.setString(4, dropdownName);
				preparedStatement.setString(5, operationName);
				preparedStatement.setString(6, dropdownName);
			}
			int rowCount = preparedStatement.executeUpdate();
			if (rowCount > 0)
			{
				logger.info("Data updated successfully. " + rowCount + " row edited");
			}
			else
			{
				logger.info("No rows updated.");
			}
		}
		catch (SQLException sqlExp)
		{
			logger.debug("Error in updating the record in database.", sqlExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.database.insert.update.error");
			throw new BulkOperationException(errorkey, sqlExp, "updating");
		}
		finally
		{
			if (preparedStatement != null)
			{
				preparedStatement.close();
			}
		}
	}

	/**
	 * Add Template.
	 * @param connection Connection.
	 * @param operationName String.
	 * @param dropdownName String.
	 * @param csvFileData String.
	 * @param xmlFileData String.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws SQLException
	 * @throws SQLException SQLException.
	 */
	private static void addTemplate(Connection connection, String operationName,
			String dropdownName, String csvFileData, String xmlFileData)
			throws BulkOperationException, SQLException
	{
		PreparedStatement preparedStatement = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try
		{
			if (BulkOperationConstants.ORACLE_DATABASE.equalsIgnoreCase(BulkOperationUtility
					.getDatabaseType()))
			{
				String sequenceQuery = "select CATISSUE_BULK_OPERATION_SEQ.NEXTVAL from dual";
				statement = connection.createStatement();
				resultSet = statement.executeQuery(sequenceQuery);
				int sequenceNumber = 0;
				if (resultSet.next())
				{
					sequenceNumber = resultSet.getInt(1);
					if (sequenceNumber > 0)
					{
						String query = "insert into catissue_bulk_operation "
								+ "(IDENTIFIER, OPERATION, CSV_TEMPLATE, XML_TEMPALTE, "
								+ "DROPDOWN_NAME ) values (?, ?, ?, ?, ?)";

						preparedStatement = connection.prepareStatement(query);
						preparedStatement.setInt(1, sequenceNumber);
						preparedStatement.setString(2, operationName);
						preparedStatement.setString(3, csvFileData);
						StringReader reader = new StringReader(xmlFileData);
						preparedStatement.setCharacterStream(4, reader, xmlFileData.length());
						preparedStatement.setString(5, dropdownName);
					}
				}
			}
			else if (BulkOperationConstants.MYSQL_DATABASE.equalsIgnoreCase(BulkOperationUtility
					.getDatabaseType()))
			{
				String query = "insert into catissue_bulk_operation (OPERATION, "
						+ "CSV_TEMPLATE, XML_TEMPALTE, DROPDOWN_NAME ) values (?, ?, ?, ?)";
				preparedStatement = connection.prepareStatement(query);
				preparedStatement.setString(1, operationName);
				preparedStatement.setString(2, csvFileData);
				preparedStatement.setString(3, xmlFileData);
				preparedStatement.setString(4, dropdownName);
			}
			int rowCount = preparedStatement.executeUpdate();
			logger.info("Data inserted successfully. " + rowCount + " row inserted.");
		}
		catch (SQLException sqlExp)
		{
			logger.debug("Error in inserting the record in database.", sqlExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.database.insert.update.error");
			throw new BulkOperationException(errorkey, sqlExp, "inserting");
		}
		finally
		{
			if (resultSet != null)
			{
				resultSet.close();
			}
			if (statement != null)
			{
				statement.close();
			}
			if (preparedStatement != null)
			{
				preparedStatement.close();
			}
		}
	}

	/**
	 * Check If Edit Case.
	 * @param connection Connection.
	 * @param operationName String.
	 * @param dropdownName String.
	 * @return boolean Boolean.
	 * @throws Exception Exception.
	 */
	private static boolean checkAddOrEditTemplateCase(Connection connection, String operationName,
			String dropdownName) throws BulkOperationException, SQLException
	{
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		boolean flag = false;
		try
		{
			String query = "select operation, dropdown_name from catissue_bulk_operation "
					+ "where operation like ? or dropdown_name like ?";
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, operationName);
			preparedStatement.setString(2, dropdownName);
			resultSet = preparedStatement.executeQuery();
			if (resultSet != null)
			{
				while (resultSet.next())
				{
					String operationNameFromDB = resultSet.getString("OPERATION");
					String dropdownNameFromDB = resultSet.getString("DROPDOWN_NAME");
					if (operationNameFromDB.equals(operationName)
							& dropdownNameFromDB.equals(dropdownName))
					{
						flag = true;
						break;
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
				}
			}
		}
		catch (SQLException exp)
		{
			logger.debug("Error in database operation. Please the database driver and database "
					+ "properties mentioned in the host application install properties file.", exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.database.error.driver.msg");
			throw new BulkOperationException(errorkey, exp, "");
		}
		finally
		{
			resultSet.close();
			preparedStatement.close();
		}
		return flag;
	}

	/**
	 * Get XML Template File Data.
	 * @param xmlFile String.
	 * @return String.
	 * @throws Exception Exception.
	 */
	private static String getXMLTemplateFileData(String xmlFile) throws BulkOperationException
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
	private static String getCSVTemplateFileData(String csvFile) throws BulkOperationException,
			IOException
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
			if (reader != null)
			{
				reader.close();
			}
		}
		return commaSeparatedString.toString();
	}

	private static void validateParameters(String[] args) throws BulkOperationException
	{
		StringBuffer errMsg = new StringBuffer();
		boolean errorFlag = false;
		errMsg.append("Please specify the ");
		if (args.length == 0)
		{
			errorFlag = true;
			errMsg.append("Operation Name,");
		}
		if (args.length < 2)
		{
			errorFlag = true;
			errMsg.append("Dropdown Name,");
		}
		if (args.length < 3)
		{
			errorFlag = true;
			errMsg.append("Absolute CSV File Path,");
		}
		if (args.length < 4)
		{
			errorFlag = true;
			errMsg.append("Absolute XML File Path,");
		}
		if (errorFlag)
		{
			errMsg.deleteCharAt(errMsg.length() - 1);
			errMsg.append('.');
			throw new BulkOperationException(null, null, errMsg.toString());
		}
	}
}