
package edu.wustl.bulkoperator.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.cab2b.common.exception.RuntimeException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.DAO;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.daofactory.DAOConfigFactory;
import edu.wustl.dao.daofactory.IDAOFactory;
import edu.wustl.dao.exception.DAOException;

public class BulkOperationUtility {

	private static final Logger logger = Logger.getCommonLogger(BulkOperationUtility.class);
	
	private static List<RecordField> whereClause = new ArrayList<RecordField>();
	
	private static Map<String, RecordField> joinClause = new HashMap<String, RecordField>();
	

	public static String createHQL(RecordMapper recMapper, CsvReader csvReader, Map<String, Object> propIdx) {
		List<String> whereClause = new ArrayList<String>();

		StringBuffer hql = null;
		for(RecordField field : recMapper.getIdFields()) {
			String name = field.getName();
			Integer idx = (Integer) propIdx.get(name);
			String csvData = csvReader.getValue(idx);
			
			if(csvData != null) {
				whereClause.add(new StringBuilder(name)
										.append(" = '").append(csvData)
										.append("' ").toString());
			}
		}
	
		if (!whereClause.isEmpty()) {
			hql = new StringBuffer(" FROM ")
				.append(recMapper.getClassName())
				.append(" WHERE ");

			for(String where : whereClause) {
				hql.append(where).append(" AND ");
			}
		}
		
		return  hql.toString().substring(0, hql.length()-4);
	}


	public static String getGetterFunctionName(String name) {
		String functionName = null;
		if (name != null && name.length() > 0) {
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase(Locale.ENGLISH);
			String remainingString = name.substring(1);
			functionName = "get" + upperCaseFirstAlphabet + remainingString;
		}
		return functionName;
	}

	public static String getSpecimenClassDomainObjectName(String name) {
		String objectName = null;
		if (name != null && name.length() > 0) {
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase(Locale.ENGLISH);
			String remainingString = name.substring(1);
			objectName = "edu.wustl.catissuecore.domain." + upperCaseFirstAlphabet + remainingString + "Specimen";
		}
		return objectName;
	}

	public static String getSetterFunctionName(String name) {
		String functionName = null;
		if (name != null && name.length() > 0) {
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase(Locale.ENGLISH);
			String remainingString = name.substring(1);
			functionName = "set" + upperCaseFirstAlphabet + remainingString;
		}
		return functionName;
	}

	public static Long getCurrentTimeInSeconds() {
		return System.currentTimeMillis() / 1000;
	}

	public static Properties getMigrationInstallProperties() {
		Properties props = new Properties();
		try {
			FileInputStream propFile = new FileInputStream(
					BulkOperationConstants.MIGRATION_INSTALL_PROPERTIES_FILE);
			props.load(propFile);
		} catch (FileNotFoundException fnfException) {
			logger.error(fnfException.getMessage(), fnfException);
		} catch (IOException ioException) {
			logger.error(ioException.getMessage(), ioException);
		}
		return props;
	}

	//
	// TODO: VP: Needs to be reviewed and fixed for descriptor leaks
	//
	public static File createZip(String csvFileName, String zipFileName) throws BulkOperationException {
		File csvFile = new File(csvFileName);
		File zipFile = null;
		try {
			if (!csvFile.exists()) {
				throw new FileNotFoundException("CSV File Not Found");
			}
			
			byte[] buffer = new byte[18024];
			zipFile = new File(zipFileName);
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile));
			out.setLevel(Deflater.DEFAULT_COMPRESSION);
			FileInputStream fileInptStream = new FileInputStream(csvFile);
			ZipEntry zipEntry = new ZipEntry(csvFile.getName());
			out.putNextEntry(zipEntry);
			int len;
			while ((len = fileInptStream.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}
			out.closeEntry();
			fileInptStream.close();
			out.close();
		
		} catch (FileNotFoundException fnfExp) {
			logger.error("Error while creating ouput report zip file.", fnfExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.zip.file");
			throw new BulkOperationException(errorkey, fnfExp, "");
		} catch (IOException ioExp) {
			logger.error("Error while creating ouput report zip file.", ioExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.zip.file");
			throw new BulkOperationException(errorkey, ioExp, "");
		}
		return zipFile;
	}

	public static String getUniqueKey() {
		Date date = new Date();
		Format formatter = new SimpleDateFormat("dd-MM-yy");
		return formatter.format(date);
	}

	/**
	 * Get Class Name From Bulk Operation Properties File.
	 * @return String String
	 * @throws BulkOperationException BulkOperationException
	 */
	public static String getAppServiceName()
			throws BulkOperationException
	{
		String fileName = System.getProperty("bulkoperator.appservice.class");
		Properties properties = BulkOperationUtility.getPropertiesFile(fileName);
		return properties.getProperty(BulkOperationConstants.BULK_OPERATION_APPSERVICE_CLASSNAME);
	}

	/**
	 * Get Install Properties file.
	 * @return Properties.
	 */
	public static Properties getPropertiesFile(String propertiesFileName)
			throws BulkOperationException {
		Properties props = new Properties();
		try 	{
			FileInputStream propFile = new FileInputStream(propertiesFileName);
			props.load(propFile);
		} catch (FileNotFoundException fnfException) {
			logger.debug(propertiesFileName + " file not found.", fnfException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.not.found");
			throw new BulkOperationException(errorkey, fnfException, propertiesFileName);
		} catch (IOException ioException) {
			logger.debug("Error while accessing " + propertiesFileName + " file.", ioException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorkey, ioException, propertiesFileName);
		}
		return props;
	}

	/**
	 * Get Database Type.
	 * @return String.
	 */
	public static String getDatabaseType() throws BulkOperationException {
		Properties properties = BulkOperationUtility.getBulkOperationPropertiesInstance();
		String applnInstallPropFileName = properties
				.getProperty(BulkOperationConstants.DATABASE_CREDENTIALS_FILE);
		Properties applnInstallProp = getPropertiesFile("./" + applnInstallPropFileName);
		return applnInstallProp.getProperty("database.type");
	}

	/**
	 * This method will change the Bulk Operation status from In Progress
	 * to Failed. The method should be called whenever the application
	 * server and stops.
	 * @param sessionData SessionDataBean
	 */
	public static void changeBulkOperationStatusToFailed() throws DAOException {
		try	{
			final String appName = CommonServiceLocator.getInstance().getAppName();
			final JDBCDAO jdbcDao = DAOConfigFactory.getInstance().getDAOFactory(appName)
					.getJDBCDAO();
			jdbcDao.openSession(null);
			jdbcDao
					.executeUpdate("update job_details set job_status = 'Failed' where job_status = 'In Progress'");
			jdbcDao.commit();
			jdbcDao.closeSession();
		} catch (final DAOException daoExp)	{
			logger.error("Could not update the table Job Details with the "
									+ "status column value from inprogess to failed."
									+ daoExp.getMessage(), daoExp);
			logger.error(daoExp.getMessage(), daoExp);
			throw daoExp;
		}
	}

	public static boolean checkIfAtLeastOneColumnHasAValue(int index, List<String> attributeList,
			CsvReader csvReader) {
		boolean hasValue = false;
		if (!attributeList.isEmpty()) {
			for (int i = 0; i < attributeList.size(); i++) {
				hasValue = checkIfColumnHasAValue(index, attributeList.get(i), csvReader);
				if (hasValue) {
					break;
				}
			}
		}
		return hasValue;
	}

	public static boolean checkIfColumnHasAValue(int index, String headerName,
			CsvReader csvReader) {
		boolean hasValue = false;
		Object value = csvReader.getValue(headerName);
		if (value != null && !"".equals(value.toString())) {
			hasValue = true;
		}
		return hasValue;
	}

	public static CSVReader getDataReader(InputStream csvFileInputStream)
			throws BulkOperationException {
		CSVReader reader = null;
		try {
			reader = new CSVReader(new InputStreamReader(csvFileInputStream));
		} catch (Exception bulkExp) {
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.reading.csv.file");
			throw new BulkOperationException(errorKey, bulkExp, "");
		}
		return reader;
	}

	public static DataList readCSVColumnNames(CSVReader reader) throws BulkOperationException {
		DataList dataList = new DataList();;
		String[] headers = null;
		try {
			if (reader != null)	{
				headers = reader.readNext();
				for (int i = 0; i < headers.length; i++) {
					dataList.addHeader(headers[i].trim());
				}
				dataList.addHeader(BulkOperationConstants.STATUS);
				dataList.addHeader(BulkOperationConstants.MESSAGE);
				dataList.addHeader(BulkOperationConstants.MAIN_OBJECT_ID);
			}
		} catch (FileNotFoundException fnfExpp)	{
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.not.found");
			throw new BulkOperationException(errorkey, fnfExpp, "");
		} catch (IOException ioExpp) {
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.reading");
			throw new BulkOperationException(errorkey, ioExpp, "");
		}
		return dataList;
	}

	public static DataList readCSVDataRow(String[] csvColumnValues, DataList dataList)
			throws BulkOperationException
	{
		String[] newValues = null;
		try {
			newValues = new String[dataList.getHeaderList().size() + 3];
			for (int m = 0; m < newValues.length; m++) {
				newValues[m] = new String();
			}
			for (int j = 0; j < csvColumnValues.length; j++) {
				newValues[j] = csvColumnValues[j];
			}
			dataList.addNewValue(newValues);
		} catch (Exception exp) {
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorkey, exp, "CSV");
		}
		return dataList;
	}

	/**
	 * Get Bulk Operation Properties Instance.
	 * @return Properties Properties
	 * @throws BulkOperationException BulkOperationException
	 */
	public static Properties getBulkOperationPropertiesInstance() throws BulkOperationException {
		Properties bulkOprProp = null;
		try {
			bulkOprProp = System.getProperties();
			String configDirectory = bulkOprProp.getProperty(BulkOperationConstants.CONFIG_DIR);
			FileInputStream propFile = new FileInputStream("./" + configDirectory + "/"
					+ BulkOperationConstants.BULKOPERATION_INSTALL_PROPERTIES);
			bulkOprProp.load(propFile);
		} catch (FileNotFoundException fnfException) {
			logger.debug("bulkOperation.properties file not found.", fnfException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.not.found");
			throw new BulkOperationException(errorkey, fnfException, "bulkOperation.properties");
		} catch (IOException ioException) {
			logger.debug("Error while accessing bulkOperation.properties file.", ioException);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorkey, ioException, "bulkOperation.properties");
		}
		return bulkOprProp;
	}

	public static void throwExceptionForColumnNameNotFound(RecordMapper recMapper, boolean validate,
			RecordField recField) throws BulkOperationException {
		if (validate) {
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change.validation");
			throw new BulkOperationException(errorkey, null, recField.getColumnName() + ":" + recField.getName()
					+ ":" + recMapper.getClassName());
		}
		else {
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.column.name.change");
			throw new BulkOperationException(errorkey, null, recField.getColumnName() + ":" + recField.getName()
					+ ":" + recMapper.getClassName());
		}
	}
	
	public static String[] concatArrays(String[] array,String[] arrayToBeConcat) {
		String[] concatedArray= new String[array.length+arrayToBeConcat.length];
		System.arraycopy(array, 0, concatedArray, 0, array.length);
		System.arraycopy(arrayToBeConcat, 0, concatedArray, array.length, arrayToBeConcat.length);
		return concatedArray;
	}


	public static Object getObject(RecordMapper recMapper, CsvReader csvReader,
			Map<String, Object> propIdx) throws DAOException {
		Object staticObject = null;
		DAO dao=null;
		
		try {
			final IDAOFactory daofactory = DAOConfigFactory.getInstance().getDAOFactory(CommonServiceLocator.getInstance().getAppName());
			dao = daofactory.getDAO();
			dao.openSession(null);
		
			String hql = BulkOperationUtility.createHQL(recMapper, csvReader, propIdx);
			ArrayList<Object> objects=(ArrayList<Object>)dao.executeQuery(hql);
			staticObject =objects.get(0);
		} catch (Exception e) {
			throw new RuntimeException("Exception occured while retrieving the Object from the DB ");
		} finally {
			dao.closeSession();
		}
		return staticObject;
	}
}
