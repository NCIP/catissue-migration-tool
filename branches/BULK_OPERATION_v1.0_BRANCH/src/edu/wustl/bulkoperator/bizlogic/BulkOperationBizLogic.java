package edu.wustl.bulkoperator.bizlogic;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import oracle.sql.CLOB;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;
import org.xml.sax.InputSource;

import au.com.bytecode.opencsv.CSVWriter;
import edu.wustl.bulkoperator.BulkOperator;
import edu.wustl.bulkoperator.DataList;
import edu.wustl.bulkoperator.DataReader;
import edu.wustl.bulkoperator.client.BulkOperatorJob;
import edu.wustl.bulkoperator.jobmanager.DefaultJobStatusListner;
import edu.wustl.bulkoperator.jobmanager.JobDetails;
import edu.wustl.bulkoperator.jobmanager.JobManager;
import edu.wustl.bulkoperator.jobmanager.JobStatusListener;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.util.AppUtility;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.validator.TemplateValidator;
import edu.wustl.common.beans.NameValueBean;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.bizlogic.DefaultBizLogic;
import edu.wustl.common.exception.ApplicationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;
import edu.wustl.dao.JDBCDAO;

/**
 * Bulk operation business logic from UI.
 * @author sagar_baldwa
 *
 */
public class BulkOperationBizLogic extends DefaultBizLogic
{
	/**
	 * Logger added for Specimen class.
	 */
	private transient final Logger logger = Logger.getCommonLogger(BulkOperationBizLogic.class);
	/**
	 * Get Template Name from DropDown List.
	 * @return List of NameValueBean.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws ApplicationException ApplicationException.
	 */
	public List<NameValueBean> getTemplateNameDropDownList()
		throws BulkOperationException, ApplicationException
	{
		List<NameValueBean> bulkOperationList = new ArrayList<NameValueBean>();
		JDBCDAO jdbcDao = null;
		try
		{
			jdbcDao = AppUtility.openJDBCSession();
			String query = "select DROPDOWN_NAME from catissue_bulk_operation";
			List list = jdbcDao.executeQuery(query);
			if(!list.isEmpty())
			{
				Iterator iterator = list.iterator();
				while(iterator.hasNext())
				{
					List innerList = (List)iterator.next();
					String innerString = (String)innerList.get(0);
					bulkOperationList.add(new NameValueBean(innerString, innerString));
				}
			}
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.dropdown");
			throw new BulkOperationException(errorKey, exp, ""); 
		}
		finally
		{
			AppUtility.closeJDBCSession(jdbcDao);
		}
		return bulkOperationList;
	}

	/**
	 * Get CSV File.
	 * @param dropdownName String.
	 * @return File.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws ApplicationException ApplicationException.
	 */
	public File getCSVFile(String dropdownName) throws 
				BulkOperationException, ApplicationException, SQLException
	{
		File csvFile = null;
		JDBCDAO jdbcDao = null;
		ResultSet resultSet = null;
		try
		{
			jdbcDao = AppUtility.openJDBCSession();
			String query = "select csv_template from catissue_bulk_operation where " +
					"DROPDOWN_NAME like '" + dropdownName +"'";
			resultSet = jdbcDao.getQueryResultSet(query);
			if (resultSet.next())
			{
				StringBuilder sb = getStringFromInputStream(resultSet.getBinaryStream(1));
				csvFile = writeCSVFile(sb.toString(), dropdownName);
			}
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		finally
		{
			if(resultSet != null)
			{
				resultSet.close();
			}
			AppUtility.closeJDBCSession(jdbcDao);
		}
		return csvFile;
	}

	/**
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private StringBuilder getStringFromInputStream(InputStream inputStream) throws SQLException,
			IOException
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		StringBuilder sb = new StringBuilder();
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			sb.append(line);
			inputStream.close();
		}
		return sb;
	}

	/**
	 * Write CSV File.
	 * @param commaSeparatedString String.
	 * @param dropdownName String.
	 * @return File.
	 * @throws Exception Exception.
	 */
	private File writeCSVFile(String commaSeparatedString, String dropdownName)
	throws Exception
	{
		CSVWriter writer = null;
		File csvFile = null;
		try
		{
			String csvFileName = dropdownName + ".csv";
			csvFile = new File(csvFileName);
			csvFile.createNewFile();
			writer = new CSVWriter(new FileWriter(csvFileName), ',');
			String[] stringArray = commaSeparatedString.split(",");
			writer.writeNext(stringArray);
		}
		catch (IOException exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file.writing");
			throw new BulkOperationException(errorkey, exp, "");
		}
		finally
		{
			writer.close();
		}
		return csvFile;
	}
	
	/**
	 * Get Operation Name And XML.
	 * @param dropdownName String.
	 * @return List of String.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws ApplicationException ApplicationException.
	 */
	public List<String> getOperationNameAndXml(String dropdownName, String operationName)
		throws BulkOperationException, SQLException, ApplicationException
	{
		List<String> returnList = new ArrayList<String>();
		ResultSet resultSet = null;
		JDBCDAO jdbcDao = null;
		try
		{
			String query = null;
			if(dropdownName != null && !"".equals(dropdownName))
			{
				query = "select operation, xml_tempalte from " +
				"catissue_bulk_operation " +
				"where DROPDOWN_NAME = '" + dropdownName + "'";
			}
			else
			{
				query = "select operation, xml_tempalte from " +
				"catissue_bulk_operation " +
				"where OPERATION = '" + operationName + "'";
			}
			jdbcDao = AppUtility.openJDBCSession();
//			List list = AppUtility.executeSQLQuery(query);
			resultSet = jdbcDao.getQueryResultSet(query);
			if(resultSet.next())
			{	
				String innerString1 = resultSet.getString(1);
				returnList.add(innerString1);
				StringBuilder xmlString = getStringFromInputStream(resultSet.getBinaryStream(2));
				returnList.add(xmlString.toString());
			}
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.database.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		finally
		{
			if(resultSet != null)
			{
				resultSet.close();
			}
			AppUtility.closeJDBCSession(jdbcDao);
		}
		return returnList;
	}
	
	/**
	 * Convert String To XML.
	 * @param xmlString String.
	 * @return BulkOperationMetaData.
	 * @throws BulkOperationException BulkOperationException.
	 */
	public BulkOperationMetaData convertStringToXml(String xmlString)
		throws BulkOperationException
	{
		BulkOperationMetaData bulkOperationMetaData = null;
		try
		{
			InputSource inputSource = new InputSource(new StringReader(xmlString));
			String mappingFilePath = CommonServiceLocator.getInstance().getPropDirPath()
					+ File.separator + "mapping.xml";
			Mapping mapping = new Mapping();
			mapping.loadMapping(mappingFilePath);
			Unmarshaller un = new Unmarshaller(BulkOperationMetaData.class);
			un.setMapping(mapping);
			bulkOperationMetaData = (BulkOperationMetaData) un.unmarshal(inputSource);
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, exp, exp.getMessage());
		}
		return bulkOperationMetaData;
	}
	/**
	 * Get Operation Name And XML.
	 * @param jobId String.
	 * @return JobDetails JobDetails.
	 * @throws BulkOperationException BulkOperationException.
	 * @throws ApplicationException ApplicationException.
	 */
	public JobDetails getJobDetails(String jobId)
		throws BulkOperationException, ApplicationException
	{
		return (JobDetails)retrieve(JobDetails.class.getName(), Long.valueOf(jobId));
	}
	/**
	 * Initialize BulkOperation.
	 * @param csvFileInputStream InputStream.
	 * @param xmlTemplateInputSource InputSource.
	 * @param retrievedOperationName String.
	 * @param sessionDataBean SessionDataBean.
	 * @return Long.
	 * @throws BulkOperationException BulkOperationException.
	 */
	public Long initBulkOperation(InputStream csvFileInputStream, InputSource xmlTemplateInputSource,
		String retrievedOperationName, SessionDataBean sessionDataBean) throws BulkOperationException
	{
		Long jobId = null;
		DataList dataList = parseCSVDataFile(csvFileInputStream);
		if (dataList != null)
		{
			BulkOperator bulkOperator = parseXMLStringAndGetBulkOperatorInstance(
					retrievedOperationName, xmlTemplateInputSource, dataList);
			validateBulkOperation(retrievedOperationName,dataList,bulkOperator);
			jobId = startBulkOperation(retrievedOperationName, dataList,
					sessionDataBean, bulkOperator);
		}
		else
		{
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.reading.csv.file");
			throw new BulkOperationException(errorKey, null, "");
		}
		return jobId;
	}
	/**
	 * parse CSV Data File.
	 * @param csvFileInputStream InputStream.
	 * @return DataList.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private DataList parseCSVDataFile(InputStream csvFileInputStream) throws BulkOperationException
	{
		DataList dataList = null;
		try
		{
			Properties properties = new Properties();
			properties.put("inputStream", csvFileInputStream);
			dataList = DataReader.getNewDataReaderInstance(properties).readData();
		}
		catch (BulkOperationException bulkExp)
		{
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.reading.csv.file");
			throw new BulkOperationException(errorKey, bulkExp, "");
		}
		return dataList;
	}
	/**
	 * Parse XML String And Get BulkOperator Instance.
	 * @param operationName String.
	 * @param templateInputSource InputSource.
	 * @param dataList DataList.
	 * @return BulkOperator.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private BulkOperator parseXMLStringAndGetBulkOperatorInstance(String operationName,
			InputSource templateInputSource, DataList dataList) throws BulkOperationException
	{
		BulkOperator bulkOperator = null;
		try
		{
			String mappingFilePath = CommonServiceLocator.getInstance().getPropDirPath()
					+ File.separator + "mapping.xml";
			logger.info(mappingFilePath);
			logger.info("templateInputSource : "+templateInputSource);
			InputSource mappingFileInputSource = new InputSource(mappingFilePath);
			bulkOperator = new BulkOperator(templateInputSource, mappingFileInputSource);
		}
		catch (BulkOperationException bulkExp)
		{
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorKey, bulkExp, bulkExp.getMessage());
		}
		return bulkOperator;
	}
	/**
	 * Validate BulkOperation.
	 * @param operationName String.
	 * @param dataList DataList.
	 * @param bulkOperator BulkOperator.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private void validateBulkOperation(String operationName, DataList dataList,
		BulkOperator bulkOperator) throws BulkOperationException
	{
		BulkOperationMetaData metaData = bulkOperator.getMetadata();
		if (metaData != null && metaData.getBulkOperationClass().isEmpty())
		{
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.error.bulk.metadata.xml.file");
			throw new BulkOperationException(errorKey, null, "");
		}
		BulkOperationClass bulkOperationClass = metaData.getBulkOperationClass().iterator().next();
		TemplateValidator templateValidator = new TemplateValidator();
		Set<String> errorList = templateValidator.validateXmlAndCsv(bulkOperationClass,
				operationName, dataList);
		if (!errorList.isEmpty())
		{
			StringBuffer strBuffer = new StringBuffer();
			Iterator<String> errorIterator = errorList.iterator();
			while(errorIterator.hasNext())
			{
				strBuffer.append(errorIterator.next());
			}
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
			throw new BulkOperationException(errorkey, null, strBuffer.toString());
		}
	}
	/**
	 * Start BulkOperation.
	 * @param operationName String.
	 * @param dataList DataList.
	 * @param sessionDataBean SessionDataBean.
	 * @param bulkOperator BulkOperator.
	 * @return Long.
	 * @throws BulkOperationException BulkOperationException.
	 */
	private Long startBulkOperation(String operationName, DataList dataList,
			SessionDataBean sessionDataBean, BulkOperator bulkOperator) throws BulkOperationException
	{
		JobStatusListener jobStatusListner = new DefaultJobStatusListner();
		String bulkOperationClassName = BulkOperationUtility.getClassNameFromBulkOperationPropertiesFile();
		BulkOperatorJob bulkOperatorJob = new BulkOperatorJob(operationName, sessionDataBean.getUserName(),
				null, String.valueOf(sessionDataBean.getUserId()), bulkOperator, dataList,
				bulkOperationClassName, jobStatusListner);
		JobManager.getInstance().addJob(bulkOperatorJob);
		while(bulkOperatorJob.getJobData() == null)
		{
			logger.debug("Job not started yet !!!");
		}
		return bulkOperatorJob.getJobData().getJobID();
	}
}