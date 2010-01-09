package edu.wustl.bulkoperator.bizlogic;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.sql.CLOB;

import org.exolab.castor.mapping.Mapping;
import org.exolab.castor.xml.Unmarshaller;
import org.xml.sax.InputSource;

import au.com.bytecode.opencsv.CSVWriter;
import edu.wustl.bulkoperator.jobmanager.JobDetails;
import edu.wustl.bulkoperator.metadata.BulkOperationMetaData;
import edu.wustl.bulkoperator.util.AppUtility;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.beans.NameValueBean;
import edu.wustl.common.bizlogic.DefaultBizLogic;
import edu.wustl.common.exception.ApplicationException;
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
			throw new BulkOperationException("Error in retrieving data from" +
					"database for populating dropdown values."); 
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
				BulkOperationException, ApplicationException
	{
		File csvFile = null;
		JDBCDAO jdbcDao = null;
		try
		{
			jdbcDao = AppUtility.openJDBCSession();
			String query = "select csv_template from catissue_bulk_operation where " +
					"DROPDOWN_NAME like '" + dropdownName +"'";
			List list = jdbcDao.executeQuery(query);
			if(!list.isEmpty())
			{
				List innerList = (List)list.get(0);
				String commaSeparatedString = (String)innerList.get(0);
				csvFile = writeCSVFile(commaSeparatedString, dropdownName);
			}
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
		}
		finally
		{
			AppUtility.closeJDBCSession(jdbcDao);
		}
		return csvFile;
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
			throw new BulkOperationException("\nError in writing the CSV Template File.");
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
		throws BulkOperationException, ApplicationException
	{
		List<String> returnList = new ArrayList<String>();
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
			List list = AppUtility.executeSQLQuery(query);
			if(!list.isEmpty())
			{
				List innerList = (List)list.get(0);
				if(!innerList.isEmpty())
				{
					String innerString1 = (String)innerList.get(0);
					returnList.add(innerString1);
					if(innerList.get(1) instanceof CLOB)
					{
						CLOB clob = (CLOB)innerList.get(1);
						Reader reader = clob.getCharacterStream();
						CharArrayWriter writer=new CharArrayWriter();
						int i = -1;
						while ( (i=reader.read())!=-1)
						{
							writer.write(i);
						}
						returnList.add(new String(writer.toCharArray()));
					}
					else
					{
						returnList.add((String)innerList.get(1));
					}
				}
			}
		}
		catch (Exception exp)
		{
			logger.error(exp.getMessage(), exp);
			throw new BulkOperationException("Error in retrieving operation " +
					"name from drop down name."); 
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
			throw new BulkOperationException("bulk.error.loading.bulk.metadata.xml.file");
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
}