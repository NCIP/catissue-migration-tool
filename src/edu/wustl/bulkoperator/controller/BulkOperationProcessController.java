
package edu.wustl.bulkoperator.controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.HookingObjectInformation;
import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.processor.IDynamicBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.IStaticBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.StaticBulkOperationProcessor;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.bulkoperator.util.DataList;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;

public class BulkOperationProcessController
{

	private static BulkOperationProcessController bulkOperationProcessController = null;
	private static final Logger logger = Logger
			.getCommonLogger(BulkOperationProcessController.class);

	private BulkOperationProcessController()
	{}

	public static BulkOperationProcessController getBulkOperationControllerInstance()
	{
		if (bulkOperationProcessController == null)
		{
			bulkOperationProcessController = new BulkOperationProcessController();
		}
		return bulkOperationProcessController;
	}

	public void handleBulkOperationJob(InputStream csvFileInputStream, JobData jobData,
			AppServiceInformationObject serviceInformationObject, BulkOperationClass bulkOperationClass)
		throws BulkOperationException
	{
		try
		{
			Long startTime = BulkOperationUtility.getCurrentTimeInSeconds();

			logger.debug("In Bulk Operation controller handle BO Job method");
			IStaticBulkOperationProcessor staticBulkOprProcessor = null;
			staticBulkOprProcessor = new StaticBulkOperationProcessor(
					bulkOperationClass, serviceInformationObject);

			List<IDynamicBulkOperationProcessor> dynamicBulkOprProcessorList = BulkOperationControllerFactory
					.getInstance().getAllDynamicBulkOperationProcessor(bulkOperationClass,
							serviceInformationObject);

			process(staticBulkOprProcessor, dynamicBulkOprProcessorList, startTime,
					csvFileInputStream, jobData);
		}
		catch (BulkOperationException bulkOprExp)
		{
			throw bulkOprExp;
		}
	}

	public void process(
			IStaticBulkOperationProcessor staticBulkOprProcessor,
			List<IDynamicBulkOperationProcessor> dynBulkOprProcessorList,
			long startTime, InputStream csvInputStream, JobData jobData)
			throws BulkOperationException
	{
		Object staticDomainObject = null, dynamicDomainObject = null;
		int failureCount = 0, successCount = 0, currentCSVRowCount = 0;
		try
		{
			StaticBulkOperationProcessor staticProcessor = (StaticBulkOperationProcessor) staticBulkOprProcessor;
			int batchSize = getBatchSize(staticProcessor.getBulkOperationClass());
			CSVReader reader = BulkOperationUtility.getDataReader(csvInputStream);
			DataList dataList = BulkOperationUtility.readCSVColumnNames(reader);
			String[] newValues = null;
			while((newValues = reader.readNext()) != null)
			{
				try
				{
					dataList = BulkOperationUtility.readCSVDataRow(newValues, dataList);
					Map<String, String> csvData = dataList.getValue(currentCSVRowCount);
	
					staticDomainObject = staticBulkOprProcessor.process(csvData, currentCSVRowCount);
					if (!dynBulkOprProcessorList.isEmpty())
					{
						Iterator<IDynamicBulkOperationProcessor> iterator =
							dynBulkOprProcessorList.iterator();
						while (iterator.hasNext())
						{
							HookingObjectInformation hookingObjectInfo = new HookingObjectInformation(
									staticDomainObject);
							IDynamicBulkOperationProcessor dynProcessorInterface = iterator
									.next();
							dynamicDomainObject = dynProcessorInterface.process(csvData,
									currentCSVRowCount, hookingObjectInfo);
						}
					}
					dataList.addStatusMessage(currentCSVRowCount, "Success", " ",
							String.valueOf(staticProcessor.getBulkOperationClass().invokeGetIdMethod(
									staticDomainObject)));
					successCount++;
				}			
				catch (BulkOperationException exp)
				{
					failureCount++;
					dataList.addStatusMessage(currentCSVRowCount, "Failure", " " + exp.getMessage(), " ");
				}
				catch (Exception exp)
				{
					dataList.addStatusMessage(currentCSVRowCount, "Failure", " " + exp.getMessage(), " ");
					failureCount++;
				}
				finally
				{
					if ((currentCSVRowCount % batchSize) == 0)
					{
						try
						{
							insertReportInDatabase(successCount, failureCount,
									JobData.JOB_IN_PROGRESS_STATUS, dataList, staticProcessor
											.getBulkOperationClass().getTemplateName(), startTime,
									jobData);
						}
						catch (BulkOperationException bulkOprExp)
						{
							throw bulkOprExp;
						}
					}
				}
			}
			postProcess(successCount, failureCount, dataList, staticProcessor.getBulkOperationClass()
					.getTemplateName(), startTime, jobData);
		}
		catch (IOException ioExp)
		{
			logger.debug("Error while reading the CSV file.", ioExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.file.reading.error");
			throw new BulkOperationException(errorkey, ioExp, "CSV");
		}
		catch (BulkOperationException bulkOprExp)
		{
			logger.error(bulkOprExp.getMessage(), bulkOprExp);
			throw bulkOprExp;
		}
	}

	private int getBatchSize(BulkOperationClass bulkOperationClass)
	{
		int batchSize = bulkOperationClass.getBatchSize();
		if (batchSize == 0)
		{
			batchSize = 100;
		}
		return batchSize;
	}

	void postProcess(int successCount, int failureCount, DataList dataList, String operationName,
			long startTime, JobData jobData) throws BulkOperationException
	{
		insertReportInDatabase(successCount, failureCount, JobData.JOB_COMPLETED_STATUS, dataList,
				operationName, startTime, jobData);
	}

	private void insertReportInDatabase(int recordsProcessed, int failureCount,
			String statusMessage, DataList dataList, String operationName, long startTime,
			JobData jobData) throws BulkOperationException
	{
		try
		{
			String commonFileName = operationName + jobData.getJobID();
			File file = dataList.createCSVReportFile(commonFileName);
			String[] fileNames = file.getName().split(".csv");
			String zipFilePath = CommonServiceLocator.getInstance().getAppHome()
					+ System.getProperty("file.separator") + fileNames[0];
			File zipFile = BulkOperationUtility.createZip(file, zipFilePath);
			long localTimetaken = (System.currentTimeMillis() / 1000) - startTime;
			Object[] keys = {JobData.LOG_FILE_KEY, JobData.NO_OF_RECORDS_PROCESSED_KEY,
					JobData.NO_OF_FAILED_RECORDS_KEY, JobData.TIME_TAKEN_KEY,
					JobData.NO_OF_TOTAL_RECORDS_KEY, JobData.LOG_FILE_NAME_KEY};
			Object[] values = {zipFile, recordsProcessed, failureCount, localTimetaken,
					dataList.size(), zipFile.getName()};
			jobData.updateJobStatus(keys, values, statusMessage);
			file.delete();
			zipFile.delete();
		}
		catch (BulkOperationException exp)
		{
			logger.error(exp.getMessage(), exp);
			throw new BulkOperationException(exp.getErrorKey(), exp, exp.getMsgValues());
		}
	}	
}