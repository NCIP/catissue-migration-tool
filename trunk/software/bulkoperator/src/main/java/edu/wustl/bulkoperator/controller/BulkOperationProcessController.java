package edu.wustl.bulkoperator.controller;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;

import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.csv.CsvWriter;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileWriter;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.processor.DynCategoryBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.DynEntityBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.IBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.StaticBulkOperationProcessor;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;

public class BulkOperationProcessController
{
	
	private static BulkOperationProcessController bulkOperationProcessController = null;
	private boolean isReRun=false;
	private static final Logger logger = Logger
			.getCommonLogger(BulkOperationProcessController.class);

	private BulkOperationProcessController()
	{}

	public static BulkOperationProcessController getBulkOperationControllerInstance() {
		if (bulkOperationProcessController == null) {
			bulkOperationProcessController = new BulkOperationProcessController();
		}
		return bulkOperationProcessController;
	}

	public void handleBulkOperationJob(InputStream csvFileInputStream,
			JobData jobData,
			AppServiceInformationObject serviceInformationObject,
			BulkOperationClass bulkOperationClass,
			SessionDataBean sessionDataBean) throws BulkOperationException {
		try {
			
			Long startTime = BulkOperationUtility.getCurrentTimeInSeconds();
			logger.debug("In Bulk Operation controller handle BO Job method");
			
			process(bulkOperationClass,startTime, csvFileInputStream, jobData, sessionDataBean,serviceInformationObject);
			
		} catch (BulkOperationException bulkOprExp) {
			throw bulkOprExp;
		}
	}

	public void process(BulkOperationClass bulkOperationClass,
			long startTime, InputStream csvInputStream, JobData jobData,
			SessionDataBean sessionDataBean,AppServiceInformationObject serviceInformationObject) throws BulkOperationException {
		Object staticDomainObject = null;
		int failureCount = 0, successCount = 0, currentCSVRowCount = 0;
		try {
			int batchSize = getBatchSize(bulkOperationClass);
			CsvReader csvReader = CsvFileReader.createCsvFileReader(
					csvInputStream, true);
			
			String[] columnNames = csvReader.getColumnNames();
			
			CsvWriter csvWriter = getCsvWriter(bulkOperationClass, jobData, batchSize,columnNames);	
			
			IBulkOperationProcessor bulkOperationProcessor = getProcessor(bulkOperationClass,serviceInformationObject);

			while (csvReader.next()) {
				try {
					if (isReRun
							&& !BulkOperationConstants.SUCCESS.equalsIgnoreCase(csvReader.getColumn(BulkOperationConstants.STATUS))) {
						processRecord(bulkOperationClass, sessionDataBean,staticDomainObject, currentCSVRowCount,csvReader, 
								columnNames, csvWriter,bulkOperationProcessor);
					} else {
						addRecordToWrite(csvReader,columnNames,csvWriter,BulkOperationConstants.SUCCESS,
								csvReader.getColumn(BulkOperationConstants.MESSAGE),csvReader.getColumn(BulkOperationConstants.MAIN_OBJECT_ID));
					}
					successCount++;

				} catch (Exception exp) {
					addRecordToWrite(csvReader, columnNames, csvWriter,"Failure", exp.getMessage(),"");
					failureCount++;
				} finally {
					if ((currentCSVRowCount % batchSize) == 0) {
						try {
							csvReader.close();
							insertReportInDatabase(successCount, failureCount,
									JobData.JOB_IN_PROGRESS_STATUS,
									csvWriter, bulkOperationClass
											.getTemplateName(), startTime,
									jobData, currentCSVRowCount, false);
							csvWriter.close();
						} catch (BulkOperationException bulkOprExp) {
							throw bulkOprExp;
						}
					}
				}
				currentCSVRowCount++;
			}
			
			postProcess(successCount, failureCount, csvWriter,
					bulkOperationClass.getTemplateName(),
					startTime, jobData, currentCSVRowCount);
		} catch (BulkOperationException bulkOprExp) {
			logger.error(bulkOprExp.getMessage(), bulkOprExp);
			throw bulkOprExp;
		}
	}

	private CsvWriter getCsvWriter(BulkOperationClass bulkOperationClass,
			JobData jobData, int batchSize, String[] columnNames) {
		CsvWriter csvWriter;
		Arrays.sort(columnNames);
		if (Arrays.binarySearch(columnNames, BulkOperationConstants.STATUS) < 0) {
			isReRun=false;
			csvWriter = CsvFileWriter.createCsvFileWriter(
					bulkOperationClass.getTemplateName()
							+ jobData.getJobID() + ".csv",
					BulkOperationUtility.concatArrays(columnNames,
							BulkOperationConstants.DEFAULT_COLUMNS),
					batchSize);
		} else {
			isReRun=true;
			csvWriter = CsvFileWriter.createCsvFileWriter(
					bulkOperationClass.getTemplateName()
							+ jobData.getJobID() + ".csv", columnNames,
					batchSize);
		}
		return csvWriter;
	}

	private IBulkOperationProcessor getProcessor(
			BulkOperationClass bulkOperationClass,
			AppServiceInformationObject serviceInformationObject) {
		IBulkOperationProcessor bulkOperationProcessor;
		if(BulkOperationConstants.ENTITY_TYPE.equalsIgnoreCase(bulkOperationClass.getType())) {
		   bulkOperationProcessor = new StaticBulkOperationProcessor(bulkOperationClass, serviceInformationObject);
		}
		else if(BulkOperationConstants.CATEGORY_TYPE.equalsIgnoreCase(bulkOperationClass.getType())) {
			bulkOperationProcessor=new DynCategoryBulkOperationProcessor(bulkOperationClass, serviceInformationObject);
		}
		else {
			bulkOperationProcessor=new DynEntityBulkOperationProcessor(bulkOperationClass, serviceInformationObject);
		}
		return bulkOperationProcessor;
	}

	private void processRecord(BulkOperationClass bulkOperationClass,SessionDataBean sessionDataBean, Object staticDomainObject,
			int currentCSVRowCount, CsvReader csvReader,String[] columnNames, CsvWriter csvWriter,
			IBulkOperationProcessor bulkOperationProcessor)
			throws BulkOperationException, Exception {
		Object processedObject = bulkOperationProcessor.process(csvReader, currentCSVRowCount,sessionDataBean);
		
		String objectId=null;
		if (processedObject instanceof StaticBulkOperationProcessor) {
			objectId=String.valueOf(bulkOperationClass.invokeGetIdMethod(staticDomainObject));
		} else {
			objectId=String.valueOf(processedObject);
		}
		addRecordToWrite(csvReader, columnNames, csvWriter,"Success", "",objectId);
	}

	private void addRecordToWrite(CsvReader csvReader, String[] columnNames,
			CsvWriter csvWriter,String status,String message, String objectId) {
		csvWriter.nextRow();
		for (String column : columnNames) {
			csvWriter.setColumnValue(column,
					csvReader.getColumn(column));
		}
		addStatusOfRow(csvWriter,status, message, objectId);
	}

	private void addStatusOfRow(CsvWriter csvWriter, String status,
			String meessage, String mainObject) {
		csvWriter.setColumnValue(BulkOperationConstants.STATUS, status);
		csvWriter.setColumnValue(BulkOperationConstants.MESSAGE, meessage);
		csvWriter.setColumnValue(BulkOperationConstants.MAIN_OBJECT_ID, mainObject);
	}

	private int getBatchSize(BulkOperationClass bulkOperationClass) {
		int batchSize = bulkOperationClass.getBatchSize();
		if (batchSize == 0) {
			batchSize = 100;
		}
		return batchSize;
	}

	void postProcess(int successCount, int failureCount,
			CsvWriter csvWriter, String operationName, long startTime,
			JobData jobData, int currentCSVRowCount)
			throws BulkOperationException {
		insertReportInDatabase(successCount, failureCount,
				JobData.JOB_COMPLETED_STATUS, csvWriter, operationName,
				startTime, jobData, currentCSVRowCount, true);
	}

	private void insertReportInDatabase(int recordsProcessed, int failureCount,
			String statusMessage, CsvWriter csvWriter,
			String operationName, long startTime, JobData jobData,
			int currentCSVRowCount, boolean finalexecute)
			throws BulkOperationException {
		try {
			String commonFileName = operationName + jobData.getJobID();
			csvWriter.nextRow();
			csvWriter.flush();
			File file = new File(commonFileName + ".csv");
			String[] fileNames = file.getName().split(".csv");
			String zipFilePath = CommonServiceLocator.getInstance()
					.getAppHome()
					+ System.getProperty("file.separator")
					+ fileNames[0];
			File zipFile = BulkOperationUtility.createZip(file, zipFilePath);
			long localTimetaken = (System.currentTimeMillis() / 1000)
					- startTime;
			Object[] keys = { JobData.LOG_FILE_KEY,
					JobData.NO_OF_RECORDS_PROCESSED_KEY,
					JobData.NO_OF_FAILED_RECORDS_KEY, JobData.TIME_TAKEN_KEY,
					JobData.NO_OF_TOTAL_RECORDS_KEY, JobData.LOG_FILE_NAME_KEY };
			Object[] values = { zipFile, recordsProcessed, failureCount,
					localTimetaken, currentCSVRowCount, zipFile.getName() };
			jobData.updateJobStatus(keys, values, statusMessage);
			if (finalexecute) 
			{
				file.delete();
				zipFile.delete();
			}
		} catch (BulkOperationException exp) {
			logger.error(exp.getMessage(), exp);
			throw new BulkOperationException(exp.getErrorKey(), exp,
					exp.getMsgValues());
		}
	}
}