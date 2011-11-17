package edu.wustl.bulkoperator.controller;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;



import edu.wustl.bulkoperator.appservice.AppServiceInformationObject;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileWriter;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.HookingInformation;
import edu.wustl.bulkoperator.processor.IDynamicBulkOperationProcessor;
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
			StaticBulkOperationProcessor staticBulkOprProcessor = null;
			staticBulkOprProcessor = new StaticBulkOperationProcessor(
					bulkOperationClass, serviceInformationObject);

			List<IDynamicBulkOperationProcessor> dynamicBulkOprProcessorList = BulkOperationControllerFactory
					.getInstance().getAllDynamicBulkOperationProcessor(
							bulkOperationClass, serviceInformationObject);

			process(staticBulkOprProcessor, dynamicBulkOprProcessorList,
					startTime, csvFileInputStream, jobData, sessionDataBean);
		} catch (BulkOperationException bulkOprExp) {
			throw bulkOprExp;
		}
	}

	public void process(StaticBulkOperationProcessor staticBulkOprProcessor,
			List<IDynamicBulkOperationProcessor> dynBulkOprProcessorList,
			long startTime, InputStream csvInputStream, JobData jobData,
			SessionDataBean sessionDataBean) throws BulkOperationException {
		Object staticDomainObject = null;
		Object object = null;
		int failureCount = 0, successCount = 0, currentCSVRowCount = 0;
		try {
			CsvFileReader csvFileReader = CsvFileReader.createCsvFileReader(
					csvInputStream, true);
			StaticBulkOperationProcessor staticProcessor = staticBulkOprProcessor;
			int batchSize = getBatchSize(staticProcessor
					.getBulkOperationClass());
			String[] columnNames = csvFileReader.getColumnNames();
			String[] defaultColumnNames = { BulkOperationConstants.STATUS, BulkOperationConstants.MESSAGE,
					BulkOperationConstants.MAIN_OBJECT_ID };
			CsvFileWriter csvFileWriter = CsvFileWriter.createCsvFileWriter(
					staticProcessor.getBulkOperationClass().getTemplateName()
							+ jobData.getJobID() + ".csv",
					csvFileReader.getColumnNames(), batchSize,
					defaultColumnNames);

			while (csvFileReader.next()) {
				try {
					if (dynBulkOprProcessorList.isEmpty()) {
						staticDomainObject = staticBulkOprProcessor.process(
								csvFileReader, currentCSVRowCount);
					} else {
						HookingInformation hookingInformation = new HookingInformation();
						hookingInformation.setStaticObject(staticDomainObject);
						hookingInformation.setSessionDataBean(sessionDataBean);
						Iterator<IDynamicBulkOperationProcessor> iterator = dynBulkOprProcessorList
								.iterator();
						while (iterator.hasNext()) {
							IDynamicBulkOperationProcessor dynProcessorInterface = iterator
									.next();
							object = dynProcessorInterface.process(
									csvFileReader, currentCSVRowCount,
									hookingInformation);
						}
					}
					csvFileWriter.nextRow();
					for (String column : columnNames) {
						csvFileWriter.setColumnValue(column,
								csvFileReader.getColumn(column));
					}
					String objectId = null;

					if (staticDomainObject != null) {
						objectId = String.valueOf(staticProcessor
								.getBulkOperationClass().invokeGetIdMethod(
										staticDomainObject));
					} else {
						objectId = String.valueOf(object);
					}
					addStatusOfRow(csvFileWriter, "Success", "", objectId);
					successCount++;
				} catch (BulkOperationException exp) {
					csvFileWriter.nextRow();
					for (String column : columnNames) {
						csvFileWriter.setColumnValue(column,
								csvFileReader.getColumn(column));
					}
					failureCount++;
					addStatusOfRow(csvFileWriter, "Failure",
							" " + exp.getMessage(), " ");
				} catch (Exception exp) {
					csvFileWriter.nextRow();
					for (String column : columnNames) {
						csvFileWriter.setColumnValue(column,
								csvFileReader.getColumn(column));
					}
					addStatusOfRow(csvFileWriter, "Failure",
							" " + exp.getMessage(), " ");

					failureCount++;
				} finally {
					if ((currentCSVRowCount % batchSize) == 0) {
						try {

							insertReportInDatabase(successCount, failureCount,
									JobData.JOB_IN_PROGRESS_STATUS,
									csvFileWriter, staticProcessor
											.getBulkOperationClass()
											.getTemplateName(), startTime,
									jobData, currentCSVRowCount, false);
						} catch (BulkOperationException bulkOprExp) {
							throw bulkOprExp;
						}
					}
				}
				currentCSVRowCount++;
			}
			postProcess(successCount, failureCount, csvFileWriter,
					staticProcessor.getBulkOperationClass().getTemplateName(),
					startTime, jobData, currentCSVRowCount);
		} catch (BulkOperationException bulkOprExp) {
			logger.error(bulkOprExp.getMessage(), bulkOprExp);
			throw bulkOprExp;
		}
	}

	private void addStatusOfRow(CsvFileWriter csvFileWriter, String status,
			String meessage, String mainObject) {
		csvFileWriter.setColumnValue(BulkOperationConstants.STATUS, status);
		csvFileWriter.setColumnValue(BulkOperationConstants.MESSAGE, meessage);
		csvFileWriter.setColumnValue(BulkOperationConstants.MAIN_OBJECT_ID, mainObject);
	}

	private int getBatchSize(BulkOperationClass bulkOperationClass) {
		int batchSize = bulkOperationClass.getBatchSize();
		if (batchSize == 0) {
			batchSize = 100;
		}
		return batchSize;
	}

	void postProcess(int successCount, int failureCount,
			CsvFileWriter csvFileWriter, String operationName, long startTime,
			JobData jobData, int currentCSVRowCount)
			throws BulkOperationException {
		insertReportInDatabase(successCount, failureCount,
				JobData.JOB_COMPLETED_STATUS, csvFileWriter, operationName,
				startTime, jobData, currentCSVRowCount, true);
	}

	private void insertReportInDatabase(int recordsProcessed, int failureCount,
			String statusMessage, CsvFileWriter csvFileWriter,
			String operationName, long startTime, JobData jobData,
			int currentCSVRowCount, boolean finalexecute)
			throws BulkOperationException {
		try {
			String commonFileName = operationName + jobData.getJobID();
			csvFileWriter.nextRow();
			csvFileWriter.flush();
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