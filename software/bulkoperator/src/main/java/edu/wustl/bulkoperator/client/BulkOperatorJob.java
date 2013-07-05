
package edu.wustl.bulkoperator.client;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.csv.CsvWriter;
import edu.wustl.bulkoperator.csv.impl.CsvFileReader;
import edu.wustl.bulkoperator.csv.impl.CsvFileWriter;
import edu.wustl.bulkoperator.jobmanager.AbstractJob;
import edu.wustl.bulkoperator.jobmanager.DefaultJobStatusListner;
import edu.wustl.bulkoperator.jobmanager.JobData;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.processor.DynamicBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.IBulkOperationProcessor;
import edu.wustl.bulkoperator.processor.StaticBulkOperationProcessor;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationUtility;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.util.global.CommonServiceLocator;
import edu.wustl.common.util.logger.Logger;

public class BulkOperatorJob extends AbstractJob {
	private final static Logger logger = Logger.getCommonLogger(BulkOperatorJob.class);
	
	private InputStream csvFileIn = null;
	
	private BulkOperation bulkOperation = null;
	
	private SessionDataBean sessionDataBean = null;
	
	private boolean isReRun;

	public BulkOperatorJob(SessionDataBean sessionDataBean, String operationName, 
			BulkOperation bulkOperation, InputStream csvFileIn) {				
		super(operationName, String.valueOf(sessionDataBean.getUserId()), new DefaultJobStatusListner());
		this.csvFileIn = csvFileIn;
		this.bulkOperation = bulkOperation;
		this.sessionDataBean = sessionDataBean;
	}

	@Override
	public void doJob()	{
		// This method exists to catch all exceptions that can occur during execution of
		// job and report them back in log file
		try	{	
			executeJob();
		} catch (Exception e) {
			logger.error("Error executing bulk operator job", e);
		}
	}

	public void executeJob() {
		long startTime    = System.currentTimeMillis();
		int successCount  = 0, failureCount = 0, currentRow = 0;
		int batchSize     = bulkOperation.getBatchSize();
		
		CsvReader csvReader = null;
		CsvWriter csvWriter = null;
		try {
			csvReader = CsvFileReader.createCsvFileReader(csvFileIn, true);
			csvWriter = getCsvWriter(csvReader.getHeaderRow());	
			
			IBulkOperationProcessor bulkOperationProcessor = getProcessor();
			while (csvReader.next()) {
				try {
					if (isReRun	&& BulkOperationConstants.SUCCESS.equalsIgnoreCase(csvReader.getValue(BulkOperationConstants.STATUS))) {
						String mainObjId = csvReader.getValue(BulkOperationConstants.MAIN_OBJECT_ID);
						String message = csvReader.getValue(BulkOperationConstants.MESSAGE);
						writeToOutputFile(csvReader, csvWriter, BulkOperationConstants.SUCCESS, message, mainObjId);
					} else {
						processRecord(bulkOperationProcessor, csvReader, currentRow, csvWriter);
					}
					
					successCount++;
				} catch (Exception exp) {
					writeToOutputFile(csvReader, csvWriter, "Failure", exp.getMessage(), "");
					failureCount++;
				} finally {
					if ((currentRow % batchSize) == 0) {
						csvWriter.flush();
						updateJobStatus(startTime, currentRow, successCount, failureCount, JobData.JOB_IN_PROGRESS_STATUS);
					}
				}
				
				currentRow++;
			}

			csvWriter.flush();
			updateJobStatus(startTime, currentRow, successCount, failureCount, JobData.JOB_COMPLETED_STATUS);
		} finally {
			if (csvReader != null) {
				csvReader.close();
			}
			
			if (csvWriter != null) {
				csvWriter.close();
			}
			
			cleanupFiles();
		}
	}
	
	private CsvWriter getCsvWriter(List<String> columnNames) {
		String outputFileName = getOutputFileName();
		String[] inputColNames = columnNames.toArray(new String[0]);
		String[] outputColNames;
		
		if (!columnNames.contains(BulkOperationConstants.STATUS)) {
			isReRun=false;
			outputColNames = BulkOperationUtility.concatArrays(inputColNames, BulkOperationConstants.DEFAULT_COLUMNS);
		} else {
			isReRun=true;
			outputColNames = inputColNames;
		}
		
		return CsvFileWriter.createCsvFileWriter(outputFileName, outputColNames, bulkOperation.getBatchSize());
	}

	private IBulkOperationProcessor getProcessor() {
		RecordMapper recMapper = bulkOperation.getRecordMapper();
		
		IBulkOperationProcessor bulkOperationProcessor = null;
		if (BulkOperationConstants.DYNAMIC.equalsIgnoreCase(recMapper.getType())) {
			bulkOperationProcessor = new DynamicBulkOperationProcessor(sessionDataBean, bulkOperation);
		} else if(BulkOperationConstants.STATIC.equalsIgnoreCase(recMapper.getType())) {
			bulkOperationProcessor = new StaticBulkOperationProcessor(sessionDataBean, bulkOperation);
		}
		
		return bulkOperationProcessor;
	}

	private void processRecord(
			IBulkOperationProcessor bulkOperationProcessor, 
			CsvReader csvReader, int currentRow, CsvWriter csvWriter) {
		
		Long objectId = bulkOperationProcessor.process(csvReader, currentRow);		
		writeToOutputFile(csvReader, csvWriter, "Success", "", objectId.toString());
	}

	private void writeToOutputFile(
			CsvReader csvReader, CsvWriter csvWriter, 
			String status, String message, String objectId) {
		List<String> outputRow = new ArrayList<String>(csvReader.getRow());
		outputRow.add(status);
		outputRow.add(message);
		outputRow.add(objectId);
		csvWriter.write(outputRow);
	}
	
	private void updateJobStatus(long startTime, int currentRow, int successCount, int failureCount, String status) {		
		try {
			String outputFileName = getOutputFileName();
			String zipFilePath = getZipFilePath();

			File zipFile = BulkOperationUtility.createZip(outputFileName, zipFilePath);
			long timeTaken = (System.currentTimeMillis() - startTime) / 1000;
			Object[] keys = { 
					JobData.LOG_FILE_KEY,
					JobData.NO_OF_RECORDS_PROCESSED_KEY,
					JobData.NO_OF_FAILED_RECORDS_KEY, 
					JobData.TIME_TAKEN_KEY,
					JobData.NO_OF_TOTAL_RECORDS_KEY, 
					JobData.LOG_FILE_NAME_KEY 
			};
			
			Object[] values = { 
					zipFile, 
					successCount, 
					failureCount,
					timeTaken, 
					currentRow, 
					zipFile.getName() 
			};
	
			getJobData().updateJobStatus(keys, values, status);
		} catch (Exception exp) {
			logger.error("Error inserting bulk operation status report into DB", exp);
			throw new RuntimeException("Error inserting bulk operation status report into DB", exp);
		}
	}
	
	private void cleanupFiles() {
		new File(getOutputFileName()).delete();
		new File(getZipFilePath()).delete();
	}
	
	private String getOutputFileName() {
		return bulkOperation.getTemplateName() + getJobData().getJobID() + ".csv";
	}
	
	private String getZipFilePath() {
		return CommonServiceLocator.getInstance().getAppHome() + 
				File.separator + 
				getJobData().getJobName() + ".zip"; 
	}
	

}