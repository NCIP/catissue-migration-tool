package edu.wustl.bulkoperator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.exception.ErrorKey;
import edu.wustl.common.util.logger.Logger;


public class DataList
{
	/**
	 * logger Logger - Generic logger.
	 */
	private static final Logger logger = Logger.getCommonLogger(DataList.class);
	private List<String>headerList = new ArrayList<String>();
	private List<Hashtable<String, String>>valueList = new ArrayList<Hashtable<String,String>>();
	private static final String STATUS_KEY="Status";
	private static final String MESSAGE_KEY="Message";
	public void setHeaderList(List<String>list)
	{
		headerList = list;
	}
	public List<String> getHeaderList()
	{
		return headerList;
	}
	public void setHeaderList(String[] headers)
	{
		for(int i=0;i<headers.length;i++)
		{
			addHeader(headers[i]);
		}
	}
	public void addHeader(String header)
	{
		headerList.add(header);
	}
	public void addNewValue(String[] values)
	{
		Hashtable<String,String> valueTable = new Hashtable<String, String>(); 
		valueList.add(valueTable);
		int lastIndex = valueList.size()-1;
		for(int i=0;i<headerList.size();i++)
		{
			String value = null; 
			if(values[i] == null)
			{
				value = "";
			}
			value = values[i].trim();
			setValue(headerList.get(i),value, lastIndex);
		}
	}
	public void setValue(String header, String value, int index)
	{
		Hashtable<String,String> valueTable = valueList.get(index);
		valueTable.put(header, value);
	}
	public Hashtable<String, String>getValue(int index)
	{
		return valueList.get(index);
	}
	
	public int size()
	{
		return  valueList.size();
	}
	
	public boolean checkIfColumnExists(String headerName)
	{
		return headerList.contains(headerName);
	}
	public boolean checkIfColumnHasAValue(int index,String headerName)
	{
		boolean hasValue = false;
		Hashtable<String,String> valueTable = valueList.get(index);
		Object value = valueTable.get(headerName);
		if(value!=null && !"".equals(value.toString()))
		{
			hasValue = true;
		}
		return hasValue;
	}
	public boolean checkIfAtLeastOneColumnHasAValue(int index,List<String> attributeList)
	{
		boolean hasValue = false;
		if(!attributeList.isEmpty())
		{
			for(int i=0;i<attributeList.size();i++)
			{
				hasValue = checkIfColumnHasAValue(index,attributeList.get(i));
				if(hasValue)
				{
					break;
				}
			}
		}
		return hasValue;
	}
	public void addStatusMessage(int index,String status,String message)
	{
		Hashtable<String,String> valueTable = valueList.get(index);
		valueTable.put(STATUS_KEY, status);
		valueTable.put(MESSAGE_KEY, message);
	}
	/**
	 * 
	 * @param csvFileName
	 * @return
	 * @throws IOException
	 */
	public File createCSVReportFile(String csvFileName) throws BulkOperationException
	{
		File file = null;
		FileWriter writer = null;
		try
		{
			file = new File(csvFileName + ".csv");
			file.createNewFile();
			writer = new FileWriter(file);
			int headerListSize = headerList.size();
			int valueListSize = valueList.size();
			StringBuffer line = new StringBuffer(); 
			for(int j=0;j<headerListSize;j++)
			{
				line.append(headerList.get(j)+",");			
			}
			line.deleteCharAt(line.length()-1);
			writer.write(line.append("\n").toString());
			for(int i=0;i<valueListSize;i++)
			{
				line.setLength(0);
				Hashtable<String,String> valueTable = valueList.get(i);
				
				for(int j=0;j<headerListSize;j++)
				{
					if(valueTable.get(headerList.get(j)).contains(","))
					{
						line.append("\"" + valueTable.get(headerList.get(j))+"\"" + ",");
					}
					else
					{
						line.append(valueTable.get(headerList.get(j))+",");
					}
				}
				line.deleteCharAt(line.length()-1);
				line.append("\n");
				writer.write(line.toString());
			}
		}
		catch (IOException ioExp)
		{
			logger.error(ioExp.getMessage(), ioExp);
			logger.error("Error while creating ouput report csv file.", ioExp);
			ErrorKey errorkey = ErrorKey.getErrorKey("bulk.error.csv.file");
			throw new BulkOperationException(errorkey, ioExp, "");
			
		}
		finally
		{
			try
			{
				writer.close();
			}
			catch (IOException exp)
			{
				ErrorKey errorkey = ErrorKey.getErrorKey("bulk.operation.issues");
				throw new BulkOperationException(errorkey, exp, exp.getMessage());
			}
		}
		return file;
	}
	/**
	 * 
	 * @return
	 */
	public boolean isEmpty()
	{
		return valueList.isEmpty();
	}
}
