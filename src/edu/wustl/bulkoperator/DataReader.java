package edu.wustl.bulkoperator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVReader;
import edu.wustl.bulkoperator.util.BulkOperationException;


public class DataReader
{
	Properties dataReaderProperties = null; 
	public DataReader(Properties properties)
	{
		dataReaderProperties = properties;
	}
	public static DataReader getNewDataReaderInstance(Properties properties)
	{
		DataReader dataReader = new DataReader(properties);
		return dataReader;
	}
	
	public DataList readData() throws BulkOperationException
	{
		DataList dataList = new DataList();		
		CSVReader reader = null;
 		List<String[]> list = null;
 		InputStream inputStream = (InputStream)dataReaderProperties.get("inputStream");
		try
		{
			reader = new CSVReader(new InputStreamReader(inputStream));
			list = reader.readAll();
			reader.close();		
			int size = list.size();
			if(size>0)
			{
				String[] headers = list.get(0);				
				for(int i=0;i<headers.length;i++)
				{
					dataList.addHeader(headers[i].trim());
				}
				dataList.addHeader("Status");
				dataList.addHeader("Message");
			}
			if(size > 1)
			{	
				for(int i = 1; i < list.size(); i++)
				{
					String[] newValues = new String[list.get(0).length + 2];
					for(int m = 0; m < newValues.length; m++)
					{
						newValues[m] = new String();
					}
					String[] oldValues = list.get(i);
					for(int j = 0; j < oldValues.length; j++)
					{
						newValues[j] = oldValues[j]; 
					}
					dataList.addNewValue(newValues);
				}
			}
			else if(size > 0)
			{
				String[] values = new String[list.get(0).length + 2];
				for(int i = 0; i < (list.get(0).length + 2); i++)
				{
					values[i] = "";
				}
				for(int i = 0;i < (list.get(0).length + 2); i++)
				{					 
					dataList.addNewValue(values);
				}
			}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			throw new BulkOperationException("\nCSV File Not Found at the specified path.");
		}
		catch (IOException e)
		{
			e.printStackTrace();
			throw new BulkOperationException("\nError in reading the CSV File.");
		}
		return dataList;
	}
}
