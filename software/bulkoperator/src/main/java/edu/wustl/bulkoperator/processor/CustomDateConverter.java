package edu.wustl.bulkoperator.processor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.beanutils.Converter;

import edu.wustl.bulkoperator.metadata.DateValue;

public class CustomDateConverter implements Converter {

	private final static String DEFAULT_FORMAT = "MM/dd/yyyy";

	public Object convert(Class type, Object value)
	{
		SimpleDateFormat format = null;
		String dateValue=null;
		Date date=null;
		if (value instanceof DateValue)
		{
			format = new SimpleDateFormat(((DateValue) value).getFormat());
			dateValue = ((DateValue) value).getValue();
		} else {
			format = new SimpleDateFormat(DEFAULT_FORMAT);
			dateValue=value.toString();
		}
		 try {
			 	date=format.parse(dateValue);
		} catch (ParseException e) {
		
			e.printStackTrace();
		}
		return date;
	}

}
