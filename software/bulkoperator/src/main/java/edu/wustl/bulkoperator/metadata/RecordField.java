
package edu.wustl.bulkoperator.metadata;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RecordField {
	private String name;

	private String columnName;
	
	private String dateFormat;

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getDateFormat() {
		return dateFormat;
	}
	
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public Date getDate(String dateStr) {
		dateFormat = dateFormat != null ? dateFormat :"dd-MM-yyyy";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = null;
		try {
			if(dateStr != null && !dateStr.isEmpty()) {
				date = sdf.parse(dateStr);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error parsing the date for field " + columnName,e);
		}
		return date;
	}
}