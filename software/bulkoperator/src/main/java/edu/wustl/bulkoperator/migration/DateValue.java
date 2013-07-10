package edu.wustl.bulkoperator.migration;

public class DateValue {
	private String value = null;
	private String format = null;

	public DateValue(String value, String format) {
		super();
		this.value = value;
		this.format = format;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

}
