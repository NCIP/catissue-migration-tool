package edu.wustl.bulkoperator.metadata;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.digester3.Digester;
import org.apache.commons.digester3.binder.DigesterLoader;
import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.util.BulkOperationConstants;

public class BulkOperation {
	private static final DigesterLoader loader = DigesterLoader.newLoader(new BulkOperationTemplateParser());
	
	private static final SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
	
	private Integer batchSize;
	
	private String templateName;
	
	private List<RecordMapper> recordMapperList = new ArrayList<RecordMapper>();

	public static BulkOperation fromXml(String xml) {
		try {
			return fromXml(xml, false);
		} catch (Exception e) {
			throw new RuntimeException("Error parsing input xml", e);
		}
	}
	
	public static BulkOperation fromXml(String xml, boolean validate) throws Exception {
		InputSource xmlTemplateInputSource = new InputSource(new StringReader(xml));	
		
		Digester digester = loader.newDigester();

		if(validate) {
			String xsdLocation = new StringBuilder()
				.append(System.getProperty(BulkOperationConstants.CONFIG_DIR))
				.append(File.separator).append("BulkOperations.xsd").toString();
	
			File schemaLocation = new File(xsdLocation);
			Schema schema = factory.newSchema(schemaLocation);
			
			digester.setValidating(validate);
			digester.setXMLSchema(schema);
	
			Validator validator = schema.newValidator();
			Source xmlFileForValidation = new StreamSource(new StringReader(xml));
			validator.validate(xmlFileForValidation);
		}
        return ((BulkOperation)digester.parse(xmlTemplateInputSource));
	}
	
	public int getBatchSize() {
		int result = 100;
		if (batchSize == null || batchSize != 0) {
			result = batchSize;
		}
		return result;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public List<RecordMapper> getRecordMapperList() {
		return recordMapperList;
	}

	public void setRecordMapperList(List<RecordMapper> recordMapperList) {
		this.recordMapperList = recordMapperList;
	}
	
	public void addRecordMapper(RecordMapper recordMapper) {
		this.recordMapperList.add(recordMapper);
	}
	
	public RecordMapper getRecordMapper() {
		return recordMapperList != null && !recordMapperList.isEmpty() ? recordMapperList.get(0) : null;
	}
}
