package edu.wustl.bulkoperator.migration.export;

import static edu.wustl.bulkoperator.migration.export.XmlUtil.writeElement;
import static edu.wustl.bulkoperator.migration.export.XmlUtil.writeElementEnd;
import static edu.wustl.bulkoperator.migration.export.XmlUtil.writeElementStart;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;


public class BulkOperationSerializer {
	
	private BulkOperation bulkOp = null;
	
	private StringBuilder xmlBuilder; 

	private boolean isDynamic = false;
	
	public BulkOperationSerializer(BulkOperation bulkOp) throws IOException {
		this.bulkOp = bulkOp;
		isDynamic = bulkOp.getRecordMapper().getType().equals("dynamic");
		xmlBuilder = new StringBuilder();
	}
	
	
	public String serialize() {
		emitBOStart();
		
		emitRecMapperStart();
		
		serializeRecMapper(bulkOp.getRecordMapper());
		
		emitIntegrationProps();
		
		emitIdentifyingProps();
		
		emitRecMapperEnd();
		
		emitBOEnd();
		
		String xmlTemplate = format(xmlBuilder.toString());
		return xmlTemplate;
	}

    public String format(String unformattedXml) {
        try {
            final Document document = parseXmlFile(unformattedXml);

            OutputFormat format = new OutputFormat(document);
            format.setLineWidth(65);
            format.setIndenting(true);
            format.setIndent(2);
            Writer out = new StringWriter();
            XMLSerializer serializer = new XMLSerializer(out, format);
            serializer.serialize(document);

            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Document parseXmlFile(String in) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(in));
            return db.parse(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	private void emitBOStart() {
		writeElementStart(xmlBuilder,"bulk-operation");	
		writeElement(xmlBuilder, "batch-size", bulkOp.getBatchSize());
		writeElement(xmlBuilder, "template-name", bulkOp.getTemplateName());
	}
	
	private void emitRecMapperStart() {
		Map<String, String> recMap = new HashMap<String, String>();
		boolean isDynamic = bulkOp.getRecordMapper().getType().equals("dynamic");
		
		if (isDynamic) {
			recMap.put("form", bulkOp.getRecordMapper().getFormName());
		} else {
			recMap.put("class", bulkOp.getRecordMapper().getClassName());
		}
		
		writeElementStart(xmlBuilder, "record-mapper",recMap);
	}

	private void serializeRecMapper(RecordMapper recMapper) {

		for (RecordMapper association : recMapper.getAssociations()) {
			Map<String, String> attrs = new HashMap<String, String>();
			attrs.put("name", association.getName());
			attrs.put("class", association.getClassName());
			if (association.getRelName() != null) {
				attrs.put("relName", association.getRelName());
			}
			writeElementStart(xmlBuilder, "association", attrs);
			
			serializeRecMapper(association);
			writeElementEnd(xmlBuilder, "association");
		}
		
		for (RecordMapper collection : recMapper.getCollections()) {
			Map<String, String> attrs = new HashMap<String, String>();
			
			//
			// There was no clear distinction between class-name and name in the earlier templates
			// now name refers to the name of the control.
			//
			if (isDynamic) {
				
				//
				// if RecordMapper has columnName then it is of type multiSelect. 
				//
				if (collection.getColumnName() != null && !collection.getColumnName().isEmpty()) {
					attrs.put("name", collection.getName());
					attrs.put("csv-column", collection.getColumnName());
					writeElement(xmlBuilder, "collection", null, attrs);

					continue;
				}
				attrs.put("name", collection.getName());
			} else {
				attrs.put("name", collection.getName());
				attrs.put("class", collection.getClassName());
				if (collection.getRelName() != null) {
					attrs.put("relName", collection.getRelName());
				}
			}
			
			writeElementStart(xmlBuilder, "collection", attrs);
			
			serializeRecMapper(collection);
			writeElementEnd(xmlBuilder, "collection");
		}
		serializeRecordFields(recMapper.getFields());
	}
	
	private void emitIntegrationProps() {
		if (isDynamic) {
			writeElementStart(xmlBuilder, "integrator");
			serializeRecordFields(bulkOp.getRecordMapper().getIntegratorCtxtFields());
			writeElementEnd(xmlBuilder, "integrator");
		}
	}
	
	private void emitIdentifyingProps() {
		writeElementStart(xmlBuilder, "identifying-columns");
		for (String idField : bulkOp.getRecordMapper().getIdentifyingFields()) {
			writeElement(xmlBuilder, "csv-column", idField);
		}
		writeElementEnd(xmlBuilder, "identifying-columns");
	}
	
	private void emitRecMapperEnd() {
		writeElementEnd(xmlBuilder, "record-mapper");
	}
	private void emitBOEnd() {
		writeElementEnd(xmlBuilder, "bulk-operation");
	}
	
	private void serializeRecordFields(List<RecordField> fields) {
		for (RecordField recField : fields) {
			Map<String, String> field = new HashMap<String, String>();
			field.put("name", recField.getName());
			field.put("csv-column", recField.getColumnName());
			
			if (recField.getDateFormat() != null && !recField.getDateFormat().isEmpty()) {
				field.put("date-format", recField.getDateFormat());
			}

			writeElement(xmlBuilder, "field", null, field);
		}		
	}
}
