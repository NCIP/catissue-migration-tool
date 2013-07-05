package edu.wustl.bulkoperator.metadata;

import org.apache.commons.digester3.binder.AbstractRulesModule;

public class BulkOperationTemplateParser extends AbstractRulesModule  {

	@Override
	protected void configure() {
		forPattern("bulk-operation").createObject().ofType(BulkOperation.class)
			.then().setProperties();
					
		
		forPattern("bulk-operation/batch-size")
			.callMethod("setBatchSize")
			.withParamTypes(Integer.class)
			.usingElementBodyAsArgument();

		forPattern("bulk-operation/template-name") 
			.callMethod("setTemplateName")
			.usingElementBodyAsArgument();

		
		forPattern("bulk-operation/record-mapper")
			.createObject().ofType(RecordMapper.class)
			.then().setProperties()
			.addAlias("class", "className")
			.addAlias("form","formName") 
			.then().setNext("addRecordMapper");
		
	

		/****************** For Attribure Collection ***********************/
		forPattern("bulk-operation/record-mapper/field")
			.createObject().ofType(RecordField.class)
			.then().setProperties()
			.addAlias("csv-column", "columnName")
			.addAlias("date-format", "dateFormat")
			.then().setNext("addRecordField"); 
		
		
		/****************** For Association Collection ***********************/
		forPattern("*/association")
			.createObject().ofType(RecordMapper.class)
			.then().setProperties()
			.addAlias("class", "className") 
			.then().setNext("addAssociation"); 

		forPattern("*/association/field")
			.createObject().ofType(RecordField.class)
			.then().setProperties()
			.addAlias("date-format", "dateFormat")
			.addAlias("csv-column", "columnName")
			
			.then().setNext("addRecordField"); 

		
		/****************** For Collections ***********************/
		//
		// association -> many-to-one
		// containment -> one-to-one
		// collection  -> one-to-many
		//
		forPattern("*/collection") 
			.createObject().ofType(RecordMapper.class)
			.then().setProperties()
			.addAlias("class", "className")
			.addAlias("csv-column", "columnName")
			.then().setNext("addCollection");

		forPattern("*/collection/field")
			.createObject().ofType(RecordField.class)
			.then().setProperties()
			.addAlias("csv-column", "columnName")
			.addAlias("date-format", "dateFormat")
			.then().setNext("addRecordField");
		
		
		/****************** For identifying keys ***********************/
		forPattern("*/identifying-columns/csv-column")
			.callMethod("addIdentifyingField")
			.usingElementBodyAsArgument();

		
		/**************** For Identifying Integrator properties *************/
		forPattern("*/integrator")
			.callMethod("setFormIntegrator")
			.usingElementBodyAsArgument();
		
		forPattern("*/integrator/field")
			.createObject().ofType(RecordField.class)
			.then().setProperties()
			.addAlias("csv-column", "columnName")
			.then().setNext("addIntegratorCtxtField");
	
	}		
}



