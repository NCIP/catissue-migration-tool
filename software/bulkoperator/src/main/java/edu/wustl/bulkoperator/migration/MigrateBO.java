package edu.wustl.bulkoperator.migration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.digester3.Digester;
import org.apache.commons.digester3.binder.DigesterLoader;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;
import edu.common.dynamicextensions.domaininterface.BaseAbstractAttributeInterface;
import edu.common.dynamicextensions.domaininterface.CategoryAssociationInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.AbstractContainmentControlInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ContainerInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ControlInterface;
import edu.common.dynamicextensions.util.DynamicExtensionsUtility;
import edu.wustl.bulkoperator.csv.impl.CsvFileWriter;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.migration.export.BulkOperationSerializer;
import edu.wustl.bulkoperator.templateImport.XmlRulesModule;
import edu.wustl.dao.HibernateDAO;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.query.generator.ColumnValueBean;

public class MigrateBO {
	
	private static List<String> colNames = new ArrayList<String>();
	
	private static String srcDir = "/home/akshay/Desktop/Bo-Migration/categorytemplates/";
	
	private static String tgtDir = "/home/akshay/Desktop/Bo-Migration/MigratedTemplates/";
	
	private static final Logger logger = Logger.getLogger(MigrateBO.class);
	
	private static final String GET_OLD_CONTAINER_ID_SQL = 
			"select " +
			"		c.identifier " +
			"from "	+ 
			"		dyextn_category dc inner join dyextn_abstract_metadata dam on dc.identifier = dam.identifier " +
			"		inner join dyextn_category_entity de on de.identifier = dc.root_category_element " +
			" 		inner join dyextn_container c on c.ABSTRACT_ENTITY_ID = de.identifier " +
			" 		inner join dyextn_abstract_metadata dam1 on dam1.identifier = de.identifier " +
			"Where dam.name = ? and dam1.name = ?";

	private static final String GET_NEW_CONTAINER_ID_SQL = 
			"select " +
			"		distinct container_id " +
			"from " +
			"		dyextn_abstract_form_context " +
			"where old_container_id = ?";
	
	
	public static void main(String[] args) throws Exception {

		// Get Xml from the directory specified 
		String[] formFileNames = null;
		File tmpDir = new File(srcDir);			
			
			
			formFileNames = tmpDir.list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".xml");
				}
			});
		for (String xmlFile : formFileNames) {
			try {
			// Get old BO Object --> BulkOperationMetaData
			
			BulkOperationMetaData oldMetadata = getOldBoObject(srcDir+xmlFile);
			
			// Transform OldMetaData to new BO Object
			BulkOperationClass boClass = ((List<BulkOperationClass>) oldMetadata.getBulkOperationClass()).get(0);
			
			// For Dynamic Extension Records, BulkOperation class is enclosed in boClass.getDynExtCategoryAssociationCollection()
			
			boolean isDynamic = ((List<BulkOperationClass>) boClass.getDynExtCategoryAssociationCollection()).size() == 1;
			if (isDynamic) {
				boClass = ((List<BulkOperationClass>) boClass.getDynExtCategoryAssociationCollection()).get(0);
			}
			else if (boClass.getType().equals("Category")) {
				isDynamic = true;
			}
			if(isDynamic) {
				migrateDyanmicBO(boClass, null);
			}
			
			BulkOperation bulkOperation = new BulkOperation();
			RecordMapper recMapper = migrateToNew(boClass, isDynamic);
			
			bulkOperation.addRecordMapper(recMapper);
			bulkOperation.setBatchSize(boClass.getBatchSize());
			bulkOperation.setTemplateName(boClass.getTemplateName());
		
			// Write ToXml for bulkOperation
			String xml = toXml(bulkOperation);
			
			System.out.println("csv Col Names ---- >"+colNames);

			
			String[] columnNames = colNames.toArray(new String[0]);
	        CSVWriter csvWriter = new CSVWriter(new FileWriter(tgtDir+bulkOperation.getTemplateName()+bulkOperation.getBatchSize()+".csv"));
			CsvFileWriter writer = new CsvFileWriter(csvWriter, columnNames, bulkOperation.getBatchSize()); 
			writer.flush();
			System.out.println(" BO XML "+xml);
		} catch (Exception e) {
			logger.error (" error");
		}
		}
	}


	private static RecordMapper migrateDyanmicBO(BulkOperationClass boClass, ContainerInterface oldContainer) 
	throws Exception {
		
		RecordMapper recMapper = new RecordMapper();
		
		if (oldContainer == null) {
			BulkOperationClass containmentClass = ((List<BulkOperationClass>)boClass.getContainmentAssociationCollection()).get(0);
			
			JDBCDAO jdbcDao = DynamicExtensionsUtility.getJDBCDAO();

			String metadataName = boClass.getClassName();
			String containerMetaDataName = containmentClass.getClassName();
			if(containerMetaDataName.contains("->")) {
				containerMetaDataName = containerMetaDataName.replace("->", "");
			}
			else if (containerMetaDataName.contains("-&gt")) {
				containerMetaDataName = containerMetaDataName.replace("-&gt;", "");
			}
			logger.info("MigrateBoTemplates :: MigrateDynamic :: metadataName :: "+metadataName);
			logger.info("MigrateBoTemplates :: MigrateDynamic :: containerMetaDataName :: "+containerMetaDataName);
			Long oldId = getContainerId(jdbcDao, metadataName, containerMetaDataName);
			logger.info("MigrateBoTemplates :: before getting oldContainer :: MigrateDynamic :: oldId :: "+oldId);
			oldContainer = getOldContainer(oldId);
			logger.info("MigrateBoTemplates :: after getting :: MigrateDynamic :: old container id  :: "+oldContainer.getId());
//			printcontainers(oldContainer);
		
			logger.info("Old container id -->"+oldContainer.getId());
			Long newId = getNewContainerId(jdbcDao, oldContainer.getId());
			logger.info("New container id -->"+newId);

			recMapper.setFormName(newId.toString());
//			recMapper.setIntegratorCtxtFields(getRecordFields(recMapper, boClass.getHookingInformation().getAttributeCollection(), null));
			boClass = containmentClass; // As the First containment collection is the one which holds all collection, association and fields
		} else {
			// 
			// Check For MultiSelect Records
			//
			boolean isMultiSelect = ((List<Attribute>)boClass.getAttributeCollection()).get(0).getName().equals(boClass.getClassName());
			if(boClass.getAttributeCollection().size() ==1 && isMultiSelect) {
				Attribute attr = ((List<Attribute>)boClass.getAttributeCollection()).get(0);
				
				String newName = getDynamicName(oldContainer.getControlCollection(), attr);
				recMapper.setName(newName);
				recMapper.setColumnName(attr.getCsvColumnName());
				colNames.add(attr.getCsvColumnName());
				logger.info("Setting MS :: recMapper.setName -->"+recMapper.getName());
				logger.info("Setting MS :: recMapper.colName -->"+recMapper.getColumnName());
				return recMapper;
			}
			
			//
			// This is of the type sub-form
			//
			String newClassName = getNewClassName(boClass.getClassName());
			oldContainer = getAssociatedContainer(oldContainer, newClassName);
			recMapper.setName(newClassName);
		}

		// Set the List of RecordField
		recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection(), oldContainer));
		
		// Set association & collection
		for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
			recMapper.addCollection(migrateDyanmicBO(containment, oldContainer));
		}
		
		for (BulkOperationClass dynCategory : boClass.getDynExtCategoryAssociationCollection()) {
			recMapper.addCollection(migrateDyanmicBO(dynCategory, oldContainer));
		}
		
		for (BulkOperationClass dybEntity : boClass.getDynExtEntityAssociationCollection()) {
			recMapper.addCollection(migrateDyanmicBO(dybEntity, oldContainer));
		}
		
		for (BulkOperationClass association : boClass.getReferenceAssociationCollection()) {
			recMapper.addCollection(migrateDyanmicBO(association, oldContainer));
		}
		
		return recMapper;
	}

	
	private static BulkOperationMetaData getOldBoObject(String xmlFile) {
		String mappingXml = "migration/bulkOperatorXMLTemplateRules.xml";

		BulkOperationMetaData bulkOperationMetaData  = null;
		try
		{
			
			DigesterLoader digesterLoader = DigesterLoader.newLoader(new XmlRulesModule(mappingXml));
			Digester digester = digesterLoader.newDigester();
            InputStream inputStream = new FileInputStream(xmlFile);
            bulkOperationMetaData = digester.parse(inputStream);
		}
		 catch (Exception e) {
			 e.printStackTrace();
			 logger.info(" Exception occured while parsing the xml"+e.getMessage());
		 }
		
		return bulkOperationMetaData;
	}

	private static void modifyBO(BulkOperationClass boClass) throws Exception {
		JDBCDAO jdbcDao = DynamicExtensionsUtility.getJDBCDAO();
		BulkOperationClass containmentClass = ((List<BulkOperationClass>)boClass.getContainmentAssociationCollection()).get(0);
		
		String metadataName = boClass.getClassName();
		String containerMetaDataName = containmentClass.getClassName();
		Long containerId = getContainerId(jdbcDao, metadataName, containerMetaDataName);
		
		ContainerInterface container = getOldContainer(containerId);
		
		Long newId = getNewContainerId(jdbcDao, container.getId());
		boClass.setClassName(newId.toString());
		
		//		
		// Using old container, find the control for the category attribute in the input template
		// oldContainer.control.attribute.name = name in template
		// (Here we are trying to find control whose category attribute name matches that specified in the input template)
		//
		for (ControlInterface oldCtrl : container.getControlCollection()) {
			String oldName = oldCtrl.getAttibuteMetadataInterface().getAttribute().getName();
			String newName = oldCtrl.getAttibuteMetadataInterface().getAttribute().getName()+oldCtrl.getId();
			updateBOAttributes(boClass, oldName, newName);
			
		}
		
		System.out.println("boClass.getContainmentAssociationCollection()" +boClass.getContainmentAssociationCollection().size());
	}
	
	private static ContainerInterface getOldContainer(Long containerId) 
	throws Exception {
		HibernateDAO dao = null;		
		ContainerInterface container = null;
		
		try {
			dao = DynamicExtensionsUtility.getHibernateDAO();
			String objectType = edu.common.dynamicextensions.domain.userinterface.Container.class.getName();
			container = (ContainerInterface)dao.retrieveById(objectType, containerId); 
		} catch (Exception e) {
			logger.error("Error obtaining container: " + containerId, e);
		} finally {
			DynamicExtensionsUtility.closeDAO(dao);			
		}
		
		return container;		
	}

	private static void updateBOAttributes(BulkOperationClass boClass, String oldName, String newName) {
		
		// TODO :: Iterate thro all the attributes, association and collection 
		//		   And replace oldName with the newName

		// 		   For sub-form, the name will be transformed as below:
		//	       RenalAnnotations[1]->RenalDeath[1] ===> RenalDeath1
	}


	private static Long getNewContainerId(JDBCDAO jdbcDao, Long id) throws Exception {
		Long containerId = null;
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(id));
		ResultSet resultSet = null;
		resultSet = jdbcDao.getResultSet(GET_NEW_CONTAINER_ID_SQL, params, null);

		while (resultSet.next()) {
			containerId = resultSet.getLong("identifier");
		}
		resultSet.close();
		
		return containerId;
	}


	private static Long getContainerId(JDBCDAO jdbcDao, String metadataName, String containerMetaDataName) 
	throws Exception {
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(metadataName));
		params.add(new ColumnValueBean(containerMetaDataName));
		ResultSet resultSet = null;
		Long containerId = null;
		resultSet = jdbcDao.getResultSet(GET_OLD_CONTAINER_ID_SQL, params, null);

		while (resultSet.next()) {
			containerId = resultSet.getLong("identifier");
		}
		resultSet.close();
		
		return containerId;
	}
	private static RecordMapper migrateToNew(BulkOperationClass boClass, boolean isDynamic) {
		RecordMapper recMapper = new RecordMapper();
		
		if (isDynamic) {
			recMapper.setFormName(boClass.getClassName());
			
			// set the integrating contexts required for dynamic records
			recMapper.setIntegratorCtxtFields(getRecordFields(recMapper, boClass.getHookingInformation().getAttributeCollection()));
			boClass  = ((List<BulkOperationClass>)boClass.getContainmentAssociationCollection()).get(0);
		} else {
			recMapper.setClassName(boClass.getClassName());
		}

		recMapper.setName(boClass.getRoleName());
		recMapper.setRelName(boClass.getParentRoleName());
		
		// Set the List of RecordField
		recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection()));
		
		for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
			recMapper.addCollection(migrateToNew(containment, false));
		}
		
		for (BulkOperationClass dynCategory : boClass.getDynExtCategoryAssociationCollection()) {
			recMapper.addCollection(migrateToNew(dynCategory, false));
		}
		
		for (BulkOperationClass dybEntity : boClass.getDynExtEntityAssociationCollection()) {
			recMapper.addCollection(migrateToNew(dybEntity, false));
		}
		
		for (BulkOperationClass association : boClass.getReferenceAssociationCollection()) {
			recMapper.addAssociation(migrateToNew(association, false));
		}
		
		return recMapper;
	}

	private static String toXml(BulkOperation bulkOperation) throws IOException {
		BulkOperationSerializer serializer = new BulkOperationSerializer(bulkOperation);
		return(serializer.serialize());
	}
	

	private static List<RecordField> getRecordFields(RecordMapper recMapper, Collection<Attribute> attributeCollection) {
		List<RecordField> fields = new ArrayList<RecordField>();
		
		for (Attribute attr : attributeCollection) {
			RecordField recField = new RecordField();
			recField.setName(attr.getName());
			recField.setColumnName(attr.getCsvColumnName());
			if(attr.getDataType().contains("Date")) {
				recField.setDateFormat(attr.getFormat());
			}
			if(attr.getUpdateBasedOn()) {
				recMapper.addIdentifyingField(recField.getName());
			}

			fields.add(recField);
			colNames.add(attr.getCsvColumnName());
		}
		return fields;
	}
	
	

			
	private static List<RecordField> getRecordFields(RecordMapper recMapper, Collection<Attribute> attributeCollection, ContainerInterface oldContainer) {
		List<RecordField> fields = new ArrayList<RecordField>();
		logger.info("attributeCollection size -->"+attributeCollection.size());
		
		for (Attribute attr : attributeCollection) {
			RecordField recField = new RecordField();
			if (oldContainer != null) {
				String newName = getDynamicName(oldContainer.getControlCollection(), attr );
				if (newName == null) {
					continue;
				}
				recField.setName(getDynamicName(oldContainer.getControlCollection(), attr ));
			} else {
				recField.setName(attr.getName());
			}
			recField.setColumnName(attr.getCsvColumnName());

			if (attr.getDataType() != null && attr.getDataType().contains("Date")) {
				recField.setDateFormat(attr.getFormat());
			}
			if (attr.getUpdateBasedOn()) {
				recMapper.addIdentifyingField(recField.getName());
			}
			
			fields.add(recField);
			colNames.add(attr.getCsvColumnName());
		}
		
		return fields;
	}
	
	
	private static  ContainerInterface getAssociatedContainer(ContainerInterface oldContainer, String newClassName) {
		ContainerInterface newContainer = null;
		for (ControlInterface ctrl : oldContainer.getControlCollection()) {
			if (ctrl instanceof AbstractContainmentControlInterface) {
				AbstractContainmentControlInterface containment = (AbstractContainmentControlInterface) ctrl;
				String associationName = getAssociationName(containment.getBaseAbstractAttribute());

				logger.info("Get Associated container : associationName --> "+associationName);
				logger.info("Get Associated container : containment.getContainer().getCaption() --> "+containment.getContainer().getCaption());
				logger.info("Get Associated container : boClass.getClassName() --> "+newClassName);

				if (associationName.equals(newClassName)) {
					newContainer = containment.getContainer();
				}
			}
		}
		return newContainer;
	}


	private static String getDynamicName(Collection<ControlInterface> ctrlCollection, Attribute attr) {
		String newAttrName = null;
		
		logger.info(" getDynamicName :: old attr name -->"+attr.getName());

		for (ControlInterface oldCtrl : ctrlCollection) {
			if (!(oldCtrl instanceof AbstractContainmentControlInterface)) {
				logger.info(" getDynamicName :: old attr name -->"+attr.getName());
				logger.info(" getDynamicName :: getAttibuteMetadataInterface ::  name -->"+oldCtrl.getAttibuteMetadataInterface().getName());
				if (oldCtrl.getAttibuteMetadataInterface().getName().contains(attr.getName())) {
					newAttrName = attr.getName();
					int idx = newAttrName.lastIndexOf(" Category Attribute");
					if (idx != -1) {
						newAttrName = newAttrName.substring(0, idx);
					}
					newAttrName = newAttrName + oldCtrl.getId();
					break;
				}
			}
		}
		logger.info(" getDynamicName :: newAttrName -->"+newAttrName);

		return newAttrName;
	}

	
	//
	// For sub-form, the name will be transformed as below:
	// RenalAnnotations[1]->RenalDeath[1] ===> RenalDeath1
	// ClinicalAnnotations[1]-&gtClinicalAnnotations[1] ClinicalAnnotations[1]
	//
	private static String getNewClassName(String className) {
		String[] treeClassName = null;
		
		if (className.contains("-&gt")) {
			treeClassName = className.split("-&gt");
		} else {
			treeClassName = className.split("->");
		}
		className = treeClassName[treeClassName.length-1].replace("[", "").replace("]", "");
		return className;
	}
	
	
	private static String getAssociationName(BaseAbstractAttributeInterface attr) {
		String name = attr.getName();
		if (attr instanceof CategoryAssociationInterface) {
			name = getLastPart(attr.getName(), 3);
		}
		
		return name;
	}
	
	private static String getLastPart(String name, int startIdx) {
		String[] nameParts = name.split("[\\[\\]]");
		int numParts = nameParts.length;			
		return new StringBuilder(nameParts[numParts - startIdx])
			.append(nameParts[numParts - (startIdx - 1)]).toString();		
	}
	
}
