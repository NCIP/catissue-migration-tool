package edu.wustl.bulkoperator.migration;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.digester3.Digester;
import org.apache.commons.digester3.binder.DigesterLoader;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import edu.common.dynamicextensions.domaininterface.BaseAbstractAttributeInterface;
import edu.common.dynamicextensions.domaininterface.CategoryAssociationInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.AbstractContainmentControlInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ContainerInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ControlInterface;
import edu.common.dynamicextensions.util.DynamicExtensionsUtility;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.BulkOperationTemplate;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.migration.export.BulkOperationSerializer;
import edu.wustl.bulkoperator.templateImport.XmlRulesModule;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.DaoUtil;
import edu.wustl.dao.HibernateDAO;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.query.generator.ColumnValueBean;

public class MigrateBOTemplates {
	
	
	public static void main(String[] args) throws Exception {
		MigrateBOTemplates migrateTemplate = new MigrateBOTemplates();
		migrateTemplate.migrate();
	}
	
	public void migrate() throws Exception {
		
		
		//
		// 1. Get All BulkOperation Templates into List
		//
		List<BulkOperationTemplate> boTemplates = getAllTemplates();

		for (BulkOperationTemplate template : boTemplates) {
			try {
				colNames.clear();
				logger.info(" template.getOperationName -----> "+template.getOperationName());
				
				//
				// 2. Get Old BulkOperation Object
				//
				BulkOperationMetaData oldMetadata = getOldBoObject(template.getXmlTemplate());
				BulkOperationClass boClass = ((List<BulkOperationClass>) oldMetadata.getBulkOperationClass()).get(0);
				
				//
				// 3. Process old BulkOperation Object for DE category objects
				// For DE Records, BulkOperation class is enclosed in boClass.getDynExtCategoryAssociationCollection()
				// Or type is declared as Category - type="Category"
				//
				boolean isDynamic = ((List<BulkOperationClass>) boClass.getDynExtCategoryAssociationCollection()).size() == 1;
				if (isDynamic) {
					boClass = ((List<BulkOperationClass>) boClass.getDynExtCategoryAssociationCollection()).get(0);
				} else if (boClass.getType().equals("Category")) {
						isDynamic = true;
				}
	
				
				//
				// 4. Migrate old BulkOperation object to new BulkOperation object
				//    For Static BO object       - migrateStaticBO(boClass);
				//    For DE(Dynamic) BO object  - migrateDyanmicBO(boClass, null);
				//
				RecordMapper recMapper = null;
				if(isDynamic) {
					recMapper = migrateDyanmicBO(boClass, null);
					recMapper.setIntegratorCtxtFields(getRecordFields(recMapper, boClass.getHookingInformation().getAttributeCollection(), null));

				} else {
					recMapper = migrateStaticBO(boClass);
				}
	
				if (boClass.getBatchSize() == null) {
					boClass.setBatchSize(oldMetadata.getBatchSize());
				}
				BulkOperation bulkOperation = new BulkOperation();
				
				bulkOperation.addRecordMapper(recMapper);
				bulkOperation.setBatchSize(boClass.getBatchSize());
				bulkOperation.setTemplateName(oldMetadata.getTemplateName());
				
				//
				// 5. Convert New BulkOperation object to xml-template
				//
				String xml = toXml(bulkOperation);
				
				//
				// 6. Prepare csv-template
				//
				String csv = colNames.toString().replace("[", "").replace("]", "").replace(", ", ",");
	
				//
				// 7. Update the DB with new xml and csv templates
				//
				updateTemplate(template.getTemplateName(), csv, xml);
			} catch (Exception e) {
				logger.error("Exception occured while migrating "+ template.getTemplateName() +" : "+e.getMessage());
				e.printStackTrace();
			}
		}
	}


	public List<BulkOperationTemplate> getAllTemplates() throws Exception {
		List<BulkOperationTemplate> templates = new ArrayList<BulkOperationTemplate>();
		JDBCDAO jdbcDao = DynamicExtensionsUtility.getJDBCDAO();
		ResultSet resultSet = null;
		resultSet = jdbcDao.getResultSet(GET_ALL_TEMPL_DETAILS_SQL, null, null);
		if (resultSet != null) {
			while (resultSet.next()) {
				BulkOperationTemplate template = new BulkOperationTemplate();
				template.setOperationName(resultSet.getString("OPERATION"));
				template.setTemplateName(resultSet.getString("DROPDOWN_NAME"));
				template.setCsvTemplate(DaoUtil.getString(resultSet.getClob("CSV_TEMPLATE")));
				template.setXmlTemplate(DaoUtil.getString(resultSet.getClob("XML_TEMPALTE")));
				templates.add(template);
			}
		}
		return templates;
	}
	
	
	private BulkOperationMetaData getOldBoObject(String xml) {
		String mappingXml = new StringBuilder().append(System.getProperty(BulkOperationConstants.CONFIG_DIR))
		.append(File.separator).append("bulkOperatorXMLTemplateRules.xml").toString();
		
		BulkOperationMetaData bulkOperationMetaData  = null;
		try	{
			DigesterLoader digesterLoader = DigesterLoader.newLoader(new XmlRulesModule(mappingXml));
			Digester digester = digesterLoader.newDigester();
			InputSource xmlTemplateInputSource = new InputSource(new StringReader(xml));	
            bulkOperationMetaData = digester.parse(xmlTemplateInputSource);
		} catch (Exception e) {
			 e.printStackTrace();
			 logger.info(" Exception occured while parsing the xml"+e.getMessage());
		 }
		
		return bulkOperationMetaData;
	}


	private RecordMapper migrateDyanmicBO(BulkOperationClass boClass, ContainerInterface oldContainer) 
	throws Exception {
		RecordMapper recMapper = new RecordMapper();
		return migrateDyanmicBO(recMapper, boClass, oldContainer);
	}
	
	private RecordMapper migrateDyanmicBO(RecordMapper recMapper, BulkOperationClass boClass, ContainerInterface oldContainer) 
			throws Exception {
			
		boolean isMultiSelect = false;
		if(boClass.getAttributeCollection().size() == 1) {
			isMultiSelect = ((List<Attribute>)boClass.getAttributeCollection()).get(0).getName().equals(boClass.getClassName());	
		}

		if (oldContainer == null && !isMultiSelect) {
			JDBCDAO jdbcDao = DynamicExtensionsUtility.getJDBCDAO();
			BulkOperationClass containmentClass = ((List<BulkOperationClass>)boClass.getContainmentAssociationCollection()).get(0);
			
			String metadataName = boClass.getClassName();
			String containerMetaDataName = containmentClass.getClassName();
			if(containerMetaDataName.contains("->")) {
				containerMetaDataName = containerMetaDataName.replace("->", "");
			}
			else if (containerMetaDataName.contains("-&gt")) {
				containerMetaDataName = containerMetaDataName.replace("-&gt;", "");
			}
			Long oldId = getContainerId(jdbcDao, metadataName, containerMetaDataName);
			oldContainer = getOldContainer(oldId);
			
			Long newId = getNewContainerId(jdbcDao, oldContainer.getId());
			logger.info("Old container id -->"+oldContainer.getId());
			logger.info("New container id -->"+newId);

			recMapper.setFormName(newId.toString());
			boClass = containmentClass; // As the First containment collection is the one which holds all collection, association and fields
			jdbcDao.commit();
			jdbcDao.closeSession();
		} else {
			// 
			// Check For MultiSelect Records
			//
			if(boClass.getAttributeCollection().size() ==1 && isMultiSelect) {
				Attribute attr = ((List<Attribute>)boClass.getAttributeCollection()).get(0);
				//oldContainer = getAssociatedContainer(oldContainer, boClass.getClassName());
				
				System.err.println(oldContainer.getCaption());
				String newName = getDynamicName(oldContainer.getControlCollection(), attr);
				
				recMapper.setName(newName);
				recMapper.setColumnName(attr.getCsvColumnName());
				colNames.add(attr.getCsvColumnName());
				return recMapper;
			}
			
			//
			// This is of the type sub-form
			//
			String newClassName = getNewClassName(boClass.getClassName());
			logger.info("new class name -- >"+newClassName);
			oldContainer = getAssociatedContainer(oldContainer, newClassName);
			recMapper.setName(newClassName);
		}
		
		populateRecMapper(recMapper, boClass, oldContainer);
		return recMapper;
	}
	
	private void populateRecMapper(RecordMapper recMapper, BulkOperationClass boClass, ContainerInterface oldContainer) 
	throws Exception {
		// Set the List of RecordField
				recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection(), oldContainer));
				
				
				// Set association & collection
				for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
					ContainerInterface childContainer = null;
					if ((childContainer = getChildContainer(oldContainer, containment.getClassName())) != null) {
						populateRecMapper(recMapper, containment, childContainer);
					} else {
						recMapper.addCollection(migrateDyanmicBO(containment, oldContainer));
					}
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
	}

	
	private RecordMapper migrateStaticBO(BulkOperationClass boClass) {
		RecordMapper recMapper = new RecordMapper();
		
		recMapper.setClassName(boClass.getClassName());
		recMapper.setName(boClass.getRoleName());
		recMapper.setRelName(boClass.getParentRoleName());
		
		// Set the List of RecordField
		recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection(), null));
		
		// Set association & collection
		for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
			recMapper.addCollection(migrateStaticBO(containment));
		}
		
		for (BulkOperationClass dynCategory : boClass.getDynExtCategoryAssociationCollection()) {
			recMapper.addCollection(migrateStaticBO(dynCategory));
		}
		
		for (BulkOperationClass dybEntity : boClass.getDynExtEntityAssociationCollection()) {
			recMapper.addCollection(migrateStaticBO(dybEntity));
		}
		
		for (BulkOperationClass association : boClass.getReferenceAssociationCollection()) {
			recMapper.addAssociation(migrateStaticBO(association));
		}
		
		return recMapper;
	}
	

	private String toXml(BulkOperation bulkOperation) throws IOException {
		BulkOperationSerializer serializer = new BulkOperationSerializer(bulkOperation);
		return(serializer.serialize());
	}
	
	private void updateTemplate(String dropdownName, String csvFileData, String xmlFileData) 
	throws Exception {
		JDBCDAO jdbcDao = DynamicExtensionsUtility.getJDBCDAO();
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(csvFileData));
		params.add(new ColumnValueBean(xmlFileData));
		params.add(new ColumnValueBean(dropdownName));
		
		DynamicExtensionsUtility.executeUpdateQuery(UPDATE_TEMPLATE, null, jdbcDao, params);
		jdbcDao.commit();
		jdbcDao.closeSession();
		logger.info(" Template is Updated");
	}
	
	private Long getContainerId(JDBCDAO jdbcDao, String metadataName, String containerMetaDataName) 
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

	private ContainerInterface getOldContainer(Long containerId) throws Exception {
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


	private Long getNewContainerId(JDBCDAO jdbcDao, Long id) throws Exception {
		Long containerId = null;
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(id));
		ResultSet resultSet = null;
		resultSet = jdbcDao.getResultSet(GET_NEW_CONTAINER_ID_SQL, params, null);

		while (resultSet.next()) {
			containerId = resultSet.getLong("CONTAINER_ID");
		}
		resultSet.close();
		
		return containerId;
	}

			
	private List<RecordField> getRecordFields(RecordMapper recMapper, Collection<Attribute> attributeCollection, ContainerInterface oldContainer) {
		List<RecordField> fields = new ArrayList<RecordField>();
		logger.info(" getRecordFields :: attributeCollection size -->"+attributeCollection.size());
		
		for (Attribute attr : attributeCollection) {
			RecordField recField = new RecordField();
			if (oldContainer != null) {
				logger.info(" getRecordFields :: old Container -->"+oldContainer.getId());
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
	
	
	private ContainerInterface getAssociatedContainer(ContainerInterface oldContainer, String newClassName) {
		ContainerInterface newContainer = null;
		for (ControlInterface ctrl : oldContainer.getControlCollection()) {
			if (ctrl instanceof AbstractContainmentControlInterface) {
				AbstractContainmentControlInterface containment = (AbstractContainmentControlInterface) ctrl;
				String associationName = getAssociationName(containment.getBaseAbstractAttribute());

				if (associationName.equals(newClassName)) {
					newContainer = containment.getContainer();
					break;
				}
			}
		}
		
		return newContainer;
	}
	
	private ContainerInterface getChildContainer(ContainerInterface oldContainer, String oldClassName) {
		oldClassName = oldClassName.replaceAll("&gt;", "").replaceAll("-", "").replaceAll(">", "");
		System.err.println("Probing child container collection with " + oldClassName);
		for (ContainerInterface childContainer : oldContainer.getChildContainerCollection()) {
			if (childContainer.getAbstractEntity().getName().equals(oldClassName)) {
				return childContainer;
			}
		}
		return null;		
	}


	private String getDynamicName(Collection<ControlInterface> ctrlCollection, Attribute attr) {
		String newAttrName = null;
		
		for (ControlInterface oldCtrl : ctrlCollection) {
			if (!(oldCtrl instanceof AbstractContainmentControlInterface)) {
				if (oldCtrl.getAttibuteMetadataInterface() != null && oldCtrl.getAttibuteMetadataInterface().getName().contains(attr.getName())) {
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
	private String getNewClassName(String className) {
		String[] treeClassName = null;
		
		if (className.contains("-&gt")) {
			treeClassName = className.split("-&gt");
		} else {
			treeClassName = className.split("->");
		}
		className = treeClassName[treeClassName.length-1].replace("[", "").replace("]", "");
		return className;
	}
	
	
	private String getAssociationName(BaseAbstractAttributeInterface attr) {
		String name = attr.getName();
		if (attr instanceof CategoryAssociationInterface) {
			name = getLastPart(attr.getName(), 3);
		}
		
		return name;
	}
	
	private String getLastPart(String name, int startIdx) {
		String[] nameParts = name.split("[\\[\\]]");
		int numParts = nameParts.length;			
		return new StringBuilder(nameParts[numParts - startIdx])
			.append(nameParts[numParts - (startIdx - 1)]).toString();		
	}
	
	
	private static List<String> colNames = new ArrayList<String>();
	
	private static Logger logger = Logger.getLogger(MigrateBOTemplates.class);

	
	private static final String GET_ALL_TEMPL_DETAILS_SQL = 
			"SELECT " +
			"		DROPDOWN_NAME, OPERATION, XML_TEMPALTE, CSV_TEMPLATE " +
			"FROM " +
			"		CATISSUE_BULK_OPERATION ";
	
	private static final String UPDATE_TEMPLATE = 
			"UPDATE " +
					"CATISSUE_BULK_OPERATION " +
			"SET  " +
			"		CSV_TEMPLATE = ?, XML_TEMPALTE = ? " +
			"WHERE " +
			"		DROPDOWN_NAME= ? ";
	
	private static final String GET_OLD_CONTAINER_ID_SQL = 
			"SELECT " +
			"		c.IDENTIFIER " +
			"FROM "	+ 
			"		DYEXTN_CATEGORY dc INNER JOIN DYEXTN_ABSTRACT_METADATA dam ON dc.IDENTIFIER = dam.IDENTIFIER " +
			"		INNER JOIN DYEXTN_CATEGORY_ENTITY de ON de.IDENTIFIER = dc.ROOT_CATEGORY_ELEMENT " +
			" 		INNER JOIN DYEXTN_CONTAINER c ON c.ABSTRACT_ENTITY_ID = de.IDENTIFIER " +
			" 		INNER JOIN DYEXTN_ABSTRACT_METADATA dam1 ON dam1.IDENTIFIER = de.IDENTIFIER " +
			"WHERE dam.NAME = ? AND dam1.NAME = ?";

	private static final String GET_NEW_CONTAINER_ID_SQL = 
			"SELECT " +
			"		DISTINCT CONTAINER_ID " +
			"FROM " +
			"		DYEXTN_ABSTRACT_FORM_CONTEXT " +
			"WHERE OLD_CONTAINER_ID = ?";


}
