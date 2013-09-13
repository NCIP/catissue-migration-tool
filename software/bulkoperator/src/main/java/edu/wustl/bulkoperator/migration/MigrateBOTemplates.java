package edu.wustl.bulkoperator.migration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.digester3.Digester;
import org.apache.commons.digester3.binder.DigesterLoader;
import org.apache.commons.digester3.xmlrules.FromXmlRulesModule;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import edu.common.dynamicextensions.domaininterface.BaseAbstractAttributeInterface;
import edu.common.dynamicextensions.domaininterface.CategoryAssociationInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.AbstractContainmentControlInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ContainerInterface;
import edu.common.dynamicextensions.domaininterface.userinterface.ControlInterface;
import edu.common.dynamicextensions.napi.VersionedContainer;
import edu.common.dynamicextensions.napi.impl.VersionedContainerImpl;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.metadata.BulkOperationTemplate;
import edu.wustl.bulkoperator.metadata.RecordField;
import edu.wustl.bulkoperator.metadata.RecordMapper;
import edu.wustl.bulkoperator.migration.export.BulkOperationSerializer;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.DaoUtil;
import edu.wustl.dao.HibernateDAO;
import edu.wustl.dao.JDBCDAO;
import edu.wustl.dao.query.generator.ColumnValueBean;

public class MigrateBOTemplates {
	
	private static DigesterLoader loader;
	
	private static Set<String >knownDateFormats = new HashSet<String>();
	
	private JDBCDAO jdbcDao;
	
	private List<String> colNames = new ArrayList<String>();
	
	private static Logger logger = Logger.getLogger(MigrateBOTemplates.class);

	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
				
		init();
		
		List<Long> templateIds = getAllTemplateIds();
		
		logger.info("Migrating "+templateIds.size()+" templates");
		for (Long templateId : templateIds) {
			MigrateBOTemplates migrateTemplate = null;
			try {
				logger.info("Migrating template with id : "+templateId);
				migrateTemplate = new MigrateBOTemplates();
				migrateTemplate.migrate(templateId);							
			} catch (Exception e) {
				e.printStackTrace();
				logger.info("Error encountered during migration of template with id : "+templateId);
			} finally {
				if (migrateTemplate != null) {
					migrateTemplate.cleanup();
				}
			}
		}
		
		long totalTime = System.currentTimeMillis() - startTime;
		logger.info("Time taken to migrate "+templateIds.size()+" : "+totalTime+" ms");
	}
	
	public static void init() {
		final String mappingXml = new StringBuilder()
			.append(System.getProperty(BulkOperationConstants.CONFIG_DIR))
			.append(File.separator)
			.append("bulkOperatorXMLTemplateRules.xml").toString();
		
		loader = DigesterLoader.newLoader(
			new FromXmlRulesModule() {
				@Override
				protected void loadRules() {
					try {
						InputStream inputStream = new FileInputStream(mappingXml);
						loadXMLRules(inputStream);
					} catch (FileNotFoundException e) {
						throw new RuntimeException("Could not find " + mappingXml);
					}
				}
			});
		
		knownDateFormats.add("dd-MM-yyyy");
		knownDateFormats.add("dd/MM/yyyy");
		knownDateFormats.add("MM-dd-yyyy");
		knownDateFormats.add("MM/dd/yyyy");
		knownDateFormats.add("yyyy-mm-dd");
		knownDateFormats.add("yyyy/mm/dd");
		knownDateFormats.add("MM/dd/yyyy HH:mm");
	}
	
	public static List<Long> getAllTemplateIds() 
	throws Exception {
		List<Long> templateIds = new ArrayList<Long>();
		JDBCDAO jdbcDao = DaoUtil.getJdbcDao();
		ResultSet rs = null;
		
		try {
			rs = jdbcDao.getResultSet(GET_ALL_TEMPL_IDS_SQL, null, null);
			while (rs.next()) {
				templateIds.add(rs.getLong(1));
			}
			
			return templateIds;
		} finally {
			DaoUtil.closeResultSet(rs);
			DaoUtil.closeDao(jdbcDao);
		}
	}
	
	
	public MigrateBOTemplates() {
		try {
			jdbcDao = DaoUtil.getJdbcDao();
		} catch (Exception e) {
			throw new RuntimeException("Error initializing jdbc session", e);
		}
	}
	
	public void cleanup() {
		DaoUtil.closeDao(jdbcDao);
	}	
			
	public void migrate(Long templateId) 
	throws Exception {
		//
		// 1. Get template identified by templateId
		//
		BulkOperationTemplate template = getTemplate(templateId);
		logger.info("Migrating template: " + template.getOperationName());
				
		//
		// 2. Get Old BulkOperation Object
		//
		BulkOperationMetaData oldMetadata = getOldBoObject(template.getXmlTemplate());
		BulkOperationClass boClass = oldMetadata.getBulkOperationClass().iterator().next();
		
		//
		// 3. Determine whether the template is for dynamic form or static entities
		//
		boolean isDynamicForms = boClass.getDynExtCategoryAssociationCollection().size() == 1;		
		if (isDynamicForms) {
			boClass = boClass.getDynExtCategoryAssociationCollection().iterator().next();
		} else if (boClass.getType().equals("Category")) {
			isDynamicForms = true;
		}

		RecordMapper recMapper = null;
		if (isDynamicForms) {
			recMapper = migrateDynamicBO(boClass);
			List<RecordField> integratorFields = getRecordFields(
					recMapper, boClass.getHookingInformation().getAttributeCollection(), null);
			recMapper.setIntegratorCtxtFields(integratorFields);
		} else {
			recMapper = migrateStaticBO(boClass);
		}
	
		if (boClass.getBatchSize() == null) {
				boClass.setBatchSize(oldMetadata.getBatchSize());
		}
			
		if (boClass.getTemplateName() == null) {
			boClass.setTemplateName(oldMetadata.getTemplateName());
		}
		
		BulkOperation bulkOperation = new BulkOperation();	
		bulkOperation.addRecordMapper(recMapper);
		bulkOperation.setBatchSize(boClass.getBatchSize());
		bulkOperation.setTemplateName(boClass.getTemplateName());
				
		String xml = toXml(bulkOperation);
		StringBuilder csvHeader = new StringBuilder();
		for (String colName : colNames) {
			csvHeader.append(colName).append(",");
		}
		
		if (csvHeader.length() > 0) {
			csvHeader.delete(csvHeader.length() - 1, csvHeader.length());
		}

		//
		// Update new template in database along with CSV file with header
		//
		updateTemplate(template.getTemplateName(), csvHeader.toString(), xml);
	}

	private BulkOperationMetaData getOldBoObject(String xml) {
		try	{
			Digester digester = loader.newDigester();
			InputSource xmlTemplateInputSource = new InputSource(new StringReader(xml));	
            BulkOperationMetaData bulkOperationMetaData = digester.parse(xmlTemplateInputSource);
            return bulkOperationMetaData;
		} catch (Exception e) {
			 throw new RuntimeException("Error parsing input xml template", e);
		}		
	}

	private RecordMapper migrateDynamicBO(BulkOperationClass boClass) 
	throws Exception {		
		ContainerInterface oldContainer = getOldContainer(boClass);
		Long newContainerId = getNewContainerId(oldContainer.getId());
		
		RecordMapper recMapper = new RecordMapper();
		recMapper.setFormName(newContainerId.toString());
		
		BulkOperationClass containmentClass = boClass.getContainmentAssociationCollection().iterator().next();
		populateRecMapper(recMapper, containmentClass, oldContainer);
		return recMapper;
	}

	
	private RecordMapper migrateDynamicBO(BulkOperationClass boClass, ContainerInterface oldContainer) {
		RecordMapper recMapper = new RecordMapper();
		return migrateDynamicBO(recMapper, boClass, oldContainer);
	}

	private RecordMapper migrateDynamicBO(RecordMapper recMapper, BulkOperationClass boClass, ContainerInterface oldContainer) {
		boolean isMultiSelect = 
				boClass.getAttributeCollection().size() == 1 &&
				boClass.getAttributeCollection().iterator().next().getName().equals(boClass.getClassName());
		
		if (isMultiSelect) {
			Attribute attr = boClass.getAttributeCollection().iterator().next();
			String newFieldName = getNewFieldName(oldContainer.getControlCollection(), attr);
			
			recMapper.setName(newFieldName);
			recMapper.setColumnName(attr.getCsvColumnName());
			colNames.add(attr.getCsvColumnName());
		} else {
			String newClassName = getNewClassName(boClass.getClassName());
			oldContainer = getSubFormContainer(oldContainer, newClassName);
			recMapper.setName(newClassName);
		}
		
		populateRecMapper(recMapper, boClass, oldContainer);
		return recMapper;
	}
	
	private void populateRecMapper(RecordMapper recMapper, BulkOperationClass boClass, ContainerInterface oldContainer) {
		recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection(), oldContainer));
				
				
		// Set association & collection
		for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
			ContainerInterface childContainer = null;
			if ((childContainer = getChildContainer(oldContainer, containment.getClassName())) != null) {
				populateRecMapper(recMapper, containment, childContainer);
			} else {
				recMapper.addCollection(migrateDynamicBO(containment, oldContainer));
			}
		}
				
		for (BulkOperationClass dynCategory : boClass.getDynExtCategoryAssociationCollection()) {
			recMapper.addCollection(migrateDynamicBO(dynCategory, oldContainer));
		}
		
		for (BulkOperationClass dybEntity : boClass.getDynExtEntityAssociationCollection()) {
			recMapper.addCollection(migrateDynamicBO(dybEntity, oldContainer));
		}
		
		for (BulkOperationClass association : boClass.getReferenceAssociationCollection()) {
			recMapper.addCollection(migrateDynamicBO(association, oldContainer));
		}
	}
	
	private ContainerInterface getOldContainer(BulkOperationClass boClass) 
	throws Exception {
		BulkOperationClass containmentClass = boClass.getContainmentAssociationCollection().iterator().next();
		
		String categoryName = boClass.getClassName();
		String categoryEntityName = containmentClass.getClassName()
				.replace("->", "").replace("-&gt;", "");
		
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(categoryName));
		params.add(new ColumnValueBean(categoryEntityName));
		
		ResultSet resultSet = null;
		Long containerId = null;
		
		try {
			resultSet = jdbcDao.getResultSet(GET_OLD_CONTAINER_ID_SQL, params, null);
			if (resultSet.next()) {
				containerId = resultSet.getLong("identifier");
			}
		} catch (Exception e) {
			throw new RuntimeException("Error obtaining old container id", e);
		} finally {
			DaoUtil.closeResultSet(resultSet);
		}

		return getOldContainer(containerId);
	}
	
	private ContainerInterface getOldContainer(Long containerId) 
	throws Exception {
		HibernateDAO dao = null;				
		try {
			dao = DaoUtil.getHibernateDao();
			String objectType = edu.common.dynamicextensions.domain.userinterface.Container.class.getName();
			return (ContainerInterface)dao.retrieveById(objectType, containerId); 
		} finally {
			DaoUtil.closeDao(dao);		
		}	
	}
	
	private Long getNewContainerId(Long id) 
	throws Exception {
		ResultSet resultSet = null;
		Long formId = null;
		try {
			LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
			params.add(new ColumnValueBean(id));
			resultSet = jdbcDao.getResultSet(GET_NEW_CONTAINER_ID_SQL, params, null);
			if (resultSet.next()) {
				formId = resultSet.getLong("CONTAINER_ID");
			}
			VersionedContainer vc = new VersionedContainerImpl();
			return vc.getContainerId(formId);
		} finally {
			DaoUtil.closeResultSet(resultSet);
		}
	}

	

	
	private RecordMapper migrateStaticBO(BulkOperationClass boClass) {
		RecordMapper recMapper = new RecordMapper();
		
		recMapper.setClassName(boClass.getClassName());
		recMapper.setName(boClass.getRoleName());
		if (boClass.getParentRoleName() != null) {
			recMapper.setRelName(boClass.getParentRoleName());
		}
		
		// Set the List of RecordField
		recMapper.setFields(getRecordFields(recMapper, boClass.getAttributeCollection(), null));
		
		// Set association & collection
		for (BulkOperationClass containment : boClass.getContainmentAssociationCollection()) {
			if(containment.getCardinality().equals("*")) {
				recMapper.addCollection(migrateStaticBO(containment));
			} else {
				recMapper.addAssociation(migrateStaticBO(containment));
			}
		}
		
		for (BulkOperationClass dynCategory : boClass.getDynExtCategoryAssociationCollection()) {
			recMapper.addCollection(migrateStaticBO(dynCategory));
		}
		
		for (BulkOperationClass dybEntity : boClass.getDynExtEntityAssociationCollection()) {
			recMapper.addCollection(migrateStaticBO(dybEntity));
		}
		
		for (BulkOperationClass association : boClass.getReferenceAssociationCollection()) {
			if(association.getCardinality().equals("*")) {
				recMapper.addCollection(migrateStaticBO(association));
			} else {
				recMapper.addAssociation(migrateStaticBO(association));
			}
		}
		
		return recMapper;
	}
	

	private String toXml(BulkOperation bulkOperation) throws IOException {
		BulkOperationSerializer serializer = new BulkOperationSerializer(bulkOperation);
		return(serializer.serialize());
	}
	
	public BulkOperationTemplate getTemplate(Long templateId) 
	throws Exception {
		ResultSet resultSet = null;
		BulkOperationTemplate template = null;
		
		try {
			LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
			params.add(new ColumnValueBean(templateId));
			
			resultSet = jdbcDao.getResultSet(GET_TEMPL_DETAILS_SQL, params, null); 
			if (resultSet.next()) {
				template = new BulkOperationTemplate();
				template.setOperationName(resultSet.getString("OPERATION"));
				template.setTemplateName(resultSet.getString("DROPDOWN_NAME"));
				template.setCsvTemplate(DaoUtil.getString(resultSet.getClob("CSV_TEMPLATE")));
				template.setXmlTemplate(DaoUtil.getString(resultSet.getClob("XML_TEMPALTE")));
			}
		} finally {
			DaoUtil.closeResultSet(resultSet);
		}
		
		return template;
	}
	
	private void updateTemplate(String dropdownName, String csvFileData, String xmlFileData) 
	throws Exception {
		LinkedList<ColumnValueBean> params = new LinkedList<ColumnValueBean>();
		params.add(new ColumnValueBean(csvFileData));
		params.add(new ColumnValueBean(xmlFileData));
		params.add(new ColumnValueBean(dropdownName));
		
		jdbcDao.executeUpdate(UPDATE_TEMPLATE, params);
		jdbcDao.commit();
		logger.info("Template is Updated: " + dropdownName);
	}
			
	private List<RecordField> getRecordFields(RecordMapper recMapper, Collection<Attribute> attributes, ContainerInterface oldContainer) {
		List<RecordField> fields = new ArrayList<RecordField>();
				
		for (Attribute attr : attributes) {
			RecordField recField = new RecordField();
			
			if (oldContainer != null) {
				String newName = getNewFieldName(oldContainer.getControlCollection(), attr);
				if (newName == null) { 
					continue;
				}
				recField.setName(newName);
			} else {
				recField.setName(attr.getName());
			}
			
			recField.setColumnName(attr.getCsvColumnName());
			String format = attr.getFormat();
			if (attr.getDataType() != null && attr.getDataType().contains("Date")) {
				recField.setDateFormat(format);
			}else if (format != null && !format.isEmpty() && knownDateFormats.contains(format)) {
				recField.setDateFormat(format);
			} 
			
			if (attr.getUpdateBasedOn()) {
				recMapper.addIdentifyingField(recField.getName());
			}
			
			fields.add(recField);
			colNames.add(attr.getCsvColumnName());
		}
		
		return fields;
	}
	
	
	private ContainerInterface getSubFormContainer(ContainerInterface oldContainer, String newClassName) {
		ContainerInterface subFormContainer = null;
		
		for (ControlInterface ctrl : oldContainer.getControlCollection()) {
			if (ctrl instanceof AbstractContainmentControlInterface) {
				AbstractContainmentControlInterface containment = (AbstractContainmentControlInterface) ctrl;
				String associationName = getAssociationName(containment.getBaseAbstractAttribute());

				if (associationName.equals(newClassName)) {
					subFormContainer = containment.getContainer();
					break;
				}
			}
		}
		
		return subFormContainer;
	}
	
	private ContainerInterface getChildContainer(ContainerInterface oldContainer, String oldClassName) {
		oldClassName = oldClassName.replaceAll("&gt;", "").replaceAll("-", "").replaceAll(">", "");
		logger.info("Probing child container collection with " + oldClassName);
		for (ContainerInterface childContainer : oldContainer.getChildContainerCollection()) {
			if (childContainer.getAbstractEntity().getName().equals(oldClassName)) {
				return childContainer;
			}
		}
		return null;		
	}


	private String getNewFieldName(Collection<ControlInterface> ctrlCollection, Attribute attr) {
		String newFieldName = null;
		String oldAttrName = null;
		for (ControlInterface oldCtrl : ctrlCollection) {
			if (oldCtrl instanceof AbstractContainmentControlInterface) {
				continue;
			}
			
			if (oldCtrl.getAttibuteMetadataInterface() != null && 
				oldCtrl.getAttibuteMetadataInterface().getName().contains(attr.getName())) {
				
				oldAttrName = getNameByRemovingCatSuffix(oldCtrl.getAttibuteMetadataInterface().getName());
				newFieldName = getNameByRemovingCatSuffix(attr.getName());

				if (oldAttrName.equals(newFieldName)) {
					newFieldName = newFieldName + oldCtrl.getId();
					break;
				}
			}
		}
		
		return newFieldName;
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
	
	private String getNameByRemovingCatSuffix(String attrName) {
		int idx = attrName.lastIndexOf(" Category Attribute");
		if (idx != -1) {
			attrName = attrName.substring(0, idx);
		}
		return attrName;
	}
	
	
	private static final String GET_ALL_TEMPL_IDS_SQL = 
			"SELECT IDENTIFIER FROM CATISSUE_BULK_OPERATION";	
	
	private static final String GET_TEMPL_DETAILS_SQL = 
			"SELECT " +
			"		DROPDOWN_NAME, OPERATION, XML_TEMPALTE, CSV_TEMPLATE " +
			"FROM " +
			"		CATISSUE_BULK_OPERATION " +
			"WHERE " +
			"       IDENTIFIER = ?";
	
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
