
package edu.wustl.bulkoperator;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import edu.wustl.bulkoperator.appservice.MigrationAppService;
import edu.wustl.bulkoperator.metadata.Attribute;
import edu.wustl.bulkoperator.metadata.BulkOperationClass;
import edu.wustl.bulkoperator.metadata.ObjectIdentifierMap;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.bulkoperator.util.BulkOperationUtility;

public class BulkOperationProcessor
{
	MigrationAppService migrationAppService;
	BulkOperationClass bulkOperationclass = null;
	ObjectIdentifierMap objectMap;
	int counter = 0;
	boolean isSearchObject = false;

	public BulkOperationClass getMigration()
	{
		return bulkOperationclass;
	}

	public void setMigration(BulkOperationClass migration, MigrationAppService migrationAppService)
	{
		this.bulkOperationclass = migration;
		this.migrationAppService=migrationAppService;
	}

	Set<Long> ids = new LinkedHashSet<Long>();

	public Set<Long> getIds()
	{
		return ids;
	}

	public void setIds(Set<Long> ids)
	{
		this.ids = ids;
	}

	public BulkOperationProcessor(BulkOperationClass migration, MigrationAppService migrationAppService)
	{
		this.bulkOperationclass = migration;
		this.migrationAppService = migrationAppService;
	}

	/**
	 * 
	 * @return
	 */
	public List<String[]> readCSVData(String csvFileAbsoluteName) throws BulkOperationException
	{
		/*Properties migrationInstallProperties = BulkOperationUtility.getMigrationInstallProperties();
		String fName = migrationInstallProperties.getProperty("csv.file.dir");*/
		CSVReader reader = null;
 		List<String[]> list = null;
		try
		{
			reader = new CSVReader(new FileReader(csvFileAbsoluteName));
			list = reader.readAll();
			reader.close();
			System.out.println("Records in CSV files : " + (list.size() - 1));
		}
		catch (FileNotFoundException e)
		{
			System.out.println("\n");
			e.printStackTrace();
			throw new BulkOperationException("\nCSV File Not Found at the specified path.");
		}
		catch (IOException e)
		{
			System.out.println("\n");
			e.printStackTrace();
			throw new BulkOperationException("\nError in reading the CSV File.");
		}		
		return list;
	}
	
	/**
	 * 
	 * @param mainObjectsList
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public void startBulkOperation(String csvFileAbsolutePath) throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
			InvocationTargetException, BulkOperationException
	{
		try
		{	
			List<String[]> list = readCSVData(csvFileAbsolutePath);
			int listSize = list.size();
			int start = 1;
			List<String[]> newList = formatColumnNamesInReportFile(list, 0);
			checkForAddOrEditTemplateType(bulkOperationclass);
			while(start < listSize)
			{
				int rowDataLength = getStringArraySize(list, start);
				String[] newRowData = formatDataColumnsInReportFile(list, start);
				Hashtable<String, String> columnNameHashTable = 
					createHashTable(list, start);							
				Object obj = createDomainObject(columnNameHashTable);							
				try
				{					
					if(isSearchObject)
					{
						Collection<Attribute> attributes = bulkOperationclass.getAttributeCollection();
						processAttributes(obj, bulkOperationclass, attributes, null, columnNameHashTable);
						isSearchObject = false;
						Object searchedObject = migrationAppService.search(obj);
						processObject(searchedObject, bulkOperationclass, objectMap, columnNameHashTable);
						isSearchObject = true;
						migrationAppService.update(searchedObject);
					}
					else
					{
						processObject(obj, bulkOperationclass, objectMap, columnNameHashTable);						
						migrationAppService.insert(obj,bulkOperationclass, objectMap);
					}
					newRowData[rowDataLength] = "Success";
				}
				catch(BulkOperationException e)
				{
					newRowData[rowDataLength] = "Failure";
					newRowData[rowDataLength + 1] = e.getMessage();
				}
				newList.add(newRowData);
				start++;
			}
			boolean flag = createCSVReportFile(newList, csvFileAbsolutePath);
		}
		catch (Exception e)
		{
			throw new BulkOperationException(e.getMessage());
		}
	}

	/**
	 * @param columnNameHashTable
	 * @return
	 * @throws BulkOperationException
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	private Object createDomainObject(
			Hashtable<String, String> columnNameHashTable)
			throws BulkOperationException, ClassNotFoundException,
			NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException
	{
		String objectName = null;
		if(columnNameHashTable.get("Specimen Class") != null)
		{
			objectName = BulkOperationUtility.getSpecimenClassDomainObjectName
								(columnNameHashTable.get("Specimen Class").trim());
		}							
		Class classObj = null;
		if(objectName == null)
		{
			classObj = bulkOperationclass.getClassObject();
		}
		else
		{
			classObj = Class.forName(objectName);
		}				
		Constructor constructor = classObj.getConstructor();
		Object object = constructor.newInstance(null);
		return object;
	}

	/**
	 * @param list
	 * @param start
	 * @return
	 */
	private int getStringArraySize(List<String[]> list, int start)
	{
		String[] rowData = list.get(start);
		int rowDataLength = rowData.length;
		return rowDataLength;
	}

	private String[] formatDataColumnsInReportFile(List<String[]> list, int start)
	{
		String[] rowData = list.get(start);
		int rowDataLength = rowData.length;
		String[] newRowData = new String[rowDataLength + 2];
		for(int i = 0 ; i < rowDataLength; i++)
		{
			newRowData[i] = rowData[i];
		}
		return newRowData;
	}

	private List<String[]> formatColumnNamesInReportFile(List<String[]> list, int listIndex)
	{
		List<String[]> newList = new ArrayList<String[]>();
		String[] newColumnName = formatDataColumnsInReportFile(list, listIndex);
		//String[] newColumnName = list.get(listIndex);
		int columnNameLength = list.get(listIndex).length;
		/*String[] newColumnName = new String[columnNameLength + 2];
		for(int i = 0 ; i < columnNameLength; i++)
		{
			newColumnName[i] = columnName[i];
		}*/
		newColumnName[columnNameLength] = "Success";
		newColumnName[columnNameLength + 1] = "Error Message";
		newList.add(newColumnName);
		return newList;
	}

	private void checkForAddOrEditTemplateType(BulkOperationClass migrationClass)
	{
		Collection<Attribute> attributes = migrationClass.getAttributeCollection();
		Iterator<Attribute> attributeItertor = attributes.iterator();
		while (attributeItertor.hasNext())
		{
			Attribute attribute = attributeItertor.next();
			if(attribute.getUpdateBasedOn())
			{
				isSearchObject = true;
				break;
			}
		}
	}

	private boolean createCSVReportFile(List<String[]> newList, String csvFileAbsolutePath)
		throws BulkOperationException
	{
		/*Properties migrationInstallProperties = BulkOperationUtility.getMigrationInstallProperties();
		String fName = migrationInstallProperties.getProperty("csv.file.dir");*/
		String[] outputFile = csvFileAbsolutePath.split(".csv");
		String outPutFileName = null; 
		if(outputFile.length > 0)
		{
			outPutFileName = outputFile[0] + "_report.csv"; 
		}
		CSVWriter writer = null;
		String[] test = new String[1];
		boolean flag = false;
		try
		{
			writer = new CSVWriter(new FileWriter(outPutFileName), ',');	
			for(String[] stringArray : newList)
			{				
				writer.writeNext(stringArray);
			}			
			writer.close();
			System.out.println("\nPlease refer Output Report at " + outPutFileName);
			flag = true;
		}
		catch (FileNotFoundException e)
		{
			System.out.println("\n");
			e.printStackTrace();
			throw new BulkOperationException("\nCSV File Not Found at the specified path.");
		}
		catch (IOException e)
		{
			System.out.println("\n");
			e.printStackTrace();
			throw new BulkOperationException("\nError in writing the Result File.");
		}
		return flag;
	}

	private Hashtable<String, String> createHashTable(List<String[]> list, int columnValueIndex)
	{
		Hashtable<String, String> hashTable = null;
		String[] columnNames = list.get(0);
		String[] columnValues = list.get(columnValueIndex);
		
		if(columnNames.length == columnValues.length)
		{
			hashTable = new Hashtable<String, String>();
			for(int i = 0; i < columnNames.length; i++)
			{				
				String key = columnNames[i].trim();
				String value = columnValues[i];
				hashTable.put(key, value);
			}
		}		
		return hashTable;
	}

	/**
	 * @param className
	 * @param mainObj
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws ClassNotFoundException 
	 */
	private void processObject(Object mainObj, BulkOperationClass migrationClass,ObjectIdentifierMap objectMap,
			Hashtable<String, String> columnNameHashTable)
			throws BulkOperationException, InstantiationException, IllegalAccessException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			InvocationTargetException, ClassNotFoundException
	{
		if (migrationClass.getContainmentAssociationCollection() != null
				&& !migrationClass.getContainmentAssociationCollection().isEmpty())
		{
			Collection<BulkOperationClass> containmentMigrationClassList = migrationClass
					.getContainmentAssociationCollection();
			processContainments(mainObj, migrationClass, containmentMigrationClassList,objectMap, columnNameHashTable);
		}		
		if (migrationClass.getReferenceAssociationCollection() != null
				&& !migrationClass.getReferenceAssociationCollection().isEmpty())
		{
			Collection<BulkOperationClass> associations = migrationClass
					.getReferenceAssociationCollection();
			processAssociations(mainObj, migrationClass, associations, columnNameHashTable);
		}		
		if (migrationClass.getAttributeCollection() != null
				&& !migrationClass.getAttributeCollection().isEmpty())
		{
			Collection<Attribute> attributes = migrationClass
					.getAttributeCollection();
			processAttributes(mainObj, migrationClass, attributes, null, columnNameHashTable);
		}				
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param containments
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException 
	 * @throws InstantiationException 
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws ClassNotFoundException 
	 */
	private void processContainments(Object mainObj, BulkOperationClass mainMigrationClass,
			Collection<BulkOperationClass> containmentMigrationClassList,
			ObjectIdentifierMap mainObjectIdentifierMap, 
			Hashtable<String, String> columnNameHashTable) throws BulkOperationException,
			InstantiationException, IllegalAccessException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, InvocationTargetException,
			ClassNotFoundException
	{

		Iterator<BulkOperationClass> containmentItert = containmentMigrationClassList.iterator();
		while (containmentItert.hasNext())
		{
			BulkOperationClass containmentMigrationClass = containmentItert.next();
			String cardinality = containmentMigrationClass.getCardinality();
			if (cardinality != null && cardinality.equals("*") && cardinality != "")
			{
				Collection containmentObjectCollection = (Collection) mainMigrationClass.invokeGetterMethod(
						containmentMigrationClass.getRoleName(), null, mainObj, null);
				List sortedList = new ArrayList(containmentObjectCollection); 
				Collections.sort(sortedList, new SortObject());
				containmentObjectCollection = new LinkedHashSet(sortedList);				
				//added for participant race
				Collection<Object> newContainmentObjectCollection = new LinkedHashSet<Object>();
				//create a containment obj and populate data in it from CSV and then add 
				//it to NewContainmentCollection
				if (containmentObjectCollection != null || containmentObjectCollection.isEmpty())
				{
					Iterator collIter = containmentObjectCollection.iterator();						
					if(containmentObjectCollection.isEmpty())
					{
						Collection<BulkOperationClass> innerContainmentMigrationClassList = containmentMigrationClass
									.getContainmentAssociationCollection();
						if(!innerContainmentMigrationClassList.isEmpty() ||
									innerContainmentMigrationClassList != null)
						{
							Collection<BulkOperationClass> associations = containmentMigrationClass.
									getReferenceAssociationCollection();
							for(BulkOperationClass migrationClass : associations)
							{
								Class innerObj = migrationClass.getClassObject();
								Constructor con = innerObj.getConstructor(null);
								Object innerObect = con.newInstance(null);
								
								processObject(innerObect, migrationClass, null,
										columnNameHashTable);
								String roleName = containmentMigrationClass.getRoleName();
								mainMigrationClass.invokeSetterMethod(roleName, new Class[]{innerObj},mainObj, innerObect);
							}

							for(BulkOperationClass migrationClass : innerContainmentMigrationClassList)
							{
								Class innerObj = migrationClass.getClassObject();
								Constructor con = innerObj.getConstructor(null);
								Object innerObect = con.newInstance(null);
								
								processObject(innerObect, migrationClass, null,
										columnNameHashTable);
								if(mainMigrationClass.getIsOneToManyAssociation())
								{
									String roleName = mainMigrationClass.getRoleName();
									migrationClass.invokeSetterMethod(roleName, 
									new Class[]{mainObj.getClass()},innerObect, 
									mainObj);
								}
								containmentObjectCollection.add(innerObect);
							}
							String roleName = containmentMigrationClass.getRoleName();
							mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},mainObj, containmentObjectCollection);
						}
						Collection<BulkOperationClass> associations = containmentMigrationClass.
								getReferenceAssociationCollection();
						for(BulkOperationClass migrationClass : associations)
						{
							Class innerObj = migrationClass.getClassObject();
							Constructor con = innerObj.getConstructor(null);
							Object innerObect = con.newInstance(null);
							
							processObject(innerObect, migrationClass, null,
									columnNameHashTable);
							String roleName = containmentMigrationClass.getRoleName();
							mainMigrationClass.invokeSetterMethod(roleName, new Class[]{innerObj},mainObj, innerObect);
							
						}						
					}
				}
			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object containmentObject = mainMigrationClass.invokeGetterMethod(
						containmentMigrationClass.getRoleName(), null, mainObj, null);
				if (containmentObject != null)
				{
					processObject(containmentObject, containmentMigrationClass,null,
								columnNameHashTable);
					String roleName = containmentMigrationClass.getRoleName();				
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{containmentObject.getClass()},
							mainObj, containmentObject);
				}
				else
				{
					Class test = containmentMigrationClass.getClassObject();					
					Constructor constructor = test.getConstructor(null);					
					containmentObject = constructor.newInstance();					
					processObject(containmentObject, containmentMigrationClass,null,
							columnNameHashTable);					
					String roleName = containmentMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, 
							new Class[]{containmentObject.getClass()}, 
							mainObj, containmentObject);					
				}
			}
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param associationsMigrationClassList
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 */
	private void processAssociations(Object mainObj, BulkOperationClass mainMigrationClass,
			Collection<BulkOperationClass> associationsMigrationClassList,
			Hashtable<String, String> columnNameHashTable) throws BulkOperationException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException
	{
		Iterator<BulkOperationClass> associationItert = associationsMigrationClassList.iterator();
		while (associationItert.hasNext())
		{
			// the associated object to the main object
			BulkOperationClass associationMigrationClass = associationItert.next();
			//the function name related to the associated object used in the main object
			String cardinality = associationMigrationClass.getCardinality();
			//Class<?> associatedClass = Class.forName(associationMigrationClass.getClassName());
			if (cardinality != null && cardinality.equals("*") && cardinality != ""
					&& mainObj != null)
			{
				Collection<Object> newAssociationCollection = new LinkedHashSet<Object>();
				Collection associationObjectCollection = (Collection) mainMigrationClass.invokeGetterMethod(
						associationMigrationClass.getRoleName(), null, mainObj, null);				
				//getterForAssociation.invoke(mainObj, null);
				if (associationObjectCollection != null)
				{
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{Collection.class},
							mainObj, newAssociationCollection);
				}
			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object associatedObject = mainMigrationClass.invokeGetterMethod(
						associationMigrationClass.getRoleName(), null, mainObj, null);
				Object newAssociatedObject = null;
				if (associatedObject != null)
				{
					newAssociatedObject = associationMigrationClass.getNewInstance();
					Collection<Attribute> attributes = associationMigrationClass.getAttributeCollection();
					//added for setting the old values to the object
					processAttributes(associatedObject, associationMigrationClass, 
							attributes, newAssociatedObject, columnNameHashTable);
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associatedObject.getClass()},mainObj, associatedObject);
				}
				else
				{
					Class test = associationMigrationClass.getClassObject();					
					Constructor constructor = test.getConstructor(null);					
					associatedObject = constructor.newInstance(null);
					Collection<Attribute> attributes = associationMigrationClass.getAttributeCollection();
					processAttributes(associatedObject, associationMigrationClass, 
							attributes, newAssociatedObject, columnNameHashTable);					
					String roleName = associationMigrationClass.getRoleName();
					mainMigrationClass.invokeSetterMethod(roleName, new Class[]{associatedObject.getClass()},mainObj, associatedObject);
				}
			}
		}
	}
	/**
	 * 
	 * @param mainObj
	 * @param mainMigrationClass
	 * @param attributes
	 * @throws BulkOperationException
	 */
	private void processAttributes(Object mainObj, BulkOperationClass mainMigrationClass,
			Collection<Attribute> attributes, Object oldObject,
			Hashtable<String, String> columnNameHashTable) throws BulkOperationException
	{
		try
		{
			Iterator<Attribute> attributeItertor = attributes.iterator();
			while (attributeItertor.hasNext())
			{
				Attribute attribute = attributeItertor.next();
				if(isSearchObject && attribute.getUpdateBasedOn())
				{
					processNewAtrributes(mainObj, mainMigrationClass,
							columnNameHashTable, attribute);
				}
				else if(!isSearchObject)
				{
					processNewAtrributes(mainObj, mainMigrationClass,
							columnNameHashTable, attribute);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new BulkOperationException("Error in attributes collection : " + e.getMessage());
		}
	}

	private void processNewAtrributes(Object mainObj,
			BulkOperationClass mainMigrationClass,
			Hashtable<String, String> columnNameHashTable, Attribute attribute)
			throws ClassNotFoundException, NoSuchMethodException,
			SecurityException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			BulkOperationException
	{
		if(attribute.getDataType() != null)
		{
			Class dataTypeClass = Class.forName(attribute.getDataType());
			Constructor constructor = null;
			if(attribute.getCsvColumnName() != null)
			{
				String csvData = columnNameHashTable.get(attribute.getCsvColumnName());
				//System.out.println(attribute.getCsvColumnName() + " : " + csvData + " --> ");
				if(csvData != null && !"".equals(csvData))
				{
					if("java.lang.Integer".equals(attribute.getDataType()) ||
							"java.lang.Double".equals(attribute.getDataType()) ||
								"java.lang.Boolean".equals(attribute.getDataType()) ||
									"java.lang.Long".equals(attribute.getDataType()) ||
										"java.util.Date".equals(attribute.getDataType()))
					{
						constructor = dataTypeClass.getConstructor(Class.forName("java.lang.String"));
					}				
					else
					{
						constructor = dataTypeClass.getConstructor(dataTypeClass);
					}			
					Object setObject = constructor.newInstance(csvData);
					Object attributeObject = null;
					if(mainObj != null)
					{
						attributeObject = mainMigrationClass.invokeGetterMethod(attribute.getName(),
								null, mainObj, null);
						if(csvData != null && !"".equals(csvData))
						{
							mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{dataTypeClass}, 
									mainObj, setObject);
						}			
					}
				}
				else
				{
					Object setObject = mainMigrationClass.invokeGetterMethod(attribute.getName(), null, mainObj, null);
					if(setObject != null)
					{
						mainMigrationClass.invokeSetterMethod(attribute.getName(), new Class[]{setObject.getClass()}, mainObj, setObject);
					}
					/*throw new BulkOperationException("Specified a attribute collection in Meta Data XML file " +
							"but no corresponding column or value found in CSV file for the attribute : "
							+ attribute.getCsvColumnName());*/
				}
			}
			else
			{
				throw new BulkOperationException("Atrribute specified in XML template but corresponding " +
						"Column not found CSV file for attribute : " + attribute.getName()); 
						
			}
		}
		else
		{
			throw new BulkOperationException("In correct data type mentioned in the XML template" +
					" file for attribute name : " + attribute.getName());
		}
	}
}