
package edu.wustl.migrator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import edu.wustl.catissuecore.domain.Specimen;
import edu.wustl.migrator.dao.HibernateSessionHandler;
import edu.wustl.migrator.metadata.MigrationClass;

public class Processor
{

	MigrationClass migrationClass = null;

	public MigrationClass getMigration()
	{
		return migrationClass;
	}

	public void setMigration(MigrationClass migration)
	{
		this.migrationClass = migration;
	}

	List<Long> ids = new ArrayList<Long>();

	public List<Long> getIds()
	{
		return ids;
	}

	public void setIds(List<Long> ids)
	{
		this.ids = ids;
	}

	public Processor(MigrationClass migration)
	{
		this.migrationClass = migration;
		this.ids = ids;
	}

	/**
	 * 
	 * @return
	 */
	public void fetchObjectIdentifier(Connection conn)
	{
		//Connection conn = null;
		//DAO dao = new DAO();
		//conn = dao.establishConnection();
		String sqlQuery = migrationClass.getSql();
		List<Long> objectList = new ArrayList<Long>();

		ResultSet rs = null;
		Statement stmt = null;
		ids.clear();
		try
		{
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlQuery);
			while (rs.next())
			{
				ids.add(rs.getLong(1));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				rs.close();
				stmt.close();
				//dao.destroyConnection(conn);
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}

		}

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
	public List<Object> fetchObjects() throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
			InvocationTargetException
	{
		Session session = null;
		Connection con = null;
		//DAO dao = new DAO();
		List<Object> listForInsertion = new ArrayList<Object>();
		try
		{
			//con = dao.establishConnection();
			session = HibernateSessionHandler.getNewSession();
			con = session.connection();
			fetchObjectIdentifier(con);
			String className = migrationClass.getClassName();
			String query = null;
			List<Object> objectList = null;
			if (ids == null && ids.isEmpty())
			{
				query = "from " + className;
			}
			else
			{
				String idsSet = "(";
				Iterator<Long> idItert = ids.iterator();
				int noOfIds = ids.size();
				int counter = 1;
				while (idItert.hasNext())
				{
					Long id = idItert.next();
					if (counter == noOfIds)
					{
						idsSet += id;
					}
					else
					{
						idsSet += id + ",";
					}
					counter++;
				}
				idsSet += ")";
				query = "from " + className + " where id in " + idsSet;
			}

			Query hqlquery = session.createQuery(query);
			objectList = hqlquery.list();
			if (objectList != null && !objectList.isEmpty())
			{
				Collections.sort(objectList, new SortObject());
				Iterator<Object> iterator = objectList.iterator();
				while (iterator.hasNext())
				{
					//object of the type = "main"
					Object object = iterator.next();
					Specimen s = (Specimen)object;
					if(!s.getLineage().equalsIgnoreCase("New"))
					{
					processObject(object, migrationClass, con);
					
					listForInsertion.add(object);
					}
				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			//dao.destroyConnection(con);
			HibernateSessionHandler.closeSession();
		}
		return listForInsertion;
	}

	/**
	 * @param className
	 * @param mainObj
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	private void processObject(Object mainObj, MigrationClass tempMigrationClass, Connection con)
			throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
			InvocationTargetException, InstantiationException
	{

		if (tempMigrationClass.getReferenceAssociationCollection() != null
				&& !tempMigrationClass.getReferenceAssociationCollection().isEmpty())
		{
			Collection<MigrationClass> associations = tempMigrationClass
					.getReferenceAssociationCollection();

			processAssociations(mainObj, tempMigrationClass, associations, con);

		}
		if (tempMigrationClass.getContainmentAssociationCollection() != null
				&& !tempMigrationClass.getContainmentAssociationCollection().isEmpty())
		{
			Collection<MigrationClass> containments = tempMigrationClass
					.getContainmentAssociationCollection();
			processContainments(mainObj, tempMigrationClass, containments, con);
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param containments
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException 
	 * @throws InstantiationException 
	 */
	private void processContainments(Object mainObj, MigrationClass tempMigrationClass,
			Collection<MigrationClass> containments, Connection con) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException, ClassNotFoundException,
			InstantiationException
	{
		Class<?> mainObjectClass = Class.forName(tempMigrationClass.getClassName());
		Iterator<MigrationClass> containmentItert = containments.iterator();
		while (containmentItert.hasNext())
		{
			MigrationClass migration = containmentItert.next();
			Class<?> containmentClass = Class.forName(migration.getClassName());
			String gettterForContainment = getFunctionName(migration.getRoleName(), "get");
			System.out.println("functionName + con = " + gettterForContainment);
			Method getContainment = mainObjectClass.getMethod(gettterForContainment, null);
			String cardinality = migration.getCardinality();
			String isToSetNull = migration.getIsToSetNull();
			if (cardinality != null && cardinality.equals("*") && cardinality != "")
			{
				Collection coll = (Collection) getContainment.invoke(mainObj, null);
				//added for participant race
				Collection<Object> newCol = new HashSet<Object>();
				
					if (coll != null && !coll.isEmpty())
					{
						if (isToSetNull.equalsIgnoreCase("Yes"))
						{
							newCol = null;
						}
						else
						{
						Iterator collIter = coll.iterator();
						while (collIter.hasNext())
						{
							Object obj = collIter.next();
							Method setId = containmentClass.getMethod("setId", Long.class);
							Long id = null;
							setId.invoke(obj, id);
							processObject(obj, migration, con);
							newCol.add(obj);
						}
					}
					//added for participant race
					String setterForContainment = getFunctionName(migration.getRoleName(), "set");
					Method setContainment = mainObjectClass.getMethod(setterForContainment,
							Collection.class);
					setContainment.invoke(mainObj, newCol);
				}
			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object obj = getContainment.invoke(mainObj, null);
				
					if (obj != null)
					{if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						obj = null;
					}
					else
					{
						Method setId = containmentClass.getMethod("setId", Long.class);
						Long id = null;
						setId.invoke(obj, id);
						processObject(obj, migration, con);
					}
					Method setMethod = null;
					try
					{
						setMethod = mainObjectClass.getMethod(getFunctionName(migration
								.getRoleName(), "set"), containmentClass);
					}
					catch (NoSuchMethodException e)
					{
						setMethod = mainObjectClass.getMethod(getFunctionName(migration
								.getRoleName(), "set"), containmentClass.getSuperclass());
					}
					setMethod.invoke(mainObj, obj);
				}
			}
		}
	}

	/**
	 * @param mainObj
	 * @param mainObjectClass
	 * @param associations
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 */
	private void processAssociations(Object mainObj, MigrationClass tempMigrationClass,
			Collection<MigrationClass> associations, Connection con) throws NoSuchMethodException,
			IllegalAccessException, InvocationTargetException, ClassNotFoundException,
			InstantiationException
	{
		Class<?> mainObjectClass = Class.forName(tempMigrationClass.getClassName());
		Iterator<MigrationClass> associationItert = associations.iterator();
		while (associationItert.hasNext())
		{
			// the associated object to the main object
			MigrationClass migration = (MigrationClass) associationItert.next();
			Long productionId = null;
			//the function name related to the associated object used in the main object
			String associatedFunction = getFunctionName(migration.getRoleName(), "get");
			System.out.println("functionName + assoc = " + associatedFunction);
			Method getterForAssociation = mainObjectClass.getMethod(associatedFunction, null);
			String cardinality = migration.getCardinality();
			String isToSetNull = migration.getIsToSetNull();
			Class<?> associatedClass = Class.forName(migration.getClassName());
			if (cardinality != null && cardinality.equals("*") && cardinality != ""
					&& mainObj != null)
			{
				Collection<Object> newSet = new HashSet<Object>();

				Collection coll = (Collection) getterForAssociation.invoke(mainObj, null);
				if (coll != null && !coll.isEmpty())
				{
					if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						newSet = null;
					}
					else
					{
						Iterator it = coll.iterator();
						while (it.hasNext())
						{
							Object persitentObj = it.next();
							//get id must of the persistent object
							Method getId = persitentObj.getClass().getMethod("getId", null);
							Object id = getId.invoke(persitentObj, null);
							// setid must be of the new object class
							Object newobj = associatedClass.newInstance();
							Method setId = associatedClass.getMethod("setId", Long.class);
							if (id != null)
							{
								productionId = getProductionId(Long.valueOf(id.toString()),
										persitentObj.getClass().getName(), con);
							}
							// to remove this if loop
							if (productionId == null)
							{
								productionId = Long.valueOf(id.toString());
							}
							// end of to remove
							setId.invoke(newobj, productionId);
							newSet.add(newobj);
						}

					}
					Method setterForAssociation = mainObjectClass.getMethod(getFunctionName(
							migration.getRoleName(), "set"), Collection.class);
					setterForAssociation.invoke(mainObj, newSet);
				}

			}
			else if (cardinality != null && cardinality.equals("1") && cardinality != "")
			{
				Object associatedObject = getterForAssociation.invoke(mainObj, null);
				Object newObj = associatedClass.newInstance();

				if (associatedObject != null)
				{
					if (isToSetNull.equalsIgnoreCase("Yes"))
					{
						newObj = null;
					}
					else
					{
						//setting the production id to the same object
						Method getId = associatedObject.getClass().getMethod("getId", null);
						Object id = getId.invoke(associatedObject, null);
						Method setId = associatedClass.getMethod("setId", Long.class);

						if (id != null)
						{
							productionId = getProductionId(Long.valueOf(id.toString()), associatedObject.getClass().getName()
									, con);
						}
						// to remove this if loop
						if (productionId == null)
						{
							productionId = Long.valueOf(id.toString());
						}
						// end of to remove
						//setting the production id to the new object
						setId.invoke(newObj, productionId);
					}
					Method setMethod = null;
					try
					{
						setMethod = mainObjectClass.getMethod(getFunctionName(migration
								.getRoleName(), "set"), associatedClass);
					}
					catch (NoSuchMethodException e)
					{
						setMethod = mainObjectClass.getMethod(getFunctionName(migration
								.getRoleName(), "set"), associatedClass.getSuperclass());
					}
					setMethod.invoke(mainObj, newObj);
				}
			}
		}
	}

	private String getFunctionName(String name, String purpose)
	{
		String functionName = null;
		if (name != null && name.length() > 0)
		{
			String firstAlphabet = name.substring(0, 1);
			String upperCaseFirstAlphabet = firstAlphabet.toUpperCase();
			String remainingString = name.substring(1);
			if (purpose.equalsIgnoreCase("get"))
				functionName = "get" + upperCaseFirstAlphabet + remainingString;
			else if (purpose.equalsIgnoreCase("set"))
				functionName = "set" + upperCaseFirstAlphabet + remainingString;
		}
		return functionName;
	}

	private Long getProductionId(Long oldId, String className, Connection con)
	{
		Long newId = null;
		try
		{
			String sqlQuery = "select new_id from catissue_migration_mapping where object_className='"
					+ className + "' and old_id=" + oldId;
			Statement st = con.createStatement();
			ResultSet result = st.executeQuery(sqlQuery);
			if (result != null && result.next())
			{
				newId = result.getLong(1);
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return newId;
	}
}
