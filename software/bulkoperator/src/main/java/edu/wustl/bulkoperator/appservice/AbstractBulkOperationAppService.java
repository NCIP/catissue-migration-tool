
package edu.wustl.bulkoperator.appservice;

import java.lang.reflect.Constructor;
import java.util.Map;

import edu.wustl.bulkoperator.csv.CsvReader;
import edu.wustl.bulkoperator.metadata.BulkOperation;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.beans.SessionDataBean;
import edu.wustl.common.exception.ErrorKey;

public abstract class AbstractBulkOperationAppService
{
	public AbstractBulkOperationAppService(boolean isAuthenticationRequired,
			String userName, String password) throws Exception {
		isAuthRequired = isAuthenticationRequired;
		initialize(userName, password);
	}

	protected transient boolean isAuthRequired = true;

	public boolean isAuthenticationRequired() {
		return isAuthRequired;
	}

	public static AbstractBulkOperationAppService getInstance(String migrationAppClassName, String userName) throws BulkOperationException {
		if (migrationAppClassName == null) {
			migrationAppClassName = BulkOperationConstants.CA_CORE_MIGRATION_APP_SERVICE;
		}
		
		AbstractBulkOperationAppService appService = null;
		
		try {
			Class migrationServiceTypeClass = Class.forName(migrationAppClassName);
			Class[] constructorParameters = new Class[3];
			constructorParameters[0] = boolean.class;
			constructorParameters[1] = String.class;
			constructorParameters[2] = String.class;
			Constructor constructor = migrationServiceTypeClass
					.getDeclaredConstructor(constructorParameters);
			appService = (AbstractBulkOperationAppService) constructor.newInstance(
					false, userName, null);
		} catch (Exception exp) {
			ErrorKey errorKey = ErrorKey.getErrorKey("bulk.invalid.username.password");
			throw new BulkOperationException(errorKey, exp, "");
		}
		return appService;
	}

	public Object insert(Object obj) throws Exception {
		return insertObject(obj);
	}

	public abstract Long insertDEObject(String entityGroupName,String entityName,final Map<String, Object> dataValue) throws Exception;


	public Object search(Object obj) throws Exception {
		return searchObject(obj);
	}

	public Object update(Object obj) throws Exception {
		return updateObject(obj);
	}

	public Long hookStaticDEObject(Object hookingInformationObject) throws Exception {
		return hookStaticDynExtObject(hookingInformationObject);
	}
	
	abstract public void initialize(String userName, String password) throws Exception;

	abstract public void authenticate(String userName, String password) throws BulkOperationException;
	
	abstract protected Object insertObject(Object obj) throws Exception;

	abstract public void deleteObject(Object obj) throws Exception;

	abstract protected Object updateObject(Object obj) throws Exception;

	abstract protected Object searchObject(Object obj) throws Exception;

	abstract protected Long hookStaticDynExtObject(Object hookingInformationObject) throws Exception;

	abstract public Long insertData(final String categoryName,final Map<String, Object> dataValue,Object hookInformationObject)
			throws Exception;

	abstract public Long insertData(BulkOperation bulkOperation, CsvReader csvReader); 

	abstract public void integrateFormDataWithStaticObject(SessionDataBean sessionDataBean, Long containerId, Long recordId, Map<String, String> formIntegratorMap ) throws Exception;
}
