/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue-migration-tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.testcase;

import edu.wustl.common.beans.SessionDataBean;
/**
 * Utility class.
 * @author sachin_lale
 *
 */
public class CaTissueSuiteTestUtil
{
	/**
	 * Default constructor.
	 */
	public CaTissueSuiteTestUtil()
	{

	}
	/**
	 * JNDI name for catissuecore.
	 */
	public static final String CATISSUE_DATASOURCE_JNDI_NAME = "java:/catissuecore";
	/**
	 * JNDI name for dynamic extension.
	 */
	public static final String DE_DATASOURCE_JNDI_NAME = "java:/dynamicextensions";
	/**
	 * JNDI name for transaction manager.
	 */
	public static final String TX_MANAGER_DATASOURCE_JNDI_NAME = "java:/TransactionManager";
	/**
	 * JNDI name for session data bean after user logged in.
	 */
	public static SessionDataBean USER_SESSION_DATA_BEAN;
	/**
	 * JNDI name for context url.
	 */
	public static final String CONTEXT_URL = "http://localhost:8080/catissuecore";
}
