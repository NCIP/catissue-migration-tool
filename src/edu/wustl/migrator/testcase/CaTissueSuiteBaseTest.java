/*L
 *  Copyright Washington University in St. Louis
 *  Copyright SemanticBits
 *  Copyright Persistent Systems
 *  Copyright Krishagni
 *
 *  Distributed under the OSI-approved BSD 3-Clause License.
 *  See http://ncip.github.com/catissue_migration_tool/LICENSE.txt for details.
 */

package edu.wustl.migrator.testcase;

import org.junit.Before;

import servletunit.HttpServletRequestSimulator;
import servletunit.struts.MockStrutsTestCase;
import edu.wustl.catissuecore.util.global.Constants;
/**
 * Base test class.
 * @author sachin_lale
 *
 */
public class CaTissueSuiteBaseTest extends MockStrutsTestCase
{
	/**
	 * Setup method called before each test case run.
	 * @throws Exception
	 */
	@Override
	@Before
	protected void setUp() throws Exception
	{
		super.setUp();
		setConfigFile("/WEB-INF/struts-config.xml");
		HttpServletRequestSimulator req = (HttpServletRequestSimulator)getRequest();
		req.setRequestURL(CaTissueSuiteTestUtil.CONTEXT_URL);
		/**
		 * Setting the session daat bean in http session becuase on each test case run
		 * the session is  lost
		 */
		if(CaTissueSuiteTestUtil.USER_SESSION_DATA_BEAN!=null)
		{
			getSession().setAttribute(Constants.SESSION_DATA,
					CaTissueSuiteTestUtil.USER_SESSION_DATA_BEAN);
		}
	}

}
