
package edu.wustl.bulkoperator.action;

import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.struts.Globals;
import org.apache.struts.action.ActionError;
import org.apache.struts.action.ActionErrors;
import org.apache.struts.action.ActionForm;
import org.apache.struts.action.ActionForward;
import org.apache.struts.action.ActionMapping;
import org.apache.struts.action.ActionMessage;
import org.apache.struts.action.ActionMessages;

import edu.wustl.bulkoperator.actionForm.BulkOperationForm;
import edu.wustl.bulkoperator.bizlogic.BulkOperationBizLogic;
import edu.wustl.bulkoperator.util.BulkOperationConstants;
import edu.wustl.bulkoperator.util.BulkOperationException;
import edu.wustl.common.action.SecureAction;
import edu.wustl.common.util.global.ApplicationProperties;

public class BulkUploadAction extends SecureAction {

	private final static Logger logger = Logger.getLogger(BulkUploadAction.class);

	protected ActionForward executeSecureAction(
			ActionMapping mapping, ActionForm form,
			HttpServletRequest request, HttpServletResponse response) 
	throws Exception {
		BulkOperationForm bulkOperationForm = (BulkOperationForm) form;
		String templateName = bulkOperationForm.getDropdownName();
		
		String forward = BulkOperationConstants.SUCCESS;
		BulkOperationBizLogic bulkOperationBizLogic = new BulkOperationBizLogic();
		try {
			InputStream csvFileIn = bulkOperationForm.getCsvFile().getInputStream();	
			Long jobId = bulkOperationBizLogic.submitJob(this.getSessionData(request), templateName, csvFileIn);

			ActionMessages msgs = new ActionMessages();
			ActionMessage msg = new ActionMessage("job.submitted");
			msgs.add(ActionErrors.GLOBAL_MESSAGE, msg);
			if (!msgs.isEmpty()) {
				saveMessages(request, msgs);
			}
			
			request.setAttribute("jobId", jobId.toString());
		} catch (BulkOperationException exp) {
			forward = BulkOperationConstants.FAILURE;
			ActionErrors errors = (ActionErrors) request.getAttribute(Globals.ERROR_KEY);
			if (errors == null)	{
				errors = new ActionErrors();
				if(templateName == null || "".equals(templateName))	{
					errors.add(ActionErrors.GLOBAL_ERROR, new ActionError("errors.item",exp.getMessage()));
				} else if(exp.getErrorKeyName().equals("bulk.operation.issues") ||
						exp.getErrorKeyName().equals("bulk.error.csv.column.name.change.validation")) {
						errors.add(ActionErrors.GLOBAL_ERROR, new ActionError("errors.item",
								ApplicationProperties.getValue("bulk.error.csv.column.name.change")));
				}
				else {
						errors.add(ActionErrors.GLOBAL_ERROR, new ActionError("errors.item",exp.getMessage()));
				}					
			}
			this.saveErrors(request, errors);
			logger.error(exp.getMessage(), exp);
		}

		return mapping.findForward(forward);
	}	
}