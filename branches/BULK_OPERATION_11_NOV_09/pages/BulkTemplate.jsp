<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<%@ taglib uri="/WEB-INF/struts-html.tld" prefix="html" %>
<%@ taglib uri="/WEB-INF/struts-logic.tld" prefix="logic" %>
<%@ taglib uri="/WEB-INF/struts-bean.tld" prefix="bean" %>
<%@ taglib uri="/WEB-INF/nlevelcombo.tld" prefix="ncombo" %>
<%@ page language="java" isELIgnored="false"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ page import="edu.wustl.bulkoperator.util.BulkOperationConstants"%>
<%@ include file="/pages/content/common/AutocompleterCommon.jsp" %>
<%@ page import="edu.wustl.common.util.global.ApplicationProperties" %>
<%@ page import="java.io.*"%>
<%@ page import="java.util.*"%>
<%@ page import="javax.servlet.*"%>
<script language="JavaScript" type="text/javascript" src="jss/javaScript.js"></script>
<LINK href="css/styleSheet.css" type="text/css" rel="stylesheet">
<link href="css/catissue_suite.css" rel="stylesheet" type="text/css" />

<script>
function onDownLoadTemplate()
{
	var dropdownName =	document.getElementById('dropdownName').value;
	if (dropdownName == null || dropdownName == 'undefined' || dropdownName == "")
	{
		alert("Incorrect Template Name.");
	}
	else
	{
		var action = "DownloadCSV.do?dropdownName=" + dropdownName;
		mywindow = window.open(action, "Download", "width=10,height=10");
		mywindow.moveTo(0,0);
	}
}

function onUploadClick()
{
	var dropdownName =	document.getElementById('dropdownName').value;
	if (dropdownName == null || dropdownName == 'undefined' || dropdownName == "")
	{
		alert("Please select a Template Name.");
	}
	else
	{
		var uploadFileName = document.getElementById('file').value;
		var fileNameArray = uploadFileName.split(".");
		var arraySize = fileNameArray.length;
		if(fileNameArray[arraySize -1] != 'csv')
		{
			alert("Please upload a comma seperated file (.csv).");
		}
		else
		{
			document.forms[0].submit();
			document.getElementById('file').value="";
		}
	}
}

function getCSVOutputReport()
{
	<%
		File file = (File)request.getSession().getAttribute("resultFile");
		if (file != null)
		{
			%>
				var action = "DownloadCSV.do?report=report";
				mywindow = window.open(action, "Download", "width=10,height=10");
				mywindow.moveTo(0,0);
			<%
		}
	%>
}
</script>
<table width="100%" border="0" cellpadding="3" cellspacing="0">
	<tr>
		<td colspan="4" align="left" height="10"><%@ include file="/pages/content/common/ActionErrors.jsp" %>
        </td>
	</tr>
	<html:form action="/FileUpload.do" method="post" enctype="multipart/form-data" target="bulkTemplate">
		<tr>
			<td width="1%" align="center" class="black_ar"><span class="blue_ar_b"></span></td>
            <td width="20%" align="left" class="black_ar"><b><bean:message key="bulk.download.template" /></b></td>
            <td width="25%" align="left" valign="middle" class="black_new">
				<autocomplete:AutoCompleteTag
					property="dropdownName"
					optionsList = "<%=request.getAttribute(BulkOperationConstants.BULK_OPERATION_LIST)%>"
					initialValue="<%=request.getAttribute("dropdownName")%>"
					onChange="onTypeChange(this)"
					readOnly="false"
					size="31"
					styleClass="black_ar"
					numberOfResults="2"
				/>
			</td>
			<td colspan="2" width="54%" class="black_ar"><span class="blue_ar_b" valign="baseline"></span>
				<html:button styleClass="blue_ar_b" onclick="onDownLoadTemplate()" accesskey="enter" property="">
								<bean:message key="bulk.button.download.template" />
					</html:button>
			</td>
		</tr>
		<tr>
			<td colspan="5" width="100%" height="20" align="center" class="black_ar"><span class="blue_ar_b"></span>
			</td>
		</tr>
		<tr>
			<td align="center" class="black_ar"><span class="blue_ar_b"></span>
			</td>
			<td align="left" class="black_ar"><b><bean:message key="bulk.upload.file" /></b>
			</td>
			<td align="left" valign="middle" width="20%">
				<input id="file" type="file" name="csvFile" value="Browse">
				</input>
			</td>
			<td align="left" valign="left" colspan="2">
				<html:button styleClass="blue_ar_b" onclick="onUploadClick()" accesskey="enter" property="">
						<bean:message key="bulk.button.upload" />
				</html:button>
			</td>
		</tr>
	</html:form>
</table>