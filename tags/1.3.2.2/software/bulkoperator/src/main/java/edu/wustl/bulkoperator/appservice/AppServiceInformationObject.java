
package edu.wustl.bulkoperator.appservice;

public class AppServiceInformationObject
{
	private String userName;
	private String password;
	private String serviceImplementorClassName;

	public final String getUserName()
	{
		return userName;
	}

	public final void setUserName(String userName)
	{
		this.userName = userName;
	}

	public final String getPassword()
	{
		return password;
	}

	public final void setPassword(String password)
	{
		this.password = password;
	}

	public final String getServiceImplementorClassName()
	{
		return serviceImplementorClassName;
	}

	public final void setServiceImplementorClassName(String serviceImplementorClassName)
	{
		this.serviceImplementorClassName = serviceImplementorClassName;
	}

}
