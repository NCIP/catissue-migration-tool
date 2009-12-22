package edu.wustl.migrator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;


public class SortObject implements Comparator<Object>
{


	public int compare(Object o1, Object o2)
	{
		try
		{
			Method getId = o1.getClass().getMethod("getId", null);
			Long id1 =(Long) getId.invoke(o1, null);
			Method getId2 = o2.getClass().getMethod("getId", null);
			Long id2 = (Long)getId2.invoke(o2, null);
			if(id1 < id2)
			{
				return -1;
			}
			else if(id1 > id2)
			{
				return 1;
			}
			else if(id1 == id2)
			{
				return 0;
			}
			
		}
		catch (SecurityException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (NoSuchMethodException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalAccessException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InvocationTargetException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
		return 0;
	}

}
