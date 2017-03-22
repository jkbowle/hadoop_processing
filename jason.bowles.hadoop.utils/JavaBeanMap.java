package jason.bowles.hadoop.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JavaBeanMap {
	  private Map<Class<?>, List<Object>> favorites = new HashMap<Class<?>, List<Object>>();

	  public Set<Class<?>> getKeys()
	  {
		  return favorites.keySet();
	  }
	  
	  public <T extends BaseJavaBean> void setJavaBean(Class<T> klass) {
		List<Object> clzzList = new ArrayList<Object>();
	    favorites.put(klass, clzzList);
	  }
	  
	  public <T extends BaseJavaBean> void addJavaBean(Class<T> klass, T thing)
	  {
		  favorites.get(klass).add(thing);
	  }
	  
	  public <T extends BaseJavaBean> List<T> getBaseJavaBeans(Class<T> klass) 
	  {
		List<Object> list = favorites.get(klass);
		List<T> retList = new ArrayList<T>();
		for (Object obj: list)
		{
			retList.add(klass.cast(obj));
		}
	    return retList;
	  }

	}
