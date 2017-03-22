/**
 * 
 */
package com.app.analytics.cf.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * @author id19868
 *
 *  This class is designed to make it easy to load up data from a flat file for use in
 *  Java
 *  
 *  I wrote this class a few years ago before I ran into Pandas with python.. which made basic flat file
 *  manipulation obsolete..
 *  
 *  But then I started using Hadoop and realized that Hadoop is just a bunch of distributed flat files
 *  
 *  So I thought, can I re-purpose this code for MapReduce??
 *  <p> 
 *  Usage:  
 * <ol>
 *  <BR><li> Create a basic java bean class with all of the variables you want to see from the flat file
 *  <BR><li> Specify their data types and use eclipse or netbeans to auto-generate the getters and setters (following java naming conventions)
 *  <BR><li> Have your class extend BaseJavaBean
 *  <ul>
 *  <BR> 		<li> You will have to implement 2 methods
 *  <BR>  		<li> (getKeyFields), this is a convenience method to help you set the key for the Mapper class
 *  <BR>  		<li> (getClassName), this is needed so that the java reflection code will work correctly (You MUST implement this)
 *  <BR>       EXAMPLE: protected String getClassName() {return this.getClassName();}  // EASY!!
 *  </ul>
 *  <BR><li> Create a constructor that does at least 1 thing, potentially more
 *  <ul>
 *  <BR>		<li> set the Header record (VERY important, make sure this is in the correct order for the auto-loading to occurr)
 *  <BR>           Most files being used in HDFS do not have headers.  The header names should line up with your fields you added
 *  <BR>           in step 1!!!
 *  <BR>        <li> Set the delimiter, the default is a comma, but in the case of Hive it is "\u00001"
 *  <BR>  		<li> Add any additional date formats needed.  There are about 6 different formats right now, so add any additional ones needed
 *  </ul>
 *  <BR><li> Write your Mapper/Reducer code.
 *  <ul>  
 *  <BR>		<li>  Load the record using the "loadRecord" method, optionally you can pass in the record number,
 *  <BR>        <li>  Now access your data through getters!  done!
 *  </ul>
 *  </ol>
 */
public abstract class BaseJavaBean
{
	// only set as true when the first record of the file is the header
	protected boolean useHeader = false;
    protected String[] loadFields;
    
	protected Map<String,Integer> beanOrder = new HashMap<String,Integer>();
	protected Map<Integer,String> orderBean = new HashMap<Integer,String>();
	protected List<String> fieldList = new ArrayList<String>();
	protected Set<String> skipList = new HashSet<String>();
	protected Integer numLastUsed = 0;
	protected boolean matched;
	protected String delimiter = ",";
	
	protected enum msgType { ERR, INFO };
	
	protected Map<msgType,List<String>> msgs = new HashMap<msgType,List<String>>();	
	protected boolean suppressMessages = true;
	
	protected String fullRecord;
	
	protected String outDelimiter = this.delimiter;
	
	protected List<String> dateFormats = new ArrayList<String>();
	
	protected Map<String, Class<?>> classMap = new HashMap<String, Class<?>>();
	
	public boolean isMatched() {
		return matched;
	}

	public void setMatched(boolean matched) {
		this.matched = matched;
	}

	//2013-01-01-06.00.00.000000
	protected static final String DATE_STRING1 = "yyyy-MM-dd-hh.mm.ss.SSSSSS";
	//1/1/2013 15:20:05
	protected static final String DATE_STRING2 = "MM/dd/yyyy HH:mm:ss";
	//11/2/2010
	protected static final String DATE_STRING3 = "MM/dd/yyyy";
	//20130709
	protected static final String DATE_STRING4 = "yyyyMMdd";
	//May 18, 2014
	protected static final String DATE_STRING5 = "MMM dd, yyyy";
	//26-May-14
	protected static final String DATE_STRING6 = "dd-MMM-yy";
	
	//output format
	protected String dateOutput = DATE_STRING3;
	
	private static String[] parseDateFormats;
	// add date formats to this static block to ensure acceptance
	static
	{
		parseDateFormats =  new String[]
		{
			DATE_STRING1,
			DATE_STRING2,
			DATE_STRING3,
			DATE_STRING4,
			DATE_STRING5,
			DATE_STRING6
		};
	}
	
	
	/**
	 * 
	 * @return the names of the javabean variables pandas equivalent = DataFrame.columns
	 */
	public Collection<String> getBeanNames() {
		return getNames();
	}
	
	abstract public String getDefaultDelimiter();
	/**
	 * Base constructor
	 */
	public BaseJavaBean() {
		super();
		init(getDefaultDelimiter());
	}
	
	/**
	 * Constructor with header record passed in
	 * @param headerRec
	 */
	public BaseJavaBean(String headerRec, String delimiter)
	{
		super();
		String[] vals = splitCsvString(headerRec);
		init(vals,delimiter);
	}
	
	/**
	 * Constructor with header records passed in as a list
	 * @param headers
	 */
	public BaseJavaBean(String delimiter)
	{
		super();
		init(delimiter);
	}
	
	public static <T extends BaseJavaBean> Optional<T> getInstance(Class<T> clzz)
	{
		Optional<T> instance = Optional.empty();
		try {
			instance = Optional.of(clzz.getConstructor().newInstance());
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return instance;
	}
	
	public static <T extends BaseJavaBean> String getDelimiter(Class<T> clzz)
	{
		Optional<T> opt = BaseJavaBean.getInstance(clzz);
		return opt.isPresent() ? opt.get().getDefaultDelimiter():",";
	}
	
	private void init(String delimiter)
	{
		init(this.getLoadFields(),delimiter);
	}
	private void init(String [] headers, String delimiter)
	{
		if (headers != null && headers.length > 0)
		{
			for (String val: headers)
			{
				addName(val);
			}
		}
		this.setDelimiter(delimiter);
		this.outDelimiter = this.delimiter;
		loadDateFormats();
		
		msgs.put(msgType.ERR, new ArrayList<String>());
		msgs.put(msgType.INFO, new ArrayList<String>());
		this.loadFields = this.getLoadFields();
	}
	
	/**
	 * This method takes the pre-built date formats and passes them into the instance data formats
	 * later you can add additional formats
	 */
	public void loadDateFormats()
	{
		for (String format: parseDateFormats)
		{
			this.dateFormats.add(format);
		}
	}
	
	/**
	 * Add additional data formats!!! Like I just said yo!
	 * @param format
	 */
	public void addDateFormat(String format)
	{
		this.dateFormats.add(format);
	}
	
	/**
	 * How do you want dates to be printed out... this is helpful if a date field is part of the key
	 * @param dateFormat
	 */
	public void setDateOutput(String dateFormat)
	{
		this.dateOutput = dateFormat;
	}
	
	public void addErrMessage(String msg)
	{
		this.msgs.get(msgType.ERR).add(msg);
		if (!suppressMessages)
		{
			System.err.println(msg);
		}
	}
	
	public void addInfoMessage(String msg)
	{
		this.msgs.get(msgType.INFO).add(msg);
		if (!suppressMessages)
		{
			System.out.println(msg);
		}
	}
	
	public List<String> getErrMsgs()
	{
		return msgs.get(msgType.ERR);
	}
	
	public List<String> getInfoMsgs()
	{
		return msgs.get(msgType.INFO);
	}
	
    /**
     * 
     * @return Get the delimitter to use on this file
     */
	public String getDelimiter(){
		return this.delimiter;
	}
	
	/**
	 * Set the delimitter, if you don't call this method the default will be a comma
	 * @param argDelim
	 */
	public void setDelimiter(String argDelim){
		this.delimiter = argDelim;
	}
	
	/**
	 * This is just in case you want to use a different delimitter,
	 * If you don't set it.. then the output is the same as the one called from {@link #getDelimiter()}
	 * @param argDelim
	 */
	public void setOutDelimiter(String argDelim){
		this.outDelimiter = argDelim;
	}
	
	/**
	 * If {@link #outDelimiter} is null, then return {@link #getDelimiter()}
	 * @return
	 */
	public String getOutDelimiter(){
		if (this.outDelimiter == null)
		{
			return getDelimiter();
		}
		return this.outDelimiter;
	}
	
	/**
	 * You may want access to the original record to write it out.. like this
	 * Write (key, full record)
	 * @return
	 */
	public String getFullRecord() {
		return fullRecord;
	}
	
	/**
	 * This will get you the full record and then allow you to put the qualifier in front..
	 * AND... You can load it from this method.
	 * @return
	 */
	public String getIdentifierRecord()
	{
		return this.getJavaBeanClass().getName()+"~"+this.getFullRecord();
	}
	
	public static <T extends BaseJavaBean> T loadJavaBeanRecord_noExcp(String delimiter, String identRecord)
	{
		try {
			return loadJavaBeanRecord(delimiter, identRecord);
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return null;
	}
	
	public static <T extends BaseJavaBean> T loadJavaBeanRecord(String delimiter, String identRecord) throws Exception
	{
		String[] parts = identRecord.split("~");
		String klazz = parts[0];
		String record = parts[1];
		

		@SuppressWarnings("unchecked")
		Class<T> clz = (Class<T>) Class.forName(klazz);
			
		T recT = clz.getConstructor(String.class,String.class).newInstance(delimiter,record);
		
		return recT;
	}
	
	public static <T extends BaseJavaBean> T loadJavaBeanRecord(String delimiter, String identRecord, Class<T> type) throws Exception
	{
		String[] parts = identRecord.split("~");
		String klazz = parts[0];
		String record = parts[1];
		
		Class<?> clz = Class.forName(klazz);
			
		Object recT = clz.getConstructor(String.class,String.class).newInstance(delimiter,record);
		
		return type.cast(recT);
	}

	/**
	 * This is called from {@link #loadRecord(String, Integer)}, so no need to call it yourself
	 * @param recList
	 */
	public void setFullRecord(String recList) {
		this.fullRecord = recList;
	}

	/**
	 * this uses {@link JavaBeanUtil} to print a pretty formatted class string
	 * - no more stringbuffer crap!
	 * (IT ROCKS!)
	 */
	@Override
	public String toString() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JavaBeanUtil.printJavaZBean(Arrays.asList(new BaseJavaBean[]{this}), baos);
		String ret = new String(baos.toByteArray());
		return ret;
	}
	
	/**
	 * Add a java bean name to the stack, with no number supplied it gets the next one available
	 * @param name
	 */
	protected void addName(String name)
	{
		addName(name,++numLastUsed,false);
	}
	
	/**
	 * Add a java bean name, that shouldn't be parsed.  Maybe it is a problematic field or something
	 * @param name
	 * @param skip
	 */
	protected void addName(String name, boolean skip)
	{
		addName(name,++numLastUsed,skip);
	}
	
	/**
	 * 
	 * @param name
	 * @param order
	 * 
	 * order is where it should be loaded.. if it already exists it moves <BR>
	 * the rest of the names up.
	 */
	protected void addName(String name, Integer order)
	{
		addName(name,order,false);
	}
	
	/**
	 * This adds the name passed in to the stack in the position that is specified.
	 * 
	 * It will change the order if a field had already been specified for the order provided.
	 * <BR>EXAMPLE if "name" is in slot #1 and then "age" wants to be slot #1, then move "name" to slot #2
	 * @param name
	 * @param order
	 * @param skip boolean value for if we should skip this record in parsing
	 */
	protected void addName(String name, Integer order, boolean skip)
	{
		if(order != null)
		{
			if(orderBean.containsKey(order))
			{
				String newName = name;
				String oldName = orderBean.get(order);
				orderBean.put(order,newName);
				beanOrder.put(newName, order);
				for(int i=(order+1);orderBean.containsKey(i);i++)
				{
					newName = oldName;
					oldName = orderBean.get(i);
					orderBean.put(i, newName);
					beanOrder.put(newName, i);
					numLastUsed = i;
				}
				addName(oldName);
			}
			else
			{
				numLastUsed = order;
				beanOrder.put(name, order);
				orderBean.put(order, name);
			}
		}
		if(skip)
		{
			skipList.add(name);
		}
		fieldList.add(name);
	}
	
	/**
	 * Force the classes that inherit from this to define the class name
	 * (helps with Java Reflection)
	 * @return
	 */
	protected abstract <T extends BaseJavaBean> Class<T> getJavaBeanClass();
	
	/**
	 * If the instantiating class provides which fields make up the key, the base class will handle the rest
	 * @return
	 */
	protected abstract String[] getKeyFields();
	//protected abstract String getFileName(boolean useTest);
	
	
	/**
	 * These are the fields that need to be loaded from the incoming flat file
	 * @return
	 */
	protected abstract String[] getLoadFields();
	
	/**
	 * Like I said.. taking care of creating the key string
	 * Essentially using the {@link #outDelimiter} as field separator
	 * @return
	 */
	public String createKey()
	{
		String[] fields = this.getKeyFields();
		return this.createKey(fields);
	}
	
	public String createKey(String [] fields)
	{
		StringBuffer sb = new StringBuffer();
		for (int i=0;i<fields.length;i++){
			String val = this.getStringValue(fields[i]);
			sb.append(val);
			if ((i+1) < fields.length) { // Dude.. don't put a delimiter on the end!!!
			    sb.append(this.getOutDelimiter());	
			}
		}
		return sb.toString();
	}
	
	/**
	 * Convenience method to load record when you don't care which record number it is
	 * @param record
	 */
	public void loadRecord(String record)
	{
		loadRecord(record,0);
	}
	
	/**
	 * Method to load a record and thus populate the java bean variables
	 * (if you need to debug which record is causing you problems.. you best pass in the recNumber)
	 * 
	 * This will split the record stirng into an array of String values
	 * @param record
	 * @param recNumber
	 */
	public void loadRecord(String record, Integer recNumber)
	{
		Collections.reverse(this.dateFormats); // ensure any added date formats are first
		setFullRecord(record);
		String[] vals;
		try 
		{
			vals = splitCsvString(record);
		} 
		catch (RuntimeException e) 
		{
			this.addErrMessage("Problem on record: "+recNumber.toString());
			throw e;
		}
		loadFromList(vals,recNumber);
	}
	
	/**
	 * Take the array of String values and start using some reflection to load the java bean
	 * @param values
	 * @param recNumber
	 */
	protected void loadFromList(String[] values, Integer recNumber)
	{
		Integer fieldNumber = null; 
		try
		{
			if(null != values && values.length>0)
			{
				for(int i=0;i<values.length;i++)
				{
					int j=i+1;
					fieldNumber = j;
					String name = orderBean.get(j);
					if(!skipList.contains(name))
					{
						String inValue = values[i];
						setJavaBeanField(name, inValue);
					}
				}
			}
		}
		catch (Throwable t)
		{
			this.addErrMessage("Problem processing rec: "+recNumber.toString());
			this.addErrMessage("Problem processing field: "+fieldNumber.toString());
			throw new RuntimeException(t);
		}
	}

	/**
	 * Look up the setter method name, then call it using reflection with the Object value (which has already been coverted)
	 * @param name
	 * @param inValue
	 */
	public void setJavaBeanField(String name, String inValue) 
	{
		Object val = convertValue(inValue,name);
		String setter = getSetterName(name);
		setValueOfBean(setter, val);
	}
	
	/**
	 * Convenience method to check of the case of a null field or an empty field
	 * (added \N as a check, since that is how Hive outputs null values)
	 * @param value
	 * @return
	 */
	private boolean valueIsNull(String value)
	{
	    if (value == null)
	    {
	    	return true;
	    }
	    String val = value.trim();
	    if (val.equalsIgnoreCase(""))
	    {
	    	return true;
	    }
	    val = value.replace("\\N","");
	    val = val.trim();
	    if (val.equalsIgnoreCase(""))
	    {
	    	return true;
	    }
		return false;
	}
	
	/**
	 * This is where the rubber hits the road... using a String name and a String value
	 * Convert the string to the correct datatype and call the setter method on the java bean
	 * 
	 * <BR> Valid Data Types: Integer, Double, Date, Boolean, String
	 * 
	 * @param value
	 * @param name
	 * @return converted Object
	 */
	private Object convertValue(String value, String name) 
	{
		Object retObj = value;
		if (!valueIsNull(value))
		{
			try {
				Class<?> clzz = getClassForName(name);
				
				if(clzz == Integer.class || clzz == Double.class)
				{
					boolean percent = false;
					boolean negative = false;
					if(value.startsWith("(") && value.endsWith(")"))
					{
						value = value.substring(1);
						value = value.substring(0, value.length()-1);
						negative = true;
					}
					if(value.startsWith("$"))
					{
						value = value.substring(1);
					}					
					if(value.endsWith("%"))
					{
						value = value.substring(0,value.length()-1);
						percent = true;
					}
					value = value.replaceAll(",", "");
					Double dbl = Double.parseDouble(value);
					if(percent)
					{
						dbl = dbl/100;
					}
					if(negative)
					{
						dbl = dbl * -1;
					}
					retObj = dbl;
					if(clzz == Integer.class)
					{
						retObj = dbl.intValue();
					}
				}
				if(clzz == Date.class)
				{
					retObj = parseDate(value);
				}
				if(clzz == Boolean.class)
				{
					retObj = this.parseBoolean(value);
				}
			} catch (Exception e) {
				// Do Nothing
				this.addErrMessage("Problem with value: "+value+" On field "+name);
			}
		}
		return retObj;
	}

	/**
	 * Convert a "1" or "t/T" to True
	 * or if it is longer than that use the {@link Boolean#parseBoolean(String)}</pre>
	 * @param argInput
	 * @return
	 */
	private Boolean parseBoolean(String argInput)
	{
		if (argInput == null)
		{
			return false;
		}
		if(argInput.trim().length() == 1)
		{
			String newVal = argInput.trim();
			return newVal.equalsIgnoreCase("1") || newVal.equalsIgnoreCase("t");
		}
		return Boolean.parseBoolean(argInput);
	}
	
/// ======== OLD INEFFECIENT METHOD ===========
//	private Object parseDate(String value) throws ParseException 
//	{
//		SimpleDateFormat fmt = new SimpleDateFormat(DATE_STRING1);
//		Date date = null;
//		try
//		{
//			date = fmt.parse(value);
//		}
//		catch(Throwable t)
//		{
//			//Now try string 2
//			fmt = new SimpleDateFormat(DATE_STRING2);
//			date = fmt.parse(value);
//		}
//		return date;
//	}
/// ======== OLD INEFFECIENT METHOD ===========
	
	/**
	 * attempt to convert the String to a Date format using one of the existin Date Formats
	 * @param value
	 * @return
	 * @throws ParseException
	 */
	public Object parseDate(String value) throws ParseException
	{		
		return parseDate(value,null,this.dateFormats.toArray(new String[0]));
	}
	
	/**
	 * Recursively go through all possible date formats until a good one is found
	 * 
	 * 1/23/2017:  Updated to allow for the class that is extending this to add additional
	 *             Date formats.
	 */
	private Object parseDate(String value, Integer sub, String[] formats) throws ParseException
	{
		if(sub == null)
		{
			sub = 0;
		}
		if(sub < formats.length)
		{
			SimpleDateFormat fmt = new SimpleDateFormat(formats[sub]);
			try
			{
				return fmt.parse(value);
			}
			catch(Throwable t)
			{
				return parseDate(value,++sub,formats);
			}
		}
		
		throw new ParseException("Unparseable Date --> "+value,0);
	}

	/**
	 * Method to return the bean variable names
	 * @return
	 */
	protected Collection<String> getNames()
	{
		return beanOrder.entrySet().stream()
				.sorted(Map.Entry.<String,Integer>comparingByValue())
				.map(Map.Entry<String,Integer>::getKey).collect(Collectors.toList());
	}
	
	
	/**
	 * Get the value from the bean for variable: name
	 * (essentially calls the "getter" method using Reflection after looking it up)
	 * 
	 * @param name
	 * @return
	 */
	public Object getValue(String name)
	{
		return getValueFromBean(getGetterName(name));
	}
	
    public Integer getIntValue(String name)
    {
    	return (Integer) getValueFromBean(getGetterName(name));
    }
    
    public Double getDblValue(String name)
    {
    	return (Double) getValueFromBean(getGetterName(name));
    }
	
    public Boolean getBoolValue(String name)
    {
    	return (Boolean) getValueFromBean(getGetterName(name));
    }
    
    public Date getDateValue(String name)
    {
    	return (Date) getValueFromBean(getGetterName(name));
    }
	
	/**
	 * Get the String value of the variable in the Java Bean
	 * 
	 * @param name
	 * @return The string representation of the java bean variable
	 */
	public String getStringValue(String name)
	{		
		Class<?> clzz = classMap.getOrDefault(name, this.getClassForName_noExcp(name));
		if(clzz == Date.class)
		{
			return this.getDateString(name);
		}
		Object val = this.getValue(name);
		return val == null? "":val.toString();
	}
	
	/**
	 * Wrapper method so you don't have to deal with the Exception
	 * 
	 * Only use this after the initial load
	 * @param name
	 * @return
	 */
	public Class<?> getClassForName_noExcp(String name)
	{
		try
		{
			return getClassForName(name);
		}
		catch (Exception e)
		{
			// purposely not handling the exception
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Convenience method to look up the class of the variable and store it in the class map
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public Class<?> getClassForName(String name) throws Exception
	{
		Class <?> clzz = null;
		if (this.classMap.containsKey(name))
		{
			return this.classMap.get(name);
		}
		else
		{
			try {

				Class<BaseJavaBean> baseClass = this.getJavaBeanClass();
				Field field = baseClass.getDeclaredField(name);
				clzz = field.getType();
				this.classMap.put(name, clzz);
			} catch (NoSuchFieldException e) {
				throw new Exception("No field exists by that name: "+name, e);
			} catch (SecurityException e) {
				throw new Exception("Security Error: No access granted to look up field: "+name, e);
			}
			return clzz;
		}		
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public String getDateString(String name)
	{
		return getDateString(name, this.dateOutput);
	}
	
	/**
	 * Call this to convert a field just by it's name
	 * 
	 * @param name of the field that is to be converted to a Date String
	 * @param dateFormat the format to show the date
	 * @return the String representation of the date
	 */
	public String getDateString(String name, String dateFormat)
	{
		Object val = this.getValue(name);
		return this.getDateString(val, dateFormat);
	}
	
	/**
	 * Will use the default date format of mm/dd/yyyy unless it is set by the implementer
	 * @param val of type object that will be cast to Date
	 * @return the String representation of the date
	 */
	public String getDateString(Object val)
	{
		return getDateString(val, this.dateOutput);
	}
	
	/**
	 * 
	 * @param val of type object that will be cast to Date
	 * @param dateFormat the format to shape the date
	 * @return the String representation of the date
	 */
	public String getDateString(Object val, String dateFormat)
	{
		if (val == null)
		{
			return "null";
		}
		Date dateVal = (Date)val;
		SimpleDateFormat format = new SimpleDateFormat(dateFormat);
		return format.format(dateVal);
		
	}
	
	/**
	 * Pass in the getter Method name and call it using Reflection
	 * 
	 * @param getter
	 * @return
	 */
	protected Object getValueFromBean(String getter) 
	{
		Object val = null;
		try
		{
			Method meth = this.getClass().getMethod(getter, new Class<?>[0]);
			val = meth.invoke(this, new Object[0]);
		}
		catch(Throwable t)
		{
			//
		}
		return val;
	}
	
	/**
	 * Set the value of the variable with the given object value and the setter method name
	 * @param setter
	 * @param value
	 */
	protected void setValueOfBean(String setter, Object value)
	{
		try
		{
			Method meth = this.getClass().getMethod(setter, new Class<?>[]{value.getClass()});
			meth.invoke(this, new Object[]{value});
		}
		catch(Throwable t)
		{
			//t.printStackTrace();
		}
	}

	/**
	 * Using the name of the variable pass back the getter method name
	 * @param beanName
	 * @return
	 */
	private String getGetterName(String beanName) 
	{
		String name = "get" + beanName.substring(0, 1).toUpperCase() + beanName.substring(1);
		try {
			Class<?> clzz = this.getClassForName(beanName);
			if(clzz.equals(boolean.class))
			{
				name = "is" + beanName.substring(0, 1).toUpperCase() + beanName.substring(1);
			}
			
		} catch (Exception e) {
			//Do Nothing
		} 
		
		
		return name;
	}
	
	/** 
	 * Using the variable name pass the setter method name
	 * @param beanName
	 * @return
	 */
	private String getSetterName(String beanName) 
	{
		if(beanName == null)
		{
			this.addInfoMessage("WHAT????");
		}
		String name = "set" + beanName.substring(0, 1).toUpperCase() + beanName.substring(1);
		return name;
	}
	
	/**
	 * return a list of all variable names from the bean
	 * @return
	 */
	public String[] allItems()
	{
		Collection<String> names = this.getBeanNames();
		return names.toArray(new String[0]);
	}
	
	/**
	 * convert a list of BaseJavaBean objects to a csv file
	 * 
	 * @param bns
	 * @param fileName
	 * @throws IOException
	 */
	public void createCSVFile(BaseJavaBean[] bns, String fileName) throws IOException
	{
		PrintWriter writer = null;

		try {
		    writer = new PrintWriter(fileName, "utf-8");
		    writer.println(bns[0].createCSVHeader());
		    for(BaseJavaBean bn:bns)
		    {
		    	writer.println(bn.createCSVRecord());
		    }
		} catch (IOException ex){
		  // report
			ex.printStackTrace();
			throw ex;
		} finally {
		   try {writer.close();} catch (Exception ex) {}
		}
	}
	
	/**
	 * create one record for the csv file
	 * @return
	 */
	public String createCSVRecord()
	{
		return createCSV(false, true);
	}
	
	public String createHDFSRecord(boolean includeSkipFields, String outputDelim)
	{
		this.setOutDelimiter(outputDelim);
		return this.createHDFSRecord(includeSkipFields);
	}
	
	public String createHDFSRecord(boolean includeSkipFields)
	{
		List<String> list = this.getFields(includeSkipFields);
		return createCSV(false,false,list);
	}
	
	public List<String> getFields(boolean includeSkipFields)
	{
		List<String> list = this.fieldList;
		if (!includeSkipFields)
		{
			list = new ArrayList<String>();
			for (String field: fieldList)
			{
				if (!skipList.contains(field))
				{
					list.add(field);
				}
			}
		}
		return list;
	}
	
	/**
	 * create one record for the csv file
	 * @return
	 */
	public String createCSVRecord(boolean lineSeparator)
	{
		return createCSV(false, lineSeparator);
	}
	
	/**
	 * create the header csv record
	 * @return
	 */
	public String createCSVHeader()
	{
		return createCSV(true, true);
	}
	
	public String createCSV(boolean header, boolean lineSeperator)
	{
		return this.createCSV(header, lineSeperator, this.fieldList);
	}
	/**
	 * Convert the fields to a record using the Out Delimitter
	 * @param header
	 * @return
	 */
	public String createCSV(boolean header, boolean lineSeperator, List<String> fields)
	{
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for(String field:fields)
		{
			if(!first)
			{
				sb.append(this.getOutDelimiter());
			}
			first = false;
			String value = "";
			if(header)
			{
				value = field;
			}
			else
			{
				value = getStringValue(field);
				if("true".equalsIgnoreCase(value))
				{
					value = "1";
				}
				if("false".equalsIgnoreCase(value))
				{
					value = "0";
				}
				if(value.indexOf(String.valueOf(this.getOutDelimiter()))>0)
				{
					value = "\""+value+"\"";
				}
			}
			sb.append(value);
		}
		
		if (lineSeperator)
		{
			sb.append(System.getProperty("line.separator"));
		}
		
		return sb.toString();
	}
	
	/**
	 * This will split the String record into fields using the {@link #delimiter}
	 * @param inputLine
	 * @return
	 */
	public String[] splitCsvString(String inputLine)
	{
		if(inputLine.trim().equalsIgnoreCase(""))
		{
			return new String[0];
		}
		String newLine = searchOutAndRemoveCommas(inputLine);
		String[] out = newLine.split(this.getDelimiter());
		List<String> outList = new ArrayList<String>();
		
		for(String input:out)
		{
			outList.add(removeQuotes(input,this.getDelimiter()));
		}
		
		return outList.toArray(new String[0]);
	}
	
	/*         THIS IS THE FIELD THAT WILL BE PUT INTO PLACE OF THE DELIMITER     */
	protected static final String REPLACE_COMMA = "-COMMA-";
	//protected static final Character COMMA = ',';
	protected static final Character QUOTE = '"';
	protected static final Character QUOTE_S = '\'';
	
	/**
	 * We are attempting to find embedded delimmiters in the file, this could happen if the value is "Bowles, Jason"
	 * (The quotes in around this field signals that we are to ingnore the comma in between!)
	 * 
	 * @param inputLine
	 * @return
	 */
	protected String searchOutAndRemoveCommas(String inputLine) 
	{
		String delim = this.getDelimiter();
		if(inputLine.indexOf(String.valueOf(QUOTE))>0)
		{
			StringBuffer sb = new StringBuffer();
			char[] chars = inputLine.toCharArray();
			boolean doubleQuoteFnd = false;
			for(int i=0;i<chars.length;i++)
			{
				char c = chars[i];
				if(c == QUOTE)
				{
					if(doubleQuoteFnd)
					{
						doubleQuoteFnd = false;
					}
					else
					{
						doubleQuoteFnd = true;
					}
					sb.append(c);
				}
				else if(c == delim.charAt(0) && doubleQuoteFnd)
				{
					sb.append(REPLACE_COMMA);
				}
				else
				{
					sb.append(c);
				}
				
			}
			return sb.toString();
		}
		return inputLine;
	}

	/**
	 * Remove quotes using just a delimitter of a comma
	 * @param input
	 * @return
	 */
	protected static String removeQuotes(String input)
	{
		return removeQuotes(input, ",");
	}
	
	/**
	 *  remove quotes using a supplied delimitter
	 * 
	 * @param input
	 * @param delimitter
	 * @return
	 */
	protected static String removeQuotes(String input, String delimitter)
	{
		String output = input;
		if(input != null && input.trim().length()>0)
		{
			if(input.startsWith(String.valueOf(QUOTE)) && input.endsWith(String.valueOf(QUOTE)))
			{
				output = input.substring(1, input.length()-1);
			}
			if(input.startsWith(String.valueOf(QUOTE_S)) && input.endsWith(String.valueOf(QUOTE_S)))
			{
				output = input.substring(1, input.length()-1);
			}
			output = output.replaceAll(REPLACE_COMMA, delimitter);
		}
		
		return output;
	}
	
	/**
	 * String matching across all fields
	 * 
	 * @param that
	 * @return
	 */
	protected boolean match(BaseJavaBean that)
	{
		String[] thisItems = this.allItems();
		return match(that,thisItems);
	}
	
	/**
	 * String matching across fields provided
	 * 
	 * @param that
	 * @param names
	 * @return
	 */
	protected boolean match(BaseJavaBean that, String[] names)
	{
		Boolean match = true;
		for (String name: names)
		{
		    if (!this.doesMatch(that, name))
		    {
		    	match = false;
		    	break;
		    }
		}
		return match;
	}
	
	/**
	 *  Field matching... override this if you don't want to do String matching
	 * @param that
	 * @param fieldName
	 * @return
	 */
	protected boolean doesMatch(BaseJavaBean that, String fieldName)
	{
		String thisStrVal = this.getStringValue(fieldName);
	    String thatStrVal = that.getStringValue(fieldName);
	    if (!thisStrVal.equalsIgnoreCase(thatStrVal))
	    {
	    	return false;
	    }
	    return true;
	    
	}
	
	/**
	 * Example:  months = inDate - thisDate
	 * 
	 * @param thisDate  this is the 2nd date.. meaning.. the subtractor
	 * @param inDate this is the 1st date.. meaning.. the subtratee
	 * @return
	 */
	public Integer getMonthDiff(Date thisDate, Date inDate)
	{
		LocalDateTime thisDt = LocalDateTime.ofInstant(thisDate.toInstant(), ZoneId.systemDefault());
		LocalDateTime thatDt = LocalDateTime.ofInstant(inDate.toInstant(), ZoneId.systemDefault());
		
		long months = thisDt.until(thatDt, ChronoUnit.MONTHS);
		return Long.valueOf(months).intValue();
	}
	
	/**
	  * Example:  years = inDate - thisDate
	 * 
	 * @param thisDate  this is the 2nd date.. meaning.. the subtractor
	 * @param inDate this is the 1st date.. meaning.. the subtratee
	 * @return
	 */
	public Integer getYearDiff(Date thisDate, Date inDate)
	{
		return getMonthDiff(thisDate, inDate)/12;
	}
	
	
	public Date addYears(Date thisDate, Integer yrsToAdd)
	{
		LocalDateTime thisDt = LocalDateTime.ofInstant(thisDate.toInstant(), ZoneId.systemDefault());
		
		return Date.from(thisDt.plusYears(yrsToAdd.longValue()).atZone(ZoneId.systemDefault()).toInstant());
	}
	
	public <T extends BaseJavaBean> T[] toArray(List<T> list) {
		 @SuppressWarnings("unchecked")
		T[] var = (T[]) java.lang.reflect.Array.newInstance(this.getClass(), list.size());
	     return list.toArray(var);
	}
}
