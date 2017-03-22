package com.app.analytics.cf.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Example of implementing a BaseJavaBean
 * @author id19868
 * 
 * <p> Steps to implement the BaseJavaBean
 * <ol>
 * <li> Identify the fields (if the file has a header, use the fields names as specified in the header) Not the case in Hadoop
 * <li> create a field in your implementing class with the correct data type, Data Types Include:
 * <ul>
 * <li> Boolean
 * <li> String
 * <li> Integer
 * <li> Double
 * <li> Float
 * <li> Date
 * </ul>
 * <li> Create getters and setters according to Java standards
 * <br> <a href="http://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fref-dialog-gettersetter.htm">Eclipse Getters and Setters</a>
 * <li> Implement the 2 methods <ul><li>{@link BaseJavaBean#getClassName()}</b></li><li>{@link BaseJavaBean#getKeyFields()}</li></ul>
 * <li> Implement a Constructor Example Below (pass in the array of field names (in load order) and the delimiter to use)
 * <pre>
 * {@code
 * public ExampleBean() {
 *    super(new String[]{"field1","field2","field3"},",");
 *   }
 * }
 * </pre>
 * <li> Now load your data
 * <pre>
 * {@code
 * ExampleBean eb = new ExampleBean();
 * eb.loadRecord("keyField,1,10/12/2015");
 * System.out.println(eb.toString());
 * }
 * 
 * OUTPUT <BR>
 * +---------+-----------+-------+
 * |  field1 |   field3  | field2||
 * +---------+-----------+-------+
 * | keyField| 10/12/2015| 1     ||
 * +---------+-----------+-------+
 *
 * </pre>
 * </ol>
 * 
 * <BR>
 * Now with a collection of BaseJavaBean's using Java 8 Streaming API
 * <a href="http://www.oracle.com/technetwork/articles/java/ma14-java-se-8-streams-2177646.html">Processing Data with Java SE 8 Streams, Part 1</a>
 * 
 * You can do something like this to sum up "field2", using the getter method for {@link #field2}
 * <pre>
 * {@code
 * int sum = bns.stream().mapToInt(ExampleBean::getField2).sum();
 * System.out.println(sum);
 * }
 * </pre>
 * or use the generic method from BaseJavaBean, {@link #getIntValue(String)} with a lambda function
 * <pre>
 * {@code
 * sum = bns.stream().mapToInt(b -> b.getIntValue("field2")).sum();
 * System.out.println(sum);
 * }
 * </pre>
 *
 * <p>
 * All this is to say that using the class {@link BaseJavaBean} makes working with data that is stored in flat files all that much easier!
 */
public class ExampleBean extends BaseJavaBean {

	public String field1;
	public Integer field2;
	public Date field3;
	
	@Override
	@SuppressWarnings("unchecked")
	protected <T extends BaseJavaBean> Class<T> getJavaBeanClass() {
		return (Class<T>) ExampleBean.class;
	}

	@Override
	protected String[] getKeyFields() {
		return new String[]{"field1"};
	}
	
	@Override
	public String getDefaultDelimiter() {
		return ",";
	}
	
	/**
	 * Example constructor with minimal functionality
	 * 
	 * In the constructor you can add additional fields that will be calculated later
	 * {@link #addName(String, boolean)} <- specify true if you don't want it to be output later
	 */
	public ExampleBean() {
		super(",");
	}
	
	/**
	 * This is an example of how you can instantiate and load a java object all in the constructor using
	 * {@link #loadRecord(String)}
	 * @param record
	 */
	public ExampleBean(String record)
	{
		super(",");
		this.loadRecord(record);
	}
	
	/**
	 * 
	 * @param delimiter
	 * @param record
	 */
	public ExampleBean(String delimiter, String record)
	{
		super(delimiter);
		this.loadRecord(record);
	}

	public static void main(String[] args) {
		countExample();
	}
	
	/**
	 * Example code to show how use Java 8 streaming
	 */
	public static void countExample(){
		List<ExampleBean> bns = getList();
		
		int sum = bns.stream().mapToInt(ExampleBean::getField2).sum();
		System.out.println(sum);
		
		sum = bns.stream().mapToInt(b -> b.getIntValue("field2")).sum();
		System.out.println(sum);
	}
	
	/**
	 * simple code to create a list of objects
	 * @return List of ExampleBean's
	 */
	public static List<ExampleBean> getList()
	{
		List<ExampleBean> exList = new ArrayList<ExampleBean>();
		ExampleBean anon = new ExampleBean("a,32,10/16/2015"){
			@Override
			public String getDefaultDelimiter() {
				return "--";
			}

//			@Override
//			public String toString() {
//				return this.getClass().toString();
//			}
			
			
			
		};
		exList.add(anon);
		
		exList.add(new ExampleBean("a,1,10/11/2015"));
		exList.add(new ExampleBean("a,3,10/12/2015"));
		exList.add(new ExampleBean("a,7,10/13/2015"));
		exList.add(new ExampleBean("a,9,10/14/2015"));
		exList.add(new ExampleBean("a,15,10/15/2015"));

		return exList;
	}

	// all the getters and setters
	
	public String getField1() {
		return field1;
	}

	public void setField1(String field1) {
		this.field1 = field1;
	}

	public Integer getField2() {
		return field2;
	}

	public void setField2(Integer field2) {
		this.field2 = field2;
	}

	public Date getField3() {
		return field3;
	}

	public void setField3(Date field3) {
		this.field3 = field3;
	}

	@Override
	protected String[] getLoadFields() {
		return new String[]{"field1","field2","field3"};
	}
	
	

}
