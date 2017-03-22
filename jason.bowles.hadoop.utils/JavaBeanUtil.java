/**
 * 
 */
package com.app.analytics.cf.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;


/**
 * @author id19868
 *
 */
public class JavaBeanUtil {

	/**
	 * 
	 */
	public JavaBeanUtil() {
		super();
		// 
	}

	public static void main(String[] args) {
		List<String> recList = new ArrayList<String>();
		List<JustATest> recs = new ArrayList<JustATest>();
		recs.add(new JustATest("Jason","Bowles"));
		recs.add(new JustATest("Heather","Bowles"));
		recs.add(new JustATest("Nolan","Bowles"));
		recs.add(new JustATest("Avery","Bowles"));
		recs.add(new JustATest("Everett","Bowles"));
		recs.add(new JustATest("Hadley","Bowles"));
		for(JustATest rec: recs)
		{
			recList.add(rec.getIdentifierRecord());
		}
		List<ExampleBean> recs2 = ExampleBean.getList();
		for(ExampleBean rec: recs2)
		{
			recList.add(rec.getIdentifierRecord());
		}

		try {
			List<Class<? extends BaseJavaBean>> typeList = new ArrayList<Class<? extends BaseJavaBean>>();
			typeList.add(JustATest.class);
			typeList.add(ExampleBean.class);
			JavaBeanMap map = parseRecs(recList,typeList);
			JavaBeanUtil.printJavaZBean(map.getBaseJavaBeans(JustATest.class));
			JavaBeanUtil.printJavaZBean(map.getBaseJavaBeans(ExampleBean.class));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	
	public static List<String> convertHadoopTextIterable(Iterable<Text> values)
	{
		List<String> list = new ArrayList<String>();
		for (Text value: values)
		{
			list.add(value.toString());
		}
		return list;
	}
	

	
	public static JavaBeanMap parseRecs(List<String> recs,List<Class<? extends BaseJavaBean>> types) throws Exception
	{
		JavaBeanMap bnMap = new JavaBeanMap();
		for(Class<? extends BaseJavaBean> type: types)
		{
			bnMap.setJavaBean(type);
		}
		
		for (String rec: recs)
		{
			String[] parts = rec.split("~");
			for(Class<? extends BaseJavaBean> type: types)
			{
				if (type.getName().equalsIgnoreCase(parts[0])){
					addRecord(getDelimiter(type),bnMap,rec,type);
				}
			}
		}
		return bnMap;
	}
	
	private static <T extends BaseJavaBean> String getDelimiter(Class<T> type)
	{
		return BaseJavaBean.getDelimiter(type);
	}
	
	private static <T extends BaseJavaBean> void addRecord(String delimiter, JavaBeanMap bnMap, String record, Class<T> type) throws Exception
	{
		bnMap.addJavaBean(type, BaseJavaBean.loadJavaBeanRecord(delimiter, record, type));
	}
	
	public static <T extends BaseJavaBean> void writeCSVFile(Collection<T> bnsList, String fileName) throws IOException
	{
		writeCSVFile(bnsList, fileName,true);
	}
	
	public static <T extends BaseJavaBean> void writeCSVFile(Collection<T> bnsList, String fileName, boolean writeHeader) throws IOException
	{
		writeCSVFile(bnsList, fileName, writeHeader, true);
	}
	
	public static <T extends BaseJavaBean> void writeCSVFile(Collection<T> bnsList, String fileName, boolean writeHeader,boolean includeSkip) throws IOException
	{
		writeFile(bnsList, fileName, writeHeader,includeSkip, ",");	
	}
	
	public static <T extends BaseJavaBean> void writeFile(Collection<T> bnsList, String fileName, boolean writeHeader,boolean includeSkip, String delimiter) throws IOException
	{
		Path path = Paths.get(fileName);
		boolean header = writeHeader;
		BufferedWriter writer = Files.newBufferedWriter(path);
		try
		{
		  for (BaseJavaBean rec: bnsList)
	 	  {
			  rec.setOutDelimiter(delimiter);
			  if (header){
				  writer.write(rec.createCSV(true, true, rec.getFields(includeSkip)));
				  header = false;
			  }
			  writer.write(rec.createCSV(false, true, rec.getFields(includeSkip)));
		  }
		}catch(IOException e)
		{
			throw e;
		}
		finally{
			writer.close();
		}	
	}

	public static <T extends BaseJavaBean> void printJavaZBean(Collection<T>bnsList)
	{
		if (bnsList.size()>0)
		{
			printJavaZBean(bnsList, System.out);
		}
		
	}
	
	/**
	 * This method attempts to output the contents of a ResultSet in a textual
	 * table. It relies on the ResultSetMetaData class, but a fair bit of the
	 * code is simple string manipulation.
	 * 
	 * 
	 * Date Modified Modified By Log Number Description 2009-03-03 Majors,
	 * Jeremy Changed "overwrite(line, colpos[i] + 1, value.toString().trim());"
	 * to "overwrite(line, colpos[i] + 1, String.valueOf(value).trim());" to
	 * avoid null pointer exceptions for objects that are null.
	 **/
	public static <T extends BaseJavaBean> void printJavaZBean(Collection<T> bns, OutputStream output){
		printResultsTable(bns, output, true, true);
	}

	/**
	 * This method attempts to output the contents of a ResultSet in a textual
	 * table. It relies on the ResultSetMetaData class, but a fair bit of the
	 * code is simple string manipulation.
	 * 
	 * 
	 * Date Modified Modified By Log Number Description 2009-03-03 Majors,
	 * Jeremy Changed "overwrite(line, colpos[i] + 1, value.toString().trim());"
	 * to "overwrite(line, colpos[i] + 1, String.valueOf(value).trim());" to
	 * avoid null pointer exceptions for objects that are null.
	 **/
	public static <T extends BaseJavaBean> void printResultsTable(Collection<T> bns, OutputStream output,
			boolean includeOutlines, boolean includeHeader) {
		printResultsTable(bns,output,includeOutlines,includeHeader,new ResultSetUtilOutputFormatter());
	}
	
	
	
	public static <T extends BaseJavaBean> void printResultsTable(Collection<T> bns, OutputStream output,
			boolean includeOutlines, boolean includeHeader, ResultSetUtilOutputFormatter outputFormatter) {
		// Set up the output stream
		PrintWriter out = new PrintWriter(new OutputStreamWriter(output));

		// Get some "meta data" (column names, etc.) about the results
		//ResultSetMetaData metadata = rs.getMetaData();

		// Variables to hold important data about the table to be displayed
		T aBean = bns.iterator().next();
		int numcols = aBean.getBeanNames().size();// how many columns
		String[] labels = new String[numcols]; // the column labels
		int[] colwidths = new int[numcols]; // the width of each
		int[] colpos = new int[numcols]; // start position of each
		int linewidth; // total width of table

		String[] beanData = aBean.allItems();
		// Figure out how wide the columns are, where each one begins,
		// how wide each row of the table will be, etc.
		linewidth = 1; // for the initial '|'.
		for (int i = 0; i < numcols; i++) { // for each column
			colpos[i] = linewidth; // save its position
			labels[i] = beanData[i]; // get its label


			// get the width of the largest piece of data
			int size = getWidth(bns, beanData[i]);
			if (size == -1)
				size = 30; // some drivers return -1...
			int labelsize = labels[i].length();
			if (labelsize > size)
				size = labelsize;
			colwidths[i] = size + (includeOutlines ? 1:0); // save the column the size
			linewidth += colwidths[i] + (includeOutlines ? 1:0); // increment total size
		}

		// Create a horizontal divider line we use in the table.
		// Also create a blank line that is the initial value of each
		// line of the table
		StringBuffer divider = new StringBuffer(linewidth);
		StringBuffer blankline = new StringBuffer(linewidth);

		for (int i = 0; i < linewidth; i++) {
			divider.insert(i, "-");
			blankline.insert(i, " ");
		}

		for (int i = 0; i < numcols; i++) {
			divider.setCharAt(colpos[i] - 1, '+');
			divider.setCharAt(linewidth - 1, '+');
		}

		if (includeOutlines) {
			// Begin the table output with a divider line
			out.println(divider);
		}

		
		// The next line of the table contains the column labels.
		// Begin with a blank line, and put the column names and column
		// divider characters "|" into it. overwrite() is defined below.
		StringBuffer line = new StringBuffer(blankline.toString() + (includeOutlines?"|":""));
		if (includeOutlines) {
			line.setCharAt(0, '|');
		}

		if (includeHeader){
			for (int i = 0; i < numcols; i++) {
				int pos = colpos[i] + 1 + (colwidths[i] - labels[i].length()) / 2;
				overwrite(line, pos, labels[i]);
				overwrite(line, colpos[i] + colwidths[i], includeOutlines ? "|"
						: "");
			}			
		}


		// Then output the line of column labels and another divider

		if (includeHeader) {
			out.println(line);
		}

		if (includeOutlines) {
			out.println(divider);
		}


		
		
		
		// iterate through the beans
		for (BaseJavaBean bn:bns) {
			line = new StringBuffer(blankline.toString() + (includeOutlines?"|":""));

			if (includeOutlines){
				line.setCharAt(0, '|');
			}
			
			
			for (int i = 0; i < numcols; i++) {
				String value = bn.getStringValue(labels[i]);

				//String columnName = rs.getMetaData().getColumnName(i+1);
			
				int increment = includeOutlines ? 1 :-1;
				overwrite(line, colpos[i] + increment, outputFormatter.format(value,null));
				overwrite(line, colpos[i] + colwidths[i],
						includeOutlines ? "|" : "");
			}
			
			// get rid of any special characters that would screw up the spacing within the report

			
			String lineWithoutSpecialChars = line.toString().replaceAll("\r", "");
			lineWithoutSpecialChars = lineWithoutSpecialChars.replaceAll("\n", "");
			lineWithoutSpecialChars = lineWithoutSpecialChars.replaceAll("\f", "");
			
			out.println(lineWithoutSpecialChars);
		}

		// Finally, end the table with one last divider line.
		if (includeOutlines){
			out.println(divider);			
		}
		out.flush();
	}
	

	private static <T extends BaseJavaBean> int getWidth(Collection<T> bns, String beanName) 
	{
		int largest = 0;
		for(BaseJavaBean bn:bns)
		{
			String val = bn.getStringValue(beanName);
			
			if(val != null)
			{
				if(val.toString().length()>largest)
				{
					largest = val.toString().length();
				}
			}
		}
		return largest;
	}

	public static Object getValueFromBean(BaseJavaBean bn, String getter) 
	{
		Object val = null;
		try
		{
			Method meth = bn.getClass().getMethod(getter, new Class<?>[0]);
			val = meth.invoke(bn, new Object[0]);
		}
		catch(Throwable t)
		{
			//
		}
		return val;
	}

	public static String getGetterName(String beanName) 
	{
		String name = "get" + beanName.substring(0, 1).toUpperCase() + beanName.substring(1);
		return name;
	}

	/** This utility method is used when printing the table of results */
	public static void overwrite(StringBuffer b, int pos, String s) {
		int len = s.length();
		for (int i = 0; i < len; i++)
			b.setCharAt(pos + i, s.charAt(i));
	}
	
	
}

