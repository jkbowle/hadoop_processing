package jason.bowles.hadoop.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JustATest extends BaseJavaBean {

	
	private String name;
	private String lastName;
	private Date someDate;
	
	public static void main(String[] args) {
		//sortTest();
		//testDates();
		//List<Integer> argLengths = Arrays.asList((new Integer[]{2,4,6,8,10}));
		//System.out.println(argLengths);
		
		testTreeMap();
	}
	
	
	public static void testTreeMap()
	{
		TreeMap<Date,Integer> testMap = new TreeMap<Date,Integer>();
		try {
			testMap.put(new SimpleDateFormat("MM/dd/yyyy").parse("10/1/2015"), 5);
			testMap.put(new SimpleDateFormat("MM/dd/yyyy").parse("3/1/2015"), 2);
			testMap.put(new SimpleDateFormat("MM/dd/yyyy").parse("6/1/2015"), 3);
			testMap.put(new SimpleDateFormat("MM/dd/yyyy").parse("1/1/2015"), 1);
			testMap.put(new SimpleDateFormat("MM/dd/yyyy").parse("8/1/2015"), 4);
			
			Date test = new SimpleDateFormat("MM/dd/yyyy").parse("5/1/2015");
			System.out.println(testMap);
			
			testMap.keySet().stream().filter((x) -> x.before(test)).forEach(System.out::println);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void testDates()
	{
		
		
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
		try {
			JustATest v1 = new JustATest("Jason", "Bowles", format.parse("11/27/1976"));
			JustATest v2 = new JustATest("Jason", "Bowles", format.parse("11/27/1978"));
			System.out.println(v1.getYearDiff(v1.getSomeDate(), v2.getSomeDate()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testSomething(){
		List<List<String>> listOLists = new ArrayList<List<String>>();

		for(int i = 0; i<5;i++)
		{
			List<String> inList = new ArrayList<String>();
			for (int j = 0; j < 5; j++)
			{
				Integer val = j*(i+1);
				inList.add(val.toString());
			}
			listOLists.add(inList);
		}

		List<String> var = listOLists.stream()
				.flatMap(List::stream)
				.filter(a -> a.equalsIgnoreCase("4"))
				.collect(Collectors.toList());
		System.out.println(var);
	}
	
	public static void sortTest()
	{
		Map<Integer, Integer> unsortMap = new HashMap<>();
        unsortMap.put(500, 10);
        unsortMap.put(600, 5);
        unsortMap.put(700, 6);
        unsortMap.put(800, 20);
        unsortMap.put(12, 1);
        unsortMap.put(4500, 7);
        unsortMap.put(6, 8);
        unsortMap.put(1, 99);
        unsortMap.put(451, 50);
        unsortMap.put(1000, 2);
        unsortMap.put(99, 9);

        System.out.println("Original...");
        System.out.println(unsortMap);

        Map<Integer, Integer> result = new LinkedHashMap<>();

        //sort by key, a,b,c..., and put it into the "result" map
        unsortMap.entrySet().stream()
                .sorted(Map.Entry.<Integer, Integer>comparingByKey().reversed())
                .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

        System.out.println("Sorted...");
        System.out.println(result);
	}
	public static void test1()
	{
		List<String> fields = new ArrayList<String>();
		fields.add("name");
		fields.add("lastName");
		List<JustATest> recs = new ArrayList<JustATest>();
		recs.add(new JustATest(",","Jason","Bowles"));
		recs.add(new JustATest(",","Heather","Bowles"));
		recs.add(new JustATest(",","Nolan","Bowles"));
		recs.add(new JustATest(",","Avery","Bowles"));
		recs.add(new JustATest(",","Everett","Bowles"));
		recs.add(new JustATest(",","Hadley","Bowles"));
		
		List<String> allNames = recs.stream().map(p -> p.getStringValue("name")).collect(Collectors.toList());
		System.out.println(allNames);
		Map<String, List<?>> colValues = new HashMap<String,List<?>>();
		fields.forEach(name -> colValues.put(name, recs.stream().map(p -> p.getValue(name)).collect(Collectors.toList())));
		System.out.println(colValues);
	}
	
	public JustATest()
	{
		super();
	}
	
	public JustATest(String delimiter, String record)
	{
		super(delimiter);
		this.loadRecord(record);
	}
	
	public JustATest(String delimiter, String argName, String argLastName)
	{
		super(delimiter);
		this.name = argName;
		this.lastName = argLastName;
		this.someDate = Date.from(Instant.now());
		this.setFullRecord(this.createHDFSRecord(true));
	}
	
	public JustATest(String argName, String argLastName, Date argSomeDate)
	{
		super(",");
		this.name = argName;
		this.lastName = argLastName;
		this.someDate = argSomeDate;
		this.setFullRecord(this.createHDFSRecord(true));
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	

	public Date getSomeDate() {
		return someDate;
	}



	public void setSomeDate(Date someDate) {
		this.someDate = someDate;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected <T extends BaseJavaBean> Class<T> getJavaBeanClass() {
		return (Class<T>) JustATest.class;
	}

	@Override
	protected String[] getKeyFields() {
		// TODO Auto-generated method stub
		return null;
	}

	public static void oldJava()
	{
		List<String> keywords = Arrays.asList("Apple", "Ananas", "Mango", "Banana", "Beer"); 
		Map<Character, List<String>> result = new HashMap<Character, List<String>>(); 
		for(String k : keywords) {   
		    char firstChar = k.charAt(0);     
		    if(!result.containsKey(firstChar)) {     
		        result.put(firstChar, new  ArrayList<String>());   
		    }     
		    result.get(firstChar).add(k); 
		} 
		for(List<String> list : result.values()) {   
		    Collections.sort(list); 
		}
		System.out.println(result); 
	}
	
	public static void newJava()
	{
		Map<Character, List<String>> result = Stream
			      .of( "Apple", "Ananas", "Mango", "Banana","Beer")
			      .sorted()
			      .collect(Collectors.groupingBy(it -> it.charAt(0)));

			System.out.println(result);
	}


	@Override
	protected String[] getLoadFields() {
		return new String[]{"name","lastName","someDate"};
	}


	@Override
	public String getDefaultDelimiter() {
		return ",";
	}
}
