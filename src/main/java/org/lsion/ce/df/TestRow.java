package org.lsion.ce.df;

import java.time.LocalDateTime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TestRow{
	public static void main(String[] args) {
		////05/11/2018 03:50:20 AM,1192,-1,Dr Martin L King Jr,NB,24th,Cermak,0.25,S,,0,0,3,6,5,1192-201805110850,41.8493482767,-87.6181586591,41.8530007099,-87.619411,POINT (-87.6181586591 41.8493482767),POINT (-87.619411 41.8530007099),34,21194,10
		//TIME,SEGMENT_ID,SPEED,STREET,DIRECTION,FROM_STREET,TO_STREET,LENGTH,STREET_HEADING,COMMENTS,BUS_COUNT,MESSAGE_COUNT,HOUR,DAY_OF_WEEK,MONTH,RECORD_ID,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE,START_LOCATION,END_LOCATION,Community Areas,Zip Codes,Wards
		
		String headers= "TIME,SEGMENT_ID,SPEED,STREET,DIRECTION,FROM_STREET,TO_STREET,LENGTH,STREET_HEADING,COMMENTS,BUS_COUNT,MESSAGE_COUNT,HOUR,DAY_OF_WEEK,MONTH,RECORD_ID,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE,START_LOCATION,END_LOCATION,Community Areas,Zip Codes,Wards";
		String input = "05/11/2018 03:50:20 AM,1192,-1,Dr Martin L King Jr,NB,24th,Cermak,0.25,S,,0,0,3,6,5,1192-201805110850,41.8493482767,-87.6181586591,41.8530007099,-87.619411,POINT (-87.6181586591 41.8493482767),POINT (-87.619411 41.8530007099),34,21194,10";
	String input2 = "05/11/2018 03:50:20 AM,1134,28,Cicero,SB,Roosevelt,Cermak,1.02,S,Outside City Limits,1,2,3,6,5,1134-201805110850,41.8659627382,-87.7449852043,41.8512451897,-87.7444337191,POINT (-87.7449852043 41.8659627382),POINT (-87.7444337191 41.8512451897),26,22216,";
		String otherInput = "POINT (-87.6181586591 41.8493482767)";
		
		//System.err.println("DATE: "+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh.mm.ss")));
		//System.err.println("DATE: "+LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm a")));
		
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss");
		//System.err.println("DATE :"+fmt.parseDateTime("02/28/2018 05:01:00 PM")+", "+fmt.parseDateTime("02/28/2018 05:01:00 PM").getMillis());
	    
		
		DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd_hh:mm:ss.S a");
System.out.println("->>>"+dtfOut.print(fmt.parseDateTime("2010-07-21_14:51:09")));
		
		
		// specify that we want to search for two groups in the string
	    Pattern p = Pattern.compile("POINT \\((.*)\\s(.*)\\)");
	    
	    
	    Matcher m = p.matcher(otherInput);

	    // if our pattern matches the string, we can try to extract our groups
	    if (m.find())
	    {
	      // get the two groups we were looking for
	      String lng = m.group(1);
	      String lat = m.group(2);
	      
	      // print the groups, with a wee bit of formatting
	      System.out.format("'%s', '%s'\n", lng, lat);
	    }else
	    	System.out.format("Nothing");
		
	
		Map<String, String> segment = new HashMap<>();
		String[] keys = headers.split(",");
		String[] values = input2.split(",");
		
		if(keys.length != values.length) {
			System.err.println("ERREUR");
		//System.exit(-1);
		}
		//OK
		/*
		for (int i = 0; i < keys.length; i++) {
			segment.put(keys[i], values[i]);
		}
		
		for (Iterator iterator = segment.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			System.err.println(key+"="+segment.get(key));
		}
		TrafficSegment trafficSegment = new TrafficSegment(
				segment.get("COMMENTS"), 
				segment.get("DIRECTION"), 
				segment.get("FROM_STREET"), 
				segment.get("TIME"), 
				segment.get("LENGTH"), 
				segment.get("_lif_lat"), 
				segment.get("_lit_lat"), 
				segment.get("_lit_lon"), 
				segment.get("DIRECTION"), 
				segment.get("TO_STREET"), 
				segment.get("SPEED"), 
				segment.get("SEGMENT_ID"), 
				segment.get("start_lon"), 
				segment.get("STREET"));
		
		System.out.println("TrafficSeg: "+trafficSegment.toString());
		 */
		
		TrafficSegment segment2 = new TrafficSegment(input2);
		System.err.println("===>"+segment2.toString());
		
		
	}
}
