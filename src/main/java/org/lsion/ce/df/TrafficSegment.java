package org.lsion.ce.df;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.format.DateTimeFormat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;	
import com.google.protobuf.ByteString;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
/**
 * 
 * 
 * @author lsion
 * Todo: AvroCoder... (Enum)
 */
public class TrafficSegment implements Serializable {
	public static final String CHICAGO_CITY_ID = "1013962";
	public static final String CHICAGO_CITY_NAME = "Chicago";
	public static final String US_COUNTRY_CODE = "US";
	public final String cityID = CHICAGO_CITY_ID;
	public final String city = CHICAGO_CITY_NAME;
	public final String countryCode = US_COUNTRY_CODE;
	
	public static final String CSV_HEADERS = 
		"TIME,SEGMENT_ID,SPEED,STREET,DIRECTION,FROM_STREET,TO_STREET,LENGTH,STREET_HEADING,COMMENTS,BUS_COUNT,MESSAGE_COUNT,HOUR,DAY_OF_WEEK,MONTH,RECORD_ID,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE,START_LOCATION,END_LOCATION,Community Areas,Zip Codes,Wards";		
	public static final String[] KEYS = CSV_HEADERS.split(",");
	
	//BIGTABLE
	private final static String CF_FAMILY   = "segtraffic";
	
	private String _comments = null;
	private String _direction = null;
	private String _fromst = null;
	private String _last_updt = null;
	private String _length = null;
	private String _lif_lat = null;
	private String _lit_lat = null;
	private String _lit_lon = null;
	private String _strheading = null;
	private String _tost = null;
	private String _traffic = null;
	private String segmentid;
	private String start_lon;
	private String street = null;

	public TrafficSegment(String _comments, String _direction, String _fromst, String _last_updt, String _length,
			String _lif_lat, String _lit_lat, String _lit_lon, String _strheading, String _tost, String _traffic,
			String segmentid, String start_lon, String street) {
		this._comments = _comments;
		this._direction = _direction;
		this._fromst = _fromst;
		this._last_updt = _last_updt;
		this._length = _length;
		this._lif_lat = _lif_lat;
		this._lit_lat = _lit_lat;
		this._lit_lon = _lit_lon;
		this._strheading = _strheading;
		this._tost = _tost;
		this._traffic = _traffic;
		this.segmentid = segmentid;
		this.start_lon = start_lon;
		this.street = street;
		
		System.out.println("NEW SEGMENT WITH ID:"+this.segmentid);
		
	}
	public TrafficSegment(String csvValues) {
		Map<String, String> segment = new HashMap<>();
		String[] values = csvValues.split(",");
		

		if(KEYS.length != values.length) {
			if(! (KEYS.length -1 == values.length && csvValues.endsWith(","))) {
				throw new IllegalArgumentException("Invalid CSV values number in: "+csvValues);
			}
		}
		//OK
		
		for (int i = 0; i < values.length; i++) {
			segment.put(KEYS[i], values[i]);
		}
		
		this._comments = segment.get("COMMENTS");
		this._direction = segment.get("DIRECTION");
		this._fromst = segment.get("FROM_STREET");
		this._last_updt = segment.get("TIME");
		this._length = segment.get("LENGTH");
		this._lif_lat = segment.get("START_LATITUDE");
		this._lit_lat = segment.get("END_LATITUDE");
		this._lit_lon = segment.get("START_LATITUDE");
		this._strheading = segment.get("DIRECTION");
		this._tost = segment.get("TO_STREET");
		this._traffic = segment.get("SPEED");
		this.segmentid = segment.get("SEGMENT_ID");
		this.start_lon = segment.get("START_LONGITUDE");
		this.street = segment.get("STREET");
	}

	public String get_comments() {
		return _comments;
	}

	public void set_comments(String _comments) {
		this._comments = _comments;
	}

	public String get_direction() {
		return _direction;
	}

	public void set_direction(String _direction) {
		this._direction = _direction;
	}

	public String get_fromst() {
		return _fromst;
	}

	public void set_fromst(String _fromst) {
		this._fromst = _fromst;
	}

	public String get_last_updt() {
		return _last_updt;
	}

	public void set_last_updt(String _last_updt) {
		this._last_updt = _last_updt;
	}

	public String get_length() {
		return _length;
	}

	public void set_length(String _length) {
		this._length = _length;
	}

	public String get_lif_lat() {
		return _lif_lat;
	}

	public void set_lif_lat(String _lif_lat) {
		this._lif_lat = _lif_lat;
	}

	public String get_lit_lat() {
		return _lit_lat;
	}

	public void set_lit_lat(String _lit_lat) {
		this._lit_lat = _lit_lat;
	}

	public String get_lit_lon() {
		return _lit_lon;
	}

	public void set_lit_lon(String _lit_lon) {
		this._lit_lon = _lit_lon;
	}

	public String get_strheading() {
		return _strheading;
	}

	public void set_strheading(String _strheading) {
		this._strheading = _strheading;
	}

	public String get_tost() {
		return _tost;
	}

	public void set_tost(String _tost) {
		this._tost = _tost;
	}

	public String get_traffic() {
		return _traffic;
	}

	public void set_traffic(String _traffic) {
		this._traffic = _traffic;
	}

	public String getSegmentid() {
		return segmentid;
	}

	public void setSegmentid(String segmentid) {
		this.segmentid = segmentid;
	}

	public String getStart_lon() {
		return start_lon;
	}

	public void setStart_lon(String start_lon) {
		this.start_lon = start_lon;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCityID() {
		return cityID;
	}
	
	public String getCity() {
		return city;
	}

	public String getCountryCode() {
		return countryCode;
	}
	
	@Override
	public String toString() {
		return countryCode +", "+getCityID() +","+getSegmentid()+", "+get_last_updt();
	}
	
	public TableRow toTableRow() {
		TableRow row = new TableRow();
		//row.set(arg0, arg1);
		row.set("cityID", cityID);
		row.set("countryCode", countryCode);
		row.set("comments", _comments==null?"":_comments);
		row.set("direction", _direction);
		row.set("fromst", _fromst);
		row.set("last_updt", _last_updt);
		row.set("length", _length);
		row.set("lif_lat", _lif_lat);
		row.set("lit_lat", _lit_lat);
		row.set("lit_lon", _lit_lon);
		row.set("strheading", _strheading);
		row.set("tost", _tost);
		row.set("traffic", _traffic);
		row.set("segmentid", segmentid);
		row.set("start_lon", start_lon);
		row.set("street", street);
		row.set("city", city);
		return row;	
	}
	 public void toMutation(ProcessContext context) {
			TrafficSegment segment = this;
			DateTime ts = null;
			String key = null;
			
			DateTimeFormatter fmt = null;
			
			System.out.println("LAST : "+segment.get_last_updt());
			
			if(segment.get_last_updt().endsWith(".0")) {
				fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
				 ts = fmt.parseDateTime(segment.get_last_updt().replace(".0", ""));
				 key = segment.getSegmentid() //
				            + "#" + segment.getCityID() //
				            + "#" + segment.get_last_updt().replace(" ", "_").replace(".0", "");
			}else if(segment.get_last_updt().endsWith("M")){
				fmt = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a");
				ts = fmt.parseDateTime(segment.get_last_updt());
				// Format for output
				DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss");
				 key = segment.getSegmentid() //
				            + "#" + segment.getCityID() //
				            + "#" + dtfOut.print(ts);
				 System.err.println("KEY: "+key);
			}
			
			// key is SEGID#cityID#last_updt

			 String comments = segment.get_comments();
			// all the data is in a wide column table with only one column family
		        List<Mutation> mutations = new ArrayList<>();
		        addCell(mutations, "cityID", segment.getCityID(), ts.getMillis());
				addCell(mutations, "countryCode", segment.getCountryCode(), ts.getMillis());
				addCell(mutations, "comments", comments==null?"":comments, ts.getMillis());
				addCell(mutations, "direction",  segment.get_direction(), ts.getMillis());
				addCell(mutations, "fromst",  segment.get_fromst(), ts.getMillis());
				addCell(mutations, "last_updt", segment.get_last_updt(), ts.getMillis());
				addCell(mutations, "length", segment.get_length(), ts.getMillis());
				addCell(mutations, "lif_lat", segment.get_lif_lat(), ts.getMillis());
				addCell(mutations, "lit_lat", segment.get_lit_lat(), ts.getMillis());
				addCell(mutations, "lit_lon",segment.get_lit_lon(), ts.getMillis());
				addCell(mutations, "strheading", segment.get_strheading(), ts.getMillis());
				addCell(mutations, "tost", segment.get_tost(), ts.getMillis());
				addCell(mutations, "traffic", segment.get_traffic(), ts.getMillis());
				addCell(mutations, "segmentid", segment.getSegmentid(), ts.getMillis());
				addCell(mutations, "start_lon", segment.getStart_lon(), ts.getMillis());
				addCell(mutations, "street",segment.getStreet(), ts.getMillis());
				addCell(mutations, "city", segment.getCity(), ts.getMillis());
				context.output(KV.of(ByteString.copyFromUtf8(key), mutations));
		}
	  

	//BIGTABLE UTILS
	
	  private static void addCell(List<Mutation> mutations, String cellName, double cellValue, long ts) {
		    addCell(mutations, cellName, Double.toString(cellValue), ts);
		  }

		  private static void addCell(List<Mutation> mutations, String cellName, String cellValue, long ts) {
		    if (cellValue.length() > 0) {
		      ByteString value = ByteString.copyFromUtf8(cellValue);
		      ByteString colname = ByteString.copyFromUtf8(cellName);
		      Mutation m = //
		          Mutation.newBuilder()
		              .setSetCell(//
		                  Mutation.SetCell.newBuilder() //
		                      .setValue(value)//
		                      .setFamilyName(CF_FAMILY)//
		                      .setColumnQualifier(colname)//
		                     // .setTimestampMicros(ts) //
		                      .setTimestampMicros(-1) //
		              ).build();
		      mutations.add(m);
		    }
		  }
}