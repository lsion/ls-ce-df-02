package org.lsion.ce.df;

import java.io.Serializable;

import com.google.api.services.bigquery.model.TableRow;

class TrafficSegment implements Serializable {
	public static final String CHICAGO_CITY_ID = "1013962";
	public static final String CHICAGO_CITY_NAME = "Chicago";
	public static final String US_COUNTRY_CODE = "US";
	public final String cityID = CHICAGO_CITY_ID;
	public final String city = CHICAGO_CITY_NAME;
	public final String countryCode = US_COUNTRY_CODE;
	
	public String _comments = null;
	public String _direction = null;
	public String _fromst = null;
	public String _last_updt = null;
	public String _length = null;
	public String _lif_lat = null;
	public String _lit_lat = null;
	public String _lit_lon = null;
	public String _strheading = null;
	public String _tost = null;
	public String _traffic = null;
	public String segmentid = null;
	public String start_lon = null;
	public String street = null;

	public TrafficSegment(String _comments, String _direction, String _fromst, String _last_updt, String _length,
			String _lif_lat, String _lit_lat, String _lit_lon, String _strheading, String _tost, String _traffic,
			String segmentid, String start_lon, String street) {
		super();
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

	@Override
	public String toString() {
		return countryCode +", "+cityID +","+segmentid+", "+_last_updt;
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

	public String getCountryCode() {
		return countryCode;
	}
}