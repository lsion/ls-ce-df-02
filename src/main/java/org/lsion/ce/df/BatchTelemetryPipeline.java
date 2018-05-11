/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lsion.ce.df;

import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class BatchTelemetryPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(BatchTelemetryPipeline.class);

  public interface BatchTelemetryOptions extends PipelineOptions {

	    /**
	     * By default, this example reads from a public dataset containing the text of
	     * King Lear. Set this option to choose a different input file or glob.
	     */
	    @Description("Path of the file to read from")
	    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
	    String getInputFile();
	    void setInputFile(String value);

	    /**
	     * Set this required option to specify where to write the output.
	     */
	    @Description("Path of the file to write to")
	    @Required
	    String getOutput();
	    void setOutput(String value);
	  }


  public static void main(String[] args) {
	  BatchTelemetryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		      .as(BatchTelemetryOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> input = p.apply(
    		  "ReadLines", TextIO.read().from(options.getInputFile()));

    
   PCollection<TrafficSegment> segments=
    		  p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
    		  .apply(ParseJsons.of(TrafficSegment.class))
    	
    		;//  .setCoder(TableRowJsonCodeSerializableCoder.of(TrafficSegment.class));
    		   
/*
    PCollection<String> output = input.apply(MapElements.via(new SimpleFunction<String, String>() {
    	@Override
        public String apply(String input) {
          return input;
        }
    }));*/
    /*p.apply(Create.of("Hello World"))
    .apply(MapElements.via(new SimpleFunction<String, String>() {
      @Override
      public String apply(String input) {
        return input.toUpperCase();
      }
    }))
    .apply(ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c)  {
        LOG.info(c.element());
      }
    }));*/
   

   segments
   .apply(ParDo.of(new DoFn<TrafficSegment, Void>() {
	      @ProcessElement
	      public void processElement(ProcessContext c)  {
	        LOG.info(c.element().getSegmentid());
	      }
	    }));
		  // .apply(TextIO.write().to(options.getOutput()));
    p.run();
  }
  
  
  class TrafficSegment implements Serializable {
	  public final long cityID = 01;
	  
	  public String _comments= null;
	  public String _direction= null;
	  public String _fromst= null;
	  public String _last_updt= null;
	  public String _length= null;
	  public String _lif_lat= null;
	  public String _lit_lat= null;
	  public String _lit_lon= null;
	  public String _strheading= null;
	  public String _tost= null;
	  public String _traffic= null;
	  public String segmentid= null;
	  public String start_lon= null;
	  public String street= null;
	  
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

	public long getCityID() {
		return cityID;
	}
	  
	}
  
}
