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

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
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
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.json.Json;
import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.bigtable.v2.Mutation;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
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
public class TrafficSegmentBatchDF {
  private static final Logger LOG = LoggerFactory.getLogger(TrafficSegmentBatchDF.class);

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
	    String getOutput();
	    void setOutput(String value);
	    
	    @Description("The Cloud Pub/Sub topic to read from.")
	    @Default.String("")
	    String getInputTopic();
	    void setInputTopic(String value);
	    
	    @Description("GCS path to javascript fn for transforming output")
	    String getJavascriptTextTransformGcsPath();

	    void setJavascriptTextTransformGcsPath(String jsTransformPath);

	    @Description("UDF Javascript Function Name")
	    String getJavascriptTextTransformFunctionName();

	    void setJavascriptTextTransformFunctionName(
	        String javascriptTextTransformFunctionName);
	  }

  

  public static void main(String[] args) {
	  BatchTelemetryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		      .as(BatchTelemetryOptions.class);
    Pipeline p = Pipeline.create(options);

  /*  PCollection<String> input1 = p.apply(
    		  "ReadLines", TextIO.read().from(options.getInputFile()));*/
    
    String rowStr = "{ \"n\":\"Sion\", \"p\":\"laurent\" , \"a\":41 }";
    String segStr = "{\n" + 
    		"        \"_direction\": \"EB\",\n" + 
    		"        \"_fromst\": \"Pulaski\",\n" + 
    		"        \"_last_updt\": \"2018-05-10 14:50:27.0\",\n" + 
    		"        \"_length\": \"0.5\",\n" + 
    		"        \"_lif_lat\": \"41.7930671862\",\n" + 
    		"        \"_lit_lat\": \"41.793140551\",\n" + 
    		"        \"_lit_lon\": \"-87.7136071496\",\n" + 
    		"        \"_strheading\": \"W\",\n" + 
    		"        \"_tost\": \"Central Park\",\n" + 
    		"        \"_traffic\": \"20\",\n" + 
    		"        \"segmentid\": \"1\",\n" + 
    		"        \"start_lon\": \"-87.7231602513\",\n" + 
    		"        \"street\": \"55th\"\n" + 
    		"    }";
    
    
    p.apply("ReadPubsub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
    
    .apply(ParDo.of(new DoFn<Object, TableRow>() {
        @ProcessElement
        public void processElement(ProcessContext c)  {
          LOG.info(c.element().toString());
          Gson gson = new Gson();
          /*TrafficSegment segment = new TrafficSegment(
        		  "comment", "EB", "Pulaski", "2018-05-10 14:50:27.0", "0.5", "41.7930671862", "41.793140551", "-87.7136071496", "W", "Central Park", "20", "1", "-87.7231602513", "55th");
          */
          if(c.element().toString().startsWith("[")) {
        	  //Array
        	  Type collectionType = new TypeToken<Collection<TrafficSegment>>(){}.getType();
        	  Collection<TrafficSegment> segments = gson.fromJson(c.element().toString(), collectionType);
        	  int i = 0;
        	  for (Iterator iterator = segments.iterator(); iterator.hasNext();) {
				TrafficSegment trafficSegment = (TrafficSegment) iterator.next();
				System.err.println("["+ ++i +"]"+"------>>"+trafficSegment.toString());
				
				c.output(trafficSegment.toTableRow());
			}
          }else {
        	  //one segment
        	  TrafficSegment trafficSegment = gson.fromJson(c.element().toString(), TrafficSegment.class);
        	  System.err.println("------>>"+trafficSegment.toString());
        	  TableRow row = trafficSegment.toTableRow();
        	  try {
				System.out.println("COUNT:" + row.size()+", "+row.toPrettyString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	  c.output(trafficSegment.toTableRow());
          }
        }
      }))
    .apply("WriteBigQuery", BigQueryIO.writeTableRows().withoutValidation()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .to("lsion-151311:Sample.segments"));

    p.run();
  }
}