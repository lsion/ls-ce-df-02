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
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
public class PubSubJSON2BQSegmentDF {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubJSON2BQSegmentDF.class);

  public interface PubSubJSON2BQSegmentDFOptions extends PipelineOptions {
	    /**
	     * Set this required option to specify where to write the output.
	     */
	    /*@Description("BigQuery table like 'PROJECT:DATASET.TABLE'")
	    String getOutputBigQuery();
	    void setOutputBigQuery(String value);
	    */
	    @Description("The Cloud Pub/Sub topic to read from.")
	    @Default.String("")
	    String getInputTopic();
	    void setInputTopic(String value);
	    
	    @Description("The BigQuery dataset.")
	    @Default.String("")
	    String getbQDataset();
	    void setbQDataset(String value);
	    
	    @Description("The BigQuery project.")
	    @Default.String("")
	    String getbQProjectID();
	    void setbQProjectID(String value);
	    
	    @Description("The BigQuery table.")
	    @Default.String("")
	    String getbQTable();
	    void setbQTable(String value);
	  }


  public static void main(String[] args) {
	  PubSubJSON2BQSegmentDFOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		      .as(PubSubJSON2BQSegmentDFOptions.class);
    Pipeline p = Pipeline.create(options);
    
    String bigQueryString = 
    		options.getbQProjectID()+":"+options.getbQDataset()+"."+options.getbQTable();
    
    LOG.info("BigQuery string: "+bigQueryString);
    
    p.apply("ReadPubsub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
    .apply("ParseJSON", ParDo.of(new DoFn<Object, TableRow>() {
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
				
				c.output(trafficSegment.toTableRow());
			}
          }else {
        	  //one segment
        	  TrafficSegment trafficSegment = gson.fromJson(c.element().toString(), TrafficSegment.class);
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
            .to(bigQueryString));

    p.run();
  }
}