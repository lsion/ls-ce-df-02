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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.v2.Mutation;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class CSV2BTSegmentDF {
	private static final Logger LOG = LoggerFactory.getLogger(CSV2BTSegmentDF.class);
	// BIGTABLE
	private final static String INSTANCE_ID = "ls-ce-bt";
	private final static String TABLE_ID = "segments";
	private final static String CF_FAMILY = "segtraffic";
	private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	public interface CSV2BTSegmentDFOptions extends PipelineOptions {

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

		void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);
	}

	public static void main(String[] args) {
		CSV2BTSegmentDFOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CSV2BTSegmentDFOptions.class);
		Pipeline p = Pipeline.create(options);
		
		PCollection<KV<ByteString, Iterable<Mutation>>> mutations = p.apply(
	    		  "ReadLines", TextIO.read().from(options.getInputFile()))
				.apply(ParDo.of(new DoFn<Object, KV<ByteString, Iterable<Mutation>>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						LOG.info(c.element().toString());
												String line = c.element().toString();
						if(line.startsWith("TIME")) {
							//header line
							//skip
							c= null;
						}else {
							TrafficSegment trafficSegment = new TrafficSegment(line);
							//LOG.info("Line: "+line);
							LOG.info("Segment prepared for BT insertion: "+trafficSegment.toString());
							;
							trafficSegment.toMutation(c);
						}
							
						Gson gson = new Gson();

						/*if (c.element().toString().startsWith("[")) {
							// Array
							Type collectionType = new TypeToken<Collection<TrafficSegment>>() {
							}.getType();
							Collection<TrafficSegment> segments = gson.fromJson(c.element().toString(), collectionType);
							int i = 0;
							for (Iterator iterator = segments.iterator(); iterator.hasNext();) {
								//TrafficSegment trafficSegment = (TrafficSegment) iterator.next();
								//System.err.println("[" + ++i + "]" + "------>>" + trafficSegment.toString());
								((TrafficSegment) iterator.next()).toMutation(c);
								++i;
							}
							LOG.info(i + " segments prepared for BT insertion.");
						} else {
							// one segment
							TrafficSegment trafficSegment = gson.fromJson(c.element().toString(), TrafficSegment.class);
							//System.err.println("****------>>" + trafficSegment.toString());
							LOG.info("Segment prepared for BT insertion: "+trafficSegment.toString());
							trafficSegment.toMutation(c);
						}*/
					}
				}));
	    		   
	/*
	    PCollection<String> output = input.apply(MapElements.via(new SimpleFunction<String, String>() {
	    	@Override
	        public String apply(String input) {
	          return input;
	        }
	    }));

		PCollection<KV<ByteString, Iterable<Mutation>>> mutations = 
				p.apply("ReadPubsub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
				.apply(ParDo.of(new DoFn<Object, KV<ByteString, Iterable<Mutation>>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						LOG.info(c.element().toString());
						Gson gson = new Gson();

						if (c.element().toString().startsWith("[")) {
							// Array
							Type collectionType = new TypeToken<Collection<TrafficSegment>>() {
							}.getType();
							Collection<TrafficSegment> segments = gson.fromJson(c.element().toString(), collectionType);
							int i = 0;
							for (Iterator iterator = segments.iterator(); iterator.hasNext();) {
								//TrafficSegment trafficSegment = (TrafficSegment) iterator.next();
								//System.err.println("[" + ++i + "]" + "------>>" + trafficSegment.toString());
								((TrafficSegment) iterator.next()).toMutation(c);
								++i;
							}
							LOG.info(i + " segments prepared for BT insertion.");
						} else {
							// one segment
							TrafficSegment trafficSegment = gson.fromJson(c.element().toString(), TrafficSegment.class);
							//System.err.println("****------>>" + trafficSegment.toString());
							LOG.info("Segment prepared for BT insertion: "+trafficSegment.toString());
							trafficSegment.toMutation(c);
						}
					}
				}));
				*/
		
		writeToBigtable(mutations, options);
		p.run();
		LOG.info("Pipeline ready.");
	}

	// BIGTABLE UTILS

	public static void writeToBigtable(PCollection<KV<ByteString, Iterable<Mutation>>> mutations,
			CSV2BTSegmentDFOptions options) {
		LOG.info("BT setup...");
		/*BigtableOptions.Builder optionsBuilder = //
				new BigtableOptions.Builder()//
						.setProjectId("lsion-151311")//options.getProject()) //
						.setInstanceId(INSTANCE_ID).setUserAgent("101258083480215606780");*/
		//optionsBuilder.setProjectId("lsion-151311");
		// batch up requests to Bigtable every 100ms, although this can be changed
		// by specifying a lower/higher value for
		// BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT
		//BulkOptions bulkOptions = new BulkOptions.Builder().enableBulkMutationThrottling().build();
		//optionsBuilder = optionsBuilder.setBulkOptions(bulkOptions);


		LOG.info("Project (hard coded): lsion-151311, BT instance: "+INSTANCE_ID+", BT table ID: "+TABLE_ID);
		mutations.apply("write:cbt", //
				BigtableIO.write().withProjectId("lsion-151311").withInstanceId(INSTANCE_ID).withTableId(TABLE_ID).withoutValidation());
	}
}