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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.v2.Mutation;
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
public class CSV2BTSegmentDFCloud {
	private static final Logger LOG = LoggerFactory.getLogger(CSV2BTSegmentDFCloud.class);
	// BIGTABLE

	public interface CSV2BTSegmentDFOptions extends PipelineOptions {

		/**
		 * By default, this example reads from a public dataset containing the text of
		 * King Lear. Set this option to choose a different input file or glob.
		 */
		@Description("Path of the file to read from")
		ValueProvider<String> getInputFile();
		void setInputFile(ValueProvider<String> value);

		@Description("The Cloud Pub/Sub topic to read from.")
		@Default.String("")
		ValueProvider<String> getInputTopic();
		void setInputTopic(ValueProvider<String> value);

		/**
		 * Set this required option to specify where to write the output.
		 */
		@Description("BigTable project ID.")
		ValueProvider<String> getbTProjectID();
		void setbTProjectID(ValueProvider<String> value);

		/**
		 * Set this required option to specify where to write the output.
		 */
		@Description("BigTable instance ID.")
		ValueProvider<String> getbTInstanceID();
		void setbTInstanceID(ValueProvider<String> value);
		
		/**
		 * Set this required option to specify where to write the output.
		 */
		@Description("BigTable table ID.")
		ValueProvider<String> getbTTableID();
		void setbTTableID(ValueProvider<String> value);
	}

	public static void main(String[] args) {
		CSV2BTSegmentDFOptions options = PipelineOptionsFactory.fromArgs(args)
				.as(CSV2BTSegmentDFOptions.class);
		Pipeline p = Pipeline.create(options);
		
		PCollection<KV<ByteString, Iterable<Mutation>>> mutations = p.apply(
	    		  "ReadLines", TextIO.read().from(options.getInputFile()))
				.apply("ParseCSV", ParDo.of(new DoFn<Object, KV<ByteString, Iterable<Mutation>>>() {
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
					}
				}));

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

		LOG.info("Project: "+options.getbTProjectID()+", BT instance: "+options.getbTInstanceID()+", BT table ID: "+options.getbTTableID());
		mutations.apply("WriteBigTable", //
				BigtableIO.write().withProjectId(options.getbTProjectID())
				.withInstanceId(options.getbTInstanceID())
				.withTableId(options.getbTTableID())
				.withoutValidation());
	}
}