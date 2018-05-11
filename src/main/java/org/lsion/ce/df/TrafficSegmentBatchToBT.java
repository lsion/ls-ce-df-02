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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.common.reflect.TypeToken;
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
public class TrafficSegmentBatchToBT {
	private static final Logger LOG = LoggerFactory.getLogger(TrafficSegmentBatchToBT.class);
	// BIGTABLE
	private final static String INSTANCE_ID = "ls-ce-bt";
	private final static String TABLE_ID = "segments";
	private final static String CF_FAMILY = "segtraffic";
	private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

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

		void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);
	}

	public static void main(String[] args) {
		BatchTelemetryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BatchTelemetryOptions.class);
		Pipeline p = Pipeline.create(options);

		/*
		 * PCollection<String> input1 = p.apply( "ReadLines",
		 * TextIO.read().from(options.getInputFile()));
		 */

		String rowStr = "{ \"n\":\"Sion\", \"p\":\"laurent\" , \"a\":41 }";
		String segStr = "{\n" + "        \"_direction\": \"EB\",\n" + "        \"_fromst\": \"Pulaski\",\n"
				+ "        \"_last_updt\": \"2018-05-10 14:50:27.0\",\n" + "        \"_length\": \"0.5\",\n"
				+ "        \"_lif_lat\": \"41.7930671862\",\n" + "        \"_lit_lat\": \"41.793140551\",\n"
				+ "        \"_lit_lon\": \"-87.7136071496\",\n" + "        \"_strheading\": \"W\",\n"
				+ "        \"_tost\": \"Central Park\",\n" + "        \"_traffic\": \"20\",\n"
				+ "        \"segmentid\": \"1\",\n" + "        \"start_lon\": \"-87.7231602513\",\n"
				+ "        \"street\": \"55th\"\n" + "    }";

		PCollection<KV<ByteString, Iterable<Mutation>>> mutations = p
				.apply("ReadPubsub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
				.apply(ParDo.of(new DoFn<Object, KV<ByteString, Iterable<Mutation>>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						LOG.info(c.element().toString());
						Gson gson = new Gson();
						/*
						 * TrafficSegment segment = new TrafficSegment( "comment", "EB", "Pulaski",
						 * "2018-05-10 14:50:27.0", "0.5", "41.7930671862", "41.793140551",
						 * "-87.7136071496", "W", "Central Park", "20", "1", "-87.7231602513", "55th");
						 */
						if (c.element().toString().startsWith("[")) {
							// Array
							Type collectionType = new TypeToken<Collection<TrafficSegment>>() {
							}.getType();
							Collection<TrafficSegment> segments = gson.fromJson(c.element().toString(), collectionType);
							int i = 0;
							for (Iterator iterator = segments.iterator(); iterator.hasNext();) {
								TrafficSegment trafficSegment = (TrafficSegment) iterator.next();
								System.err.println("[" + ++i + "]" + "------>>" + trafficSegment.toString());
								trafficSegment.toMutation(c);
								// c.output(trafficSegment.toTableRow());

							}
						} else {
							// one segment
							TrafficSegment trafficSegment = gson.fromJson(c.element().toString(), TrafficSegment.class);
							System.err.println("****------>>" + trafficSegment.toString());
							// TableRow row = trafficSegment.toTableRow();
							// c.output(trafficSegment.toTableRow());
							trafficSegment.toMutation(c);
						}
					}
				}));

		writeToBigtable(mutations, options);
		/*
		 * .apply("WriteBigQuery", BigQueryIO.writeTableRows().withoutValidation()
		 * .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
		 * .to("lsion-151311:Sample.segments"));
		 */

		p.run();
	}

	// BIGTABLE UTILS

	public static void writeToBigtable(PCollection<KV<ByteString, Iterable<Mutation>>> mutations,
			BatchTelemetryOptions options) {
		BigtableOptions.Builder optionsBuilder = //
				new BigtableOptions.Builder()//
						.setProjectId("lsion-151311")//options.getProject()) //
						.setInstanceId(INSTANCE_ID).setUserAgent("101258083480215606780");
		optionsBuilder.setProjectId("lsion-151311");
		// batch up requests to Bigtable every 100ms, although this can be changed
		// by specifying a lower/higher value for
		// BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT
		BulkOptions bulkOptions = new BulkOptions.Builder().enableBulkMutationThrottling().build();
		optionsBuilder = optionsBuilder.setBulkOptions(bulkOptions);

		// createEmptyTable(options, optionsBuilder);
		//mutations.apply("write:cbt", //
		//		BigtableIO.write().withBigtableOptions(optionsBuilder.build()).withTableId(TABLE_ID));
		mutations.apply("write:cbt", //
				BigtableIO.write().withProjectId("lsion-151311").withInstanceId(INSTANCE_ID).withTableId(TABLE_ID).withoutValidation());
	}
}