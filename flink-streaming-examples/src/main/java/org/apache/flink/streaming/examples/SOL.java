package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Created by manu on 15-12-1.
 */
public class SOL {
	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		int sizeInBytes = Integer.valueOf(args[0]);
		int sourceParallelism = Integer.valueOf(args[1]);
		int sinkParallelism = Integer.valueOf(args[2]);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream stream = env.addSource(new DataSource(sizeInBytes)).setParallelism(sourceParallelism);
		stream.shuffle().map(new Const()).setParallelism(sinkParallelism);

		// execute program
		env.execute("Streaming SOL");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class DataSource extends RichParallelSourceFunction<String> {
		private Random rand = new Random();
		final int differentMessages = 100;
		private String [] messages = new String[differentMessages];
		private int sizeInBytes = 100;
		private volatile boolean isRunning = true;

		public DataSource(int sizeInBytes) {
			this.sizeInBytes = sizeInBytes;
		}


		@Override
		public void open(Configuration parameters) throws Exception {
			for (int i = 0; i < differentMessages; i++) {
				StringBuilder sb = new StringBuilder(sizeInBytes);
				for(int j = 0; j < sizeInBytes; j++) {
					sb.append(rand.nextInt(9));
				}
				messages[i] = sb.toString();
			}
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while(isRunning) {
				String message = messages[rand.nextInt(messages.length)];
				ctx.collect(message);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}


	}


	public static final class Const implements MapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			System.out.println(value);
			return value;
		}
	}
}
