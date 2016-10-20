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
package org.apache.flink.streaming.runtime.operators.windowing.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Internal interface for functions that are evaluated over keyed (grouped) windows.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 */
public abstract class InternalProcessWindowFunction<IN, OUT, KEY, W extends Window>
		implements Function, Serializable, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	public abstract void process(KEY key, Iterable<IN> elements, Context ctx, Collector<OUT> out) throws Exception;

	protected void process(ProcessWindowFunction<IN, OUT, KEY, W> fn,
		KEY key, Iterable<IN> elements, Context ctx, Collector<OUT> out) throws Exception {
		ContextWrapper wrapper = new ContextWrapper(fn, ctx.window(), ctx.watermark());
		fn.process(key, elements, wrapper, out);
	}

	public abstract class Context {
		public abstract W window();
		public abstract long watermark();
	}

	protected class ContextWrapper
		extends ProcessWindowFunction.Context {

		private final W window;
		private final long watermark;

		public ContextWrapper(ProcessWindowFunction fn, W window, long watermark) {
			fn.super();
			this.window = window;
			this.watermark = watermark;
		}

		@Override
		public W window() {
			return window;
		}

		@Override
		public long watermark() {
			return watermark;
		}
	}
}
