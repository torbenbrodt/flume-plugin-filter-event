/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package filterevent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * 
 */
public class FilterEventDecorator<S extends EventSink> extends EventSinkDecorator<S> {

	final String attribute;
	final String value;
	final String value_type;

	public FilterEventDecorator(S s, String attribute, String value, String value_type) {
		super(s);
		Preconditions.checkNotNull(attribute);
		Preconditions.checkNotNull(value);
		Preconditions.checkNotNull(value_type);
		this.attribute  = attribute;
		this.value      = value;
		this.value_type = value_type;
	}

	@Override
	public void append(Event e) throws IOException, InterruptedException {
		Preconditions.checkNotNull(e);
		byte[] value = e.get(this.attribute);
		if (value != null) {
		       if (this.value_type.equals("int")) {
			       if (Integer.parseInt(new String(value)) == Integer.parseInt(this.value))
				       super.append(e);
		       } else {
				if (new String(value).equals(this.value)) {
				       super.append(e);
				}
		       }
		}
	}

	public static SinkDecoBuilder builder() {
		return new SinkDecoBuilder() {
			// construct a new parameterized decorator
			@Override
			public EventSinkDecorator<EventSink> build(Context context,
						String... argv) {
				Preconditions.checkArgument(argv.length == 3,
						"usage: filterEvent(attr, value, value_type {int,string})");

				return new FilterEventDecorator<EventSink>(null, argv[0], argv[1], argv[2]);
			}

		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this class
	 * as a plugin decorator.
	 */
	public static List<Pair<String, SinkDecoBuilder>> getDecoratorBuilders() {
		List<Pair<String, SinkDecoBuilder>> builders =
			new ArrayList<Pair<String, SinkDecoBuilder>>();
		builders.add(new Pair<String, SinkDecoBuilder>("filterEvent",
					builder()));
		return builders;
	}
}
