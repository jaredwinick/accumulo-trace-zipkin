/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.trace.instrument.receivers.zipkin;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.AnnotationType;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import com.twitter.zipkin.gen.zipkinCoreConstants;

public class ZipkinSpanBuilder {

	private Endpoint endPoint;
	
	private final Span zipkinSpan;
	
	private ZipkinSpanBuilder(int ipv4Address, short port, String serviceName) { 
		
		endPoint = new Endpoint(ipv4Address, port, serviceName);
		
		zipkinSpan = new Span();
	}
	
	/**
	 * Create a new ZipkinSpanBuilder
	 * @return
	 */
	public static ZipkinSpanBuilder newBuilder(int ipv4Address, short port, String serviceName) {
		return new ZipkinSpanBuilder(ipv4Address, port, serviceName);
	}
	
	public ZipkinSpanBuilder traceId(long traceId) {
		zipkinSpan.setTrace_id(traceId);
		return this;
	}
	
	public ZipkinSpanBuilder spanId(long spanId) {
		zipkinSpan.setId(spanId);
		return this;
	}
	
	public ZipkinSpanBuilder parentId(long parentId) {
		if (parentId != 0) {
			zipkinSpan.setParent_id(parentId);
		}
		return this;
	}
	
	public ZipkinSpanBuilder data(Map<String,String> data) {
		List<BinaryAnnotation> binaryAnnotations = Lists.newArrayList();
		for (Entry<String,String> entry : data.entrySet()) {
			BinaryAnnotation binaryAnnotation = new BinaryAnnotation();
			binaryAnnotation.setAnnotation_type(AnnotationType.BYTES);
			binaryAnnotation.setKey(entry.getKey());
			binaryAnnotation.setValue(entry.getValue().getBytes());
			binaryAnnotation.setHost(endPoint);
			binaryAnnotations.add(binaryAnnotation);
		}
		zipkinSpan.setBinary_annotations(binaryAnnotations);
		return this;
	}
	
	/**
	 * Set start time of Span in milliseconds
	 * @param start Start of Span in milliseconds
	 * @return this
	 */
	public ZipkinSpanBuilder startTimeMs(long start) {
		Annotation annotation = new Annotation();
		// Zipkin uses microseconds
		annotation.setTimestamp(start * 1000);
		annotation.setValue(zipkinCoreConstants.CLIENT_SEND);
		annotation.setHost(endPoint);
		zipkinSpan.addToAnnotations(annotation);
		return this;
	}
	
	/**
	 * Set the stop time of Span in milliseconds
	 * @param stop Stop of Span in milliseconds
	 * @return
	 */
	public ZipkinSpanBuilder stopTimeMs(long stop) {
		Annotation annotation = new Annotation();
		// Zipkin uses microseconds
		annotation.setTimestamp(stop * 1000);
		annotation.setValue(zipkinCoreConstants.CLIENT_RECV);
		annotation.setHost(endPoint);
		zipkinSpan.addToAnnotations(annotation);
		return this;
	}
	
	public ZipkinSpanBuilder description(String description) {
		zipkinSpan.setName(description);
		return this;
	}
	
	/**
	 * Return the built Zipkin Span
	 * @return the Span
	 */
	public Span build() {
		return zipkinSpan;
	}
}
