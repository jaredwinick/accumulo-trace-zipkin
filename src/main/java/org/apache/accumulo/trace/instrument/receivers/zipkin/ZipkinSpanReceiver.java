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

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.instrument.receivers.AsyncSpanReceiver;
import org.apache.accumulo.trace.instrument.receivers.SpanReceiver;
import org.apache.accumulo.trace.thrift.RemoteSpan;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.twitter.zipkin.gen.LogEntry;
import com.twitter.zipkin.gen.Scribe;
import com.twitter.zipkin.gen.Scribe.Client;
import com.twitter.zipkin.gen.Span;

/**
 * An Accumulo SpanReceiver implementation that sends Spans to a Zipkin Collector. 
 * Much of the logic/code is borrowed from HTrace's ZipkinSpanReceiver
 * https://github.com/cloudera/htrace/blob/master/htrace-zipkin/src/main/java/org/cloudera/htrace/impl/ZipkinSpanReceiver.java
 * 
 * TODO: Handle Client vs. Server annotations (see ZipkinSpanBuilder startTimeMs/stopTimeMs)
 * TODO: see comment on send method - we could optimize if we can batch Spans sent in the Scribe Client
 * 
 */
public class ZipkinSpanReceiver extends AsyncSpanReceiver<InetSocketAddress, Client> {

	private final Logger log = Logger.getLogger(ZipkinSpanReceiver.class);

	private final String CATEGORY = "zipkin";

	private final String host;
	private final String serviceName;
	private final String collectorHost;
	private final int collectorPort;
	private final InetSocketAddress collectorSocketAddress;

	private final TProtocolFactory protocolFactory;
	private final ByteArrayOutputStream byteArrayOutputStream;
	private final TProtocol streamProtocol;

	// the IP address of the specified host
	private int ipv4Address;
	private short port = 8080;

	/**
	 * Create a ZipkinSpanReceiver to send Spans to a Zipkin Collector
	 * @param host The host name of the service being traced
	 * @param serviceName The name of the traced service
	 * @param collectorHost The host name of the Zipkin Collector
	 * @param collectorPort The port of the Zipkin Collector
	 * @param millis period to flush queued up Spans
	 */
	public ZipkinSpanReceiver(String host, String serviceName,
			String collectorHost, int collectorPort, int millis) {
		super(host, serviceName, millis);
		
		this.host = host;
		this.serviceName = serviceName;
		this.collectorHost = collectorHost;
		this.collectorPort = collectorPort;
		this.collectorSocketAddress = new InetSocketAddress(collectorHost, collectorPort);
		

		this.protocolFactory = new TBinaryProtocol.Factory();
		byteArrayOutputStream = new ByteArrayOutputStream();
		streamProtocol = protocolFactory.getProtocol(new TIOStreamTransport(
				byteArrayOutputStream));

		// Attempt to get the IP address of the specified host
		try {
			ipv4Address = ByteBuffer.wrap(
					InetAddress.getByName(host).getAddress()).getInt();
		} catch (UnknownHostException e) {
			log.error("Unable to get address for host:" + host);
			ipv4Address = 0;
		}
	}

	public void flush() {
		super.flush();
	}

	@Override
	protected Client createDestination(InetSocketAddress destination) throws Exception {
		if (destination == null)
			return null;
		
		log.debug("Connecting to " + destination.getHostName() + ":" + destination.getPort());
		TTransport transport = new TFramedTransport(new TSocket(destination.getHostName(), destination.getPort()));
		try {
			transport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		TProtocol protocol = protocolFactory.getProtocol(transport);
		return new Scribe.Client(protocol);
	}

	@Override
	protected InetSocketAddress getSpanKey(Map<String, String> data) {
		return collectorSocketAddress;
	}

	/**
	 * Seems like we would want to be able to batch the Spans going to the Zipkin
	 * Collector as that is what the Scribe clients supports. This would require
	 * a change to the AsyncSpanReceiver class
	 * 
	 * Also having already converted to a RemoteSpan doesn't make sense with a different
	 * receiver than the SendSpansViaThrift receiver
	 */
	@Override
	protected void send(Client client, RemoteSpan remoteSpan) throws Exception {
		if (client != null) {
			
			log.debug("Span:" + remoteSpan.toString());
			
			Span zipkinSpan = ZipkinSpanBuilder
					.newBuilder(ipv4Address, port, serviceName)
					.traceId(remoteSpan.getTraceId())
					.spanId(remoteSpan.getSpanId())
					.parentId(remoteSpan.getParentId())
					.startTimeMs(remoteSpan.getStart())
					.stopTimeMs(remoteSpan.getStop())
					.description(remoteSpan.getDescription())
					.data(remoteSpan.getData())
					.build();
			
			try {
				// clear old data
				byteArrayOutputStream.reset();
				// write the Span to the underlying ByteArrayOutputStream
				zipkinSpan.write(streamProtocol);
	
				// Do Base64 encoding and put the string into a log entry.
				LogEntry logEntry = new LogEntry(CATEGORY, Base64.encodeBase64String(byteArrayOutputStream.toByteArray()));
	
				List<LogEntry> logEntries = Lists.newArrayList(logEntry);
				client.Log(logEntries);
			} catch (Exception e) {
				log.error("Error writing to collector:" + collectorHost, e);
				client.getInputProtocol().getTransport().close();
				client = null;
			}
		}

	}
	
	public static void main(String[] args) {
		BasicConfigurator.configure();
		Tracer.getInstance().addReceiver(new ZipkinSpanReceiver("localhost", "Test Service", "localhost", 9410, 500));
		
		Trace.on("Query");
		org.apache.accumulo.trace.instrument.Span childSpan = Trace.start("Child");
		try {
			Thread.sleep(20);
		} catch (Exception e) { }
		childSpan.data("myKey", "myValue");
		childSpan.stop();
		Trace.off();
		
	}

}
