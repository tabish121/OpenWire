/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openwire.codec;

import io.openwire.commands.BrokerInfo;
import io.openwire.commands.Command;
import io.openwire.commands.KeepAliveInfo;
import io.openwire.commands.Response;
import io.openwire.commands.ShutdownInfo;
import io.openwire.commands.WireFormatInfo;
import io.openwire.util.TcpTransport;
import io.openwire.util.TransportListener;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class used in testing the interoperability between the OpenWire
 * commands and Marshalers in this library and those in ActiveMQ.
 */
public abstract class OpenWireInteropTestSupport implements TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(OpenWireInteropTestSupport.class);

    private TcpTransport transport;
    private URI connectionURI;
    private BrokerService brokerService;

    private OpenWireFormatFactory factory;
    private OpenWireFormat wireFormat;

    private CountDownLatch connected;

    private WireFormatInfo remoteWireformatInfo;
    private BrokerInfo remoteInfo;
    private Exception failureCause;

    private Command latest;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();

        factory = new OpenWireFormatFactory();
        factory.setVersion(getOpenWireVersion());

        wireFormat = factory.createWireFormat();
    }

    @After
    public void tearDown() throws Exception {
        disconnect();

        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected abstract int getOpenWireVersion();

    protected void connect() throws Exception {
        connected = new CountDownLatch(1);

        transport = new TcpTransport(wireFormat, connectionURI);
        transport.setTransportListener(this);
        transport.start();
    }

    protected void disconnect() throws Exception {
        if (transport != null && transport.isStarted()) {
            ShutdownInfo done = new ShutdownInfo();
            transport.oneway(done);
            Thread.sleep(50);
            transport.stop();
        }
    }

    protected boolean awaitConnected(long time, TimeUnit unit) throws InterruptedException {
        return connected.await(time, unit);
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseJmx(true);

        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        connectionURI = connector.getPublishableConnectURI();
        LOG.debug("Using openwire port: {}", connectionURI);
        return brokerService;
    }

    @Override
    public void onCommand(Object command) {
        try {
            if (command instanceof WireFormatInfo) {
                handleWireFormatInfo((WireFormatInfo) command);
            } else if (command instanceof KeepAliveInfo) {
                handleKeepAliveInfo((KeepAliveInfo) command);
            } else if (command instanceof BrokerInfo) {
                handleBrokerInfo((BrokerInfo) command);
            } else if (command instanceof Response) {
                this.latest = (Command) command;
            }
        } catch (Exception e) {
            failureCause = e;
        }
    }

    @Override
    public void onException(IOException error) {
        failureCause = error;
    }

    @Override
    public void transportInterupted() {
    }

    @Override
    public void transportResumed() {
    }

    public WireFormatInfo getRemoteWireFormatInfo() {
        return this.remoteWireformatInfo;
    }

    public BrokerInfo getRemoteBrokerInfo() {
        return this.remoteInfo;
    }

    public Command getLastCommandReceived() {
        return this.latest;
    }

    public boolean isFailed() {
        return this.failureCause != null;
    }

    protected void handleWireFormatInfo(WireFormatInfo info) throws Exception {
        LOG.info("Received remote WireFormatInfo: {}", info);
        this.remoteWireformatInfo = info;

        if (LOG.isDebugEnabled()) {
            LOG.debug(this + " before negotiation: " + wireFormat);
        }
        if (!info.isValid()) {
            onException(new IOException("Remote wire format magic is invalid"));
        } else if (info.getVersion() < getOpenWireVersion()) {
            onException(new IOException("Remote wire format (" + info.getVersion() +
                        ") is lower the minimum version required (" + getOpenWireVersion() + ")"));
        }

        wireFormat.renegotiateWireFormat(info);
        if (LOG.isDebugEnabled()) {
            LOG.debug(this + " after negotiation: " + wireFormat);
        }

        connected.countDown();
    }

    protected void handleKeepAliveInfo(KeepAliveInfo info) throws Exception {
        LOG.info("Received remote KeepAliveInfo: {}", info);
        if (info.isResponseRequired()) {
            KeepAliveInfo response = new KeepAliveInfo();
            transport.oneway(response);
        }
    }

    protected void handleBrokerInfo(BrokerInfo info) throws Exception {
        LOG.info("Received remote BrokerInfo: {}", info);
        this.remoteInfo = info;
    }
}