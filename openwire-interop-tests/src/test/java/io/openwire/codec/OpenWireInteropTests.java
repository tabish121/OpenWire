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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.openwire.commands.ConnectionInfo;
import io.openwire.commands.ConsumerInfo;
import io.openwire.commands.OpenWireTopic;
import io.openwire.commands.ProducerInfo;
import io.openwire.utils.OpenWireConnectionId;
import io.openwire.utils.OpenWireConsumerId;
import io.openwire.utils.OpenWireProducerId;
import io.openwire.utils.OpenWireSessionId;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public abstract class OpenWireInteropTests extends OpenWireInteropTestSupport {

    @Rule public TestName name = new TestName();

    protected OpenWireConnectionId connectionId;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connectionId = new OpenWireConnectionId();
    }

    @Test(timeout = 60000)
    public void testCanConnect() throws Exception {
        connect();
        assertTrue(awaitConnected(10, TimeUnit.SECONDS));
        assertEquals(getOpenWireVersion(), getRemoteWireFormatInfo().getVersion());
    }

    @Test(timeout = 60000)
    public void testCreateConnection() throws Exception {
        connect();
        assertTrue(awaitConnected(10, TimeUnit.SECONDS));
        assertTrue(request(createConnectionInfo(), 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testCreateSession() throws Exception {
        connect();
        assertTrue(awaitConnected(10, TimeUnit.SECONDS));
        assertTrue(request(createConnectionInfo(), 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getCurrentConnectionsCount());
        OpenWireSessionId sessionId = connectionId.createOpenWireSessionId();
        assertTrue(request(sessionId.createSessionInfo(), 10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testCreateProducer() throws Exception {
        connect();
        assertTrue(awaitConnected(10, TimeUnit.SECONDS));
        assertTrue(request(createConnectionInfo(), 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getCurrentConnectionsCount());

        OpenWireSessionId sessionId = connectionId.createOpenWireSessionId();
        assertTrue(request(sessionId.createSessionInfo(), 10, TimeUnit.SECONDS));
        OpenWireProducerId producerId = sessionId.createOpenWireProducerId();

        ProducerInfo info = producerId.createProducerInfo(new OpenWireTopic(name.getMethodName() + "-Topic"));
        info.setDispatchAsync(false);
        assertTrue(request(info, 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getTopicProducers().length);

        assertTrue(request(producerId.createRemoveInfo(), 10, TimeUnit.SECONDS));
        assertEquals(0, brokerService.getAdminView().getTopicProducers().length);
    }

    @Test(timeout = 60000)
    public void testCreateConsumer() throws Exception {
        connect();
        assertTrue(awaitConnected(10, TimeUnit.SECONDS));
        assertTrue(request(createConnectionInfo(), 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getCurrentConnectionsCount());

        OpenWireSessionId sessionId = connectionId.createOpenWireSessionId();
        assertTrue(request(sessionId.createSessionInfo(), 10, TimeUnit.SECONDS));
        OpenWireConsumerId consumerId = sessionId.createOpenWireConsumerId();

        ConsumerInfo info = consumerId.createConsumerInfo(new OpenWireTopic(name.getMethodName() + "-Topic"));
        info.setDispatchAsync(false);
        assertTrue(request(info, 10, TimeUnit.SECONDS));
        assertEquals(1, brokerService.getAdminView().getTopicSubscribers().length);

        assertTrue(request(consumerId.createRemoveInfo(), 10, TimeUnit.SECONDS));
        assertEquals(0, brokerService.getAdminView().getTopicSubscribers().length);
    }

    protected ConnectionInfo createConnectionInfo() {
        ConnectionInfo info = new ConnectionInfo(connectionId.getConnectionId());
        info.setManageable(false);
        info.setFaultTolerant(false);
        info.setClientId(name.getMethodName());
        return info;
    }
}
