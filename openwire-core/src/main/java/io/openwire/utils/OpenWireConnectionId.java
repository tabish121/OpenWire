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
package io.openwire.utils;

import io.openwire.commands.ConnectionId;
import io.openwire.commands.ConsumerId;
import io.openwire.commands.LocalTransactionId;
import io.openwire.commands.SessionId;
import io.openwire.commands.TransactionId;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates an ActiveMQ compatible OpenWire connection Id used to create instance
 * of ConnectionId objects and provides methods for creating OpenWireSessionId instances
 * that are children of this Connection.
 */
public class OpenWireConnectionId {

    private static final OpenWireIdGenerator idGenerator = new OpenWireIdGenerator();

    private final ConnectionId connectionId;
    private SessionId connectionSessionId;

    private final AtomicLong sessionIdGenerator = new AtomicLong(1);
    private final AtomicLong consumerIdGenerator = new AtomicLong(1);
    private final AtomicLong tempDestinationIdGenerator = new AtomicLong(1);
    private final AtomicLong localTransactionIdGenerator = new AtomicLong(1);

    /**
     * Creates a fixed OpenWire Connection Id instance.
     */
    public OpenWireConnectionId() {
        this(idGenerator.generateId());
    }

    /**
     * Creates a fixed OpenWire Connection Id instance.
     *
     * @param connectionId
     *        the set ConnectionId value that this class will use to seed new Session IDs.
     */
    public OpenWireConnectionId(String connectionId) {
        this.connectionId = new ConnectionId(connectionId);
    }

    /**
     * Creates a fixed OpenWire Connection Id instance.
     *
     * @param connectionId
     *        the set ConnectionId value that this class will use to seed new Session IDs.
     */
    public OpenWireConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * @return the SessionId used for the internal Connection Session instance.
     */
    public SessionId getConnectionSessionId() {
        if (this.connectionSessionId == null) {
            synchronized (this) {
                if (this.connectionSessionId == null) {
                    this.connectionSessionId = new SessionId(connectionId, -1);
                }
            }
        }

        return this.connectionSessionId;
    }

    /**
     * Creates a new SessionId for a Session instance that is rooted by this Connection
     *
     * @return the next logical SessionId for this ConnectionId instance.
     */
    public SessionId getNextSessionId() {
        return new SessionId(connectionId, sessionIdGenerator.getAndIncrement());
    }

    /**
     * Creates a new Transaction ID used for local transactions created from this Connection.
     *
     * @return a new TransactionId instance.
     */
    public TransactionId getNextLocalTransactionId() {
        return new LocalTransactionId(connectionId, localTransactionIdGenerator.getAndIncrement());
    }

    /**
     * Create a new Consumer Id for ConnectionConsumer instances.
     *
     * @returns a new ConsumerId valid for use in ConnectionConsumer instances.
     */
    public ConsumerId getNextConnectionConsumerId() {
        return new ConsumerId(getConnectionSessionId(), consumerIdGenerator.getAndIncrement());
    }

    /**
     * Creates a new Temporary Destination name based on the Connection ID.
     *
     * @returns a new String destination name used to create temporary destinations.
     */
    public String getNextTemporaryDestinationName() {
        return connectionId.getValue() + ":" + tempDestinationIdGenerator.getAndIncrement();
    }

    @Override
    public String toString() {
        return connectionId.toString();
    }
}
