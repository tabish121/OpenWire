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
import io.openwire.commands.ConnectionInfo;
import io.openwire.commands.ConsumerId;
import io.openwire.commands.LocalTransactionId;
import io.openwire.commands.RemoveInfo;
import io.openwire.commands.SessionId;
import io.openwire.commands.TransactionId;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates an ActiveMQ compatible OpenWire connection Id used to create instance
 * of ConnectionId objects and provides methods for creating OpenWireSession instances
 * that are children of this Connection.
 */
public class OpenWireConnection {

    private static final OpenWireIdGenerator idGenerator = new OpenWireIdGenerator();

    private final ConnectionId connectionId;
    private SessionId connectionSessionId;

    private final AtomicLong sessionIdGenerator = new AtomicLong(1);
    private final AtomicLong consumerIdGenerator = new AtomicLong(1);
    private final AtomicLong tempDestinationIdGenerator = new AtomicLong(1);
    private final AtomicLong localTransactionIdGenerator = new AtomicLong(1);

    private String clientId;
    private String clientIp;
    private String userName;
    private String password;
    private boolean manageable;
    private boolean faultTolerant = false;

    /**
     * Creates a fixed OpenWire Connection Id instance.
     */
    public OpenWireConnection() {
        this(idGenerator.generateId());
    }

    /**
     * Creates a fixed OpenWire Connection Id instance.
     *
     * @param connectionId
     *        the set ConnectionId value that this class will use to seed new Session IDs.
     */
    public OpenWireConnection(String connectionId) {
        this.connectionId = new ConnectionId(connectionId);
    }

    /**
     * Creates a fixed OpenWire Connection Id instance.
     *
     * @param connectionId
     *        the set ConnectionId value that this class will use to seed new Session IDs.
     */
    public OpenWireConnection(ConnectionId connectionId) {
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

    /**
     * Factory method for creating a ConnectionInfo command that contains the connection
     * ID from this OpenWireConnection instance.
     *
     * @return a new ConnectionInfo that contains the proper connection Id.
     */
    public ConnectionInfo createConnectionInfo() {
        ConnectionInfo info = new ConnectionInfo(getConnectionId());

        info.setClientId(getClientId());
        info.setClientIp(getClientIp());
        info.setUserName(getUserName());
        info.setPassword(getPassword());
        info.setManageable(isManageable());
        info.setFaultTolerant(isFaultTolerant());

        return info;
    }

    /**
     * Factory method for creating a suitable RemoveInfo command that can be used to remove
     * this connection from a Broker.
     *
     * @return a new RemoveInfo that properly references this connection's Id.
     */
    public RemoveInfo createRemoveInfo() {
        return new RemoveInfo(getConnectionId());
    }

    /**
     * Factory method for OpenWireSession instances
     *
     * @return a new OpenWireSession with the next logical session ID for this connection.
     */
    public OpenWireSession createOpenWireSession() {
        return new OpenWireSession(connectionId, sessionIdGenerator.getAndIncrement());
    }

    /**
     * @return the client ID that the connection is assigned.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the client ID that the connection is assigned.
     *
     * @param clientId
     *        the clientId to set
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return the IP Address for the connection being created.
     */
    public String getClientIp() {
        return clientIp;
    }

    /**
     * Sets the IP Address that the connection is using to communicate with the
     * remote broker instance.  This is generally the host name of the client.
     *
     * @param clientIp
     *        the clientIp to set
     */
    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    /**
     * @return the User Name used to authenticate this connection.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the User Name used for Authentication and Authorization of this Connection.
     *
     * @param userName
     *        the userName to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return the password assigned to this connection.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password used in combination with the set User Name value.
     *
     * @param password
     *        the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the manageable flag for this connection.
     */
    public boolean isManageable() {
        return manageable;
    }

    /**
     * Sets whether this Connection is manageable by the remote Broker.
     *
     * @param manageable
     *        configures whether this connection is marked as managable.
     */
    public void setManageable(boolean manageable) {
        this.manageable = manageable;
    }

    /**
     * @return the faultTolerant flag for this connection.
     */
    public boolean isFaultTolerant() {
        return faultTolerant;
    }

    /**
     * Sets whether this connection is fault tolerant and will try to reconnect on
     * loss of connectivity with the remote broker.
     *
     * @param faultTolerant
     *        the faultTolerant state of this connection.
     */
    public void setFaultTolerant(boolean faultTolerant) {
        this.faultTolerant = faultTolerant;
    }
}
