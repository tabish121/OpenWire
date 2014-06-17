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

import io.openwire.commands.MessageId;
import io.openwire.commands.OpenWireDestination;
import io.openwire.commands.ProducerId;
import io.openwire.commands.ProducerInfo;
import io.openwire.commands.RemoveInfo;
import io.openwire.commands.SessionId;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates an ActiveMQ compatible OpenWire Producer ID and provides
 * functionality used to generate message IDs for the producer.
 */
public class OpenWireProducerId {

    private final ProducerId producerId;
    private final OpenWireSessionId parent;

    private final AtomicLong messageSequence = new AtomicLong(1);

    /**
     * Creates a new instance with the given parent Session Id and assigned Producer Id
     *
     * @param parent
     *        the OpenWireSessionId that is the parent of the new Producer.
     * @param producerId
     *        the ProducerId assigned to this instance.
     */
    public OpenWireProducerId(OpenWireSessionId parent, ProducerId producerId) {
        this.parent = parent;
        this.producerId = producerId;
    }

    /**
     * @return the producerId
     */
    public ProducerId getProducerId() {
        return producerId;
    }

    /**
     * @return the SessionId of this ProducerId instance.
     */
    public SessionId getSessionId() {
        return this.parent.getSessionId();
    }

    /**
     * @return the parent OpenWireSessionId
     */
    public OpenWireSessionId getParent() {
        return parent;
    }

    /**
     * Factory method used to simplify creation of MessageIds from this Producer
     *
     * @return the next logical MessageId for the producer this instance represents.
     */
    public MessageId getNextMessageId() {
        return new MessageId(producerId, messageSequence.getAndIncrement());
    }

    @Override
    public String toString() {
        return producerId.toString();
    }

    /**
     * Factory method for creating a ProducerInfo to wrap this instance's ProducerId.
     *
     * @param destination
     *        the target destination for this ProducerInfo instance.
     *
     * @return a new ProducerInfo instance that can be used to register a remote producer.
     */
    public ProducerInfo createProducerInfo(OpenWireDestination destination) {
        ProducerInfo info = new ProducerInfo(getProducerId());
        info.setDestination(destination);
        return info;
    }

    /**
     * Factory method for creating a RemoveInfo command that can be used to remove this
     * producer instance from the Broker.
     *
     * @return a new RemoveInfo instance that can remove this producer.
     */
    public RemoveInfo createRemoveInfo() {
        return new RemoveInfo(getProducerId());
    }
}
