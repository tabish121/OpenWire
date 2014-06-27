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

import io.openwire.commands.ConsumerId;
import io.openwire.commands.ConsumerInfo;
import io.openwire.commands.OpenWireDestination;
import io.openwire.commands.RemoveInfo;

/**
 * Encapsulates an ActiveMQ compatible MessageConsumer ID using an OpenWire
 * ConsumerId generated from a parent Session instance.
 */
public class OpenWireConsumer extends ConsumerInfo {

    private final OpenWireSession parent;

    /**
     * Creates a new OpenWireConsumer instance with the assigned consumerId.
     *
     * @param parent
     *        the OpenWireSession that created this instance.
     * @param consumerId
     *        the assigned consumer Id for this Consumer.
     */
    public OpenWireConsumer(OpenWireSession parent, ConsumerId consumerId) {
        super(consumerId);
        this.parent = parent;
    }

    /**
     * Creates a new OpenWireConsumer from the given ConsumerInfo instance.
     *
     * @param parent
     *        the OpenWireSession that created this instance.
     * @param consumerInfo
     *        the ConsumerInfo instance used to populate this one.
     */
    public OpenWireConsumer(OpenWireSession parent, ConsumerInfo consumerInfo) {
        this.parent = parent;
        consumerInfo.copy(this);
    }

    /**
     * @return the parent OpenWireSessionId instance.
     */
    public OpenWireSession getParent() {
        return parent;
    }

    /**
     * @return the next logical delivery Id for messages dispatched by the consumer.
     */
    public long getNextDeliveryId() {
        return parent.getNextDeliveryId();
    }

    @Override
    public String toString() {
        return consumerId.toString();
    }

    /**
     * Factory method for creating a ConsumerInfo to wrap this instance's ConsumerId.
     *
     * @param destination
     *        the target destination for this ProducerInfo instance.
     *
     * @return a new ConsumerInfo instance that can be used to register a remote Consumer.
     */
    public ConsumerInfo createConsumerInfo(OpenWireDestination destination) {
        this.setDestination(destination);
        return this.copy();
    }

    /**
     * Factory method for creating a RemoveInfo command that can be used to remove this
     * consumer instance from the Broker.
     *
     * @return a new RemoveInfo instance that can remove this consumer.
     */
    public RemoveInfo createRemoveInfo() {
        return new RemoveInfo(getConsumerId());
    }
}
