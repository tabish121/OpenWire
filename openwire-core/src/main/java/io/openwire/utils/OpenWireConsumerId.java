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

/**
 * Encapsulates an ActiveMQ compatible MessageConsumer ID using an OpenWire
 * ConsumerId generated from a parent Session instance.
 */
public class OpenWireConsumerId {

    private final OpenWireSessionId parent;
    private final ConsumerId consumerId;

    public OpenWireConsumerId(OpenWireSessionId parent, ConsumerId consumerId) {
        this.parent = parent;
        this.consumerId = consumerId;
    }

    /**
     * @return the parent OpenWireSessionId instance.
     */
    public OpenWireSessionId getParent() {
        return parent;
    }

    /**
     * @return the consumerId managed by this OpenWireConsumerId instance.
     */
    public ConsumerId getConsumerId() {
        return consumerId;
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
}
