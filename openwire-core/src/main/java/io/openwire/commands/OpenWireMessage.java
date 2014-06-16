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
package io.openwire.commands;

import io.openwire.utils.ExceptionSupport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * Base implementation of a JMS Message object.
 *
 * openwire:marshaller code="23"
 */
public class OpenWireMessage extends Message {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.OPENWIRE_MESSAGE;
    public static final String DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";
    public static final String BROKER_PATH_PROPERTY = "JMSActiveMQBrokerPath";

    private static final Map<String, PropertySetter> JMS_PROPERTY_SETERS = new HashMap<String, PropertySetter>();

    protected transient boolean useCompression;

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public Message copy() {
        OpenWireMessage copy = new OpenWireMessage();
        copy(copy);
        return copy;
    }

    protected void copy(OpenWireMessage copy) {
        copy.useCompression = useCompression;
        super.copy(copy);
    }

    @Override
    public int hashCode() {
        MessageId id = getMessageId();
        if (id != null) {
            return id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        OpenWireMessage msg = (OpenWireMessage) o;
        MessageId oMsg = msg.getMessageId();
        MessageId thisMsg = this.getMessageId();
        return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
    }

    @Override
    public void clearBody() throws JMSException {
        setContent(null);
        readOnlyBody = false;
    }

    public String getJMSMessageID() {
        MessageId messageId = this.getMessageId();
        if (messageId == null) {
            return null;
        }
        return messageId.toString();
    }

    /**
     * Seems to be invalid because the parameter doesn't initialize MessageId
     * instance variables ProducerId and ProducerSequenceId
     *
     * @param value
     * @throws JMSException
     */
    public void setJMSMessageID(String value) throws JMSException {
        if (value != null) {
            try {
                MessageId id = new MessageId(value);
                this.setMessageId(id);
            } catch (NumberFormatException e) {
                // we must be some foreign JMS provider or strange user-supplied
                // String
                // so lets set the IDs to be 1
                MessageId id = new MessageId();
                id.setTextView(value);
                this.setMessageId(id);
            }
        } else {
            this.setMessageId(null);
        }
    }

    /**
     * This will create an object of MessageId. For it to be valid, the instance
     * variable ProducerId and producerSequenceId must be initialized.
     *
     * @param producerId
     * @param producerSequenceId
     * @throws JMSException
     */
    public void setJMSMessageID(ProducerId producerId, long producerSequenceId) throws JMSException {
        MessageId id = null;
        try {
            id = new MessageId(producerId, producerSequenceId);
            this.setMessageId(id);
        } catch (Throwable e) {
            throw ExceptionSupport.create("Invalid message id '" + id + "', reason: " + e.getMessage(), e);
        }
    }

    public long getJMSTimestamp() {
        return this.getTimestamp();
    }

    public void setJMSTimestamp(long timestamp) {
        this.setTimestamp(timestamp);
    }

    public String getJMSCorrelationID() {
        return this.getCorrelationId();
    }

    public void setJMSCorrelationID(String correlationId) {
        this.setCorrelationId(correlationId);
    }

    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return encodeString(this.getCorrelationId());
    }

    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        this.setCorrelationId(decodeString(correlationId));
    }

    public String getJMSXMimeType() {
        return "jms/message";
    }

    protected static String decodeString(byte[] data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    protected static byte[] encodeString(String data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    public void setJMSReplyTo(Destination destination) throws JMSException {
        this.setReplyTo(OpenWireDestination.transform(destination));
    }

    public void setJMSDestination(Destination destination) throws JMSException {
        this.setDestination(OpenWireDestination.transform(destination));
    }

    public int getJMSDeliveryMode() {
        return this.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    public void setJMSDeliveryMode(int mode) {
        this.setPersistent(mode == DeliveryMode.PERSISTENT);
    }

    public boolean getJMSRedelivered() {
        return this.isRedelivered();
    }

    public void setJMSRedelivered(boolean redelivered) {
        this.setRedelivered(redelivered);
    }

    public String getJMSType() {
        return this.getType();
    }

    public void setJMSType(String type) {
        this.setType(type);
    }

    public long getJMSExpiration() {
        return this.getExpiration();
    }

    public void setJMSExpiration(long expiration) {
        this.setExpiration(expiration);
    }

    public int getJMSPriority() {
        return this.getPriority();
    }

    public void setJMSPriority(int priority) {
        this.setPriority((byte) priority);
    }

    @Override
    public void clearProperties() {
        super.clearProperties();
        readOnlyProperties = false;
    }

    public boolean propertyExists(String name) throws JMSException {
        try {
            return (this.getProperties().containsKey(name) || getObjectProperty(name)!= null);
        } catch (IOException e) {
            throw ExceptionSupport.create(e);
        }
    }

    @SuppressWarnings("rawtypes")
    public Enumeration getPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(this.getProperties().keySet());
            if( getRedeliveryCounter()!=0 ) {
                result.add("JMSXDeliveryCount");
            }
            if( getGroupId()!=null ) {
                result.add("JMSXGroupID");
            }
            if( getGroupId()!=null ) {
                result.add("JMSXGroupSeq");
            }
            if( getUserId()!=null ) {
                result.add("JMSXUserID");
            }
            return result.elements();
        } catch (IOException e) {
            throw ExceptionSupport.create(e);
        }
    }

    /**
     * return all property names, including standard JMS properties and JMSX properties
     * @return  Enumeration of all property names on this message
     * @throws JMSException
     */
    @SuppressWarnings("rawtypes")
    public Enumeration getAllPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(this.getProperties().keySet());
            result.addAll(JMS_PROPERTY_SETERS.keySet());
            return result.elements();
        } catch (IOException e) {
            throw ExceptionSupport.create(e);
        }
    }

    interface PropertySetter {
        void set(Message message, Object value) throws MessageFormatException;
    }

    public void setObjectProperty(String name, Object value) throws JMSException {
        setObjectProperty(name, value, true);
    }

    public void setObjectProperty(String name, Object value, boolean checkReadOnly) throws JMSException {

        if (checkReadOnly) {
            checkReadOnlyProperties();
        }
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }

        if (value instanceof UTF8Buffer) {
            value = value.toString();
        }

        checkValidObject(value);
        PropertySetter setter = JMS_PROPERTY_SETERS.get(name);

        if (setter != null && value != null) {
            setter.set(this, value);
        } else {
            try {
                this.setProperty(name, value);
            } catch (IOException e) {
                throw ExceptionSupport.create(e);
            }
        }
    }

    public void setProperties(Map<String, ?> properties) throws JMSException {
        for (Map.Entry<String, ?> entry : properties.entrySet()) {
            // Lets use the object property method as we may contain standard
            // extension headers like JMSXGroupID
            setObjectProperty(entry.getKey(), entry.getValue());
        }
    }

    protected void checkValidObject(Object value) throws MessageFormatException {

        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character || value instanceof String || value == null;

        if (!valid) {
            // TODO - AMQ does allow for messages to contain nested Maps and Lists in properties
            //        we could add support for this if we have a way to enable / disable it.
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }

    public Object getObjectProperty(String name) throws JMSException {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

        // PropertyExpression handles converting message headers to properties.
        // PropertyExpression expression = new PropertyExpression(name);
        // return expression.evaluate(this);
        return name;   // TODO trim down the JMS bits since we are behind a framework.
    }

    private void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    /**
     * Send operation event listener. Used to get the message ready to be sent.
     */
    public void onSend() throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processMessage(this);
    }

    @Override
    public void storeContent() {
    }

    @Override
    public void storeContentAndClear() {
        storeContent();
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public boolean isUseCompression() {
        return useCompression;
    }
}
