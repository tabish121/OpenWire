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
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * Base implementation of a JMS Message object.
 *
 * openwire:marshaller code="23"
 */
public class OpenWireMessage extends Message {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.OPENWIRE_MESSAGE;

    protected transient boolean useCompression;
    protected transient boolean nestedMapAndListAllowed;

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
    }

    public String getMessageIdAsString() {
        MessageId messageId = this.getMessageId();
        if (messageId == null) {
            return null;
        }
        return messageId.toString();
    }

    public byte[] getCorrelationIdAsBytes() throws JMSException {
        return encodeString(this.getCorrelationId());
    }

    public void setCorrelationIdAsBytes(byte[] correlationId) throws JMSException {
        this.setCorrelationId(decodeString(correlationId));
    }

    /**
     * Seems to be invalid because the parameter doesn't initialize MessageId
     * instance variables ProducerId and ProducerSequenceId
     *
     * @param value
     *
     * @throws JMSException if an error occurs while parsing the String to a MessageID
     */
    public void setMessageId(String value) throws JMSException {
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
            this.setMessageId((MessageId)null);
        }
    }

    /**
     * This will create an object of MessageId. For it to be valid, the instance
     * variable ProducerId and producerSequenceId must be initialized.
     *
     * @param producerId
     *        the ProducerId of the producer that sends this message.
     * @param producerSequenceId
     *        the logical producer sequence Id of this message.
     *
     * @throws JMSException if an error occurs while setting this MessageId
     */
    public void setMessageId(ProducerId producerId, long producerSequenceId) throws JMSException {
        MessageId id = null;
        try {
            id = new MessageId(producerId, producerSequenceId);
            this.setMessageId(id);
        } catch (Throwable e) {
            throw ExceptionSupport.create("Invalid message id '" + id + "', reason: " + e.getMessage(), e);
        }
    }

    public boolean propertyExists(String name) throws JMSException {
        try {
            return (this.getProperties().containsKey(name) || getProperty(name)!= null);
        } catch (IOException e) {
            throw ExceptionSupport.create(e);
        }
    }

    @SuppressWarnings("rawtypes")
    public Enumeration getPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(this.getProperties().keySet());
            return result.elements();
        } catch (IOException e) {
            throw ExceptionSupport.create(e);
        }
    }

    @Override
    public void setProperty(String name, Object value) throws JMSException {
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }

        if (value instanceof UTF8Buffer) {
            value = value.toString();
        }

        checkValidObject(value);
        super.setProperty(name, value);
    }

    /**
     * Allows for bulk set of Message properties.
     *
     * The base method does not attempt to intercept and map JMS specific properties
     * into the fields of the Message, this is left to any wrapper implementation that
     * wishes to apply JMS like behavior to the standard OpenWireMessage object.
     *
     * @param properties
     *        the properties to set on the Message instance.
     *
     * @throws JMSException if an error occurs while setting the properties.
     */
    public void setProperties(Map<String, ?> properties) throws JMSException {
        for (Map.Entry<String, ?> entry : properties.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
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

    /**
     * Method that allows an application to inform the Message instance that it is
     * about to be sent and that it should prepare its internal state for dispatch.
     *
     * @throws JMSException if an error occurs or Message state is invalid.
     */
    public void onSend() throws JMSException {
    }

    /**
     * @return whether the Message allows for Map and List elements in its properties.
     */
    public boolean isNestedMapAndListAllowed() {
        return nestedMapAndListAllowed;
    }

    /**
     * Sets whether the Message will allow for setting a Map or List instance in the
     * Message properties.  By default these elements are not allowed but can added if
     * this option is set to true.
     *
     * @param nestedMapAndListAllowed
     *        true if nested Map and List instances are allowed in Message properties.
     */
    public void setNestedMapAndListAllowed(boolean nestedMapAndListAllowed) {
        this.nestedMapAndListAllowed = nestedMapAndListAllowed;
    }

    /**
     * Sets whether the payload of the Message should be compressed.
     *
     * @param useCompression
     *        true if the binary payload should be compressed.
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * @return true if the Message will compress the byte payload.
     */
    public boolean isUseCompression() {
        return useCompression;
    }

    /**
     * @return String value that represents the MIMI type for the OpenWireMessage type.
     */
    public String getMimeType() {
        return "jms/message";
    }

    protected void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character || value instanceof String || value == null;

        if (!valid) {
            if (isNestedMapAndListAllowed()) {
                if (!(value instanceof Map || value instanceof List)) {
                    throw new MessageFormatException("Only objectified primitive objects, String, Map and List types are allowed but was: " + value + " type: " + value.getClass());
                }
            } else {
                throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
            }
        }
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
}
