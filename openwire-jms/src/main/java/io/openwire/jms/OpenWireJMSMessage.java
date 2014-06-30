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
package io.openwire.jms;

import io.openwire.commands.OpenWireDestination;
import io.openwire.commands.OpenWireMessage;
import io.openwire.jms.utils.TypeConversionSupport;
import io.openwire.utils.ExceptionSupport;

import java.util.Enumeration;
import java.util.concurrent.Callable;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

/**
 * A JMS Message implementation that extends the basic OpenWireMessage instance
 * to enforce adherence to the JMS specification rules for the javax.jms.Message
 * type.
 */
public class OpenWireJMSMessage implements Message {

    private final OpenWireMessage message;

    private Callable<Void> acknowledgeCallback;

    /**
     * Creates a new instance that wraps a new OpenWireMessage isntance.
     */
    public OpenWireJMSMessage() {
        this(new OpenWireMessage());
    }

    /**
     * Creates a new instance that wraps the given OpenWireMessage
     *
     * @param message
     *        the OpenWireMessage to wrap.
     */
    public OpenWireJMSMessage(OpenWireMessage message) {
        this.message = message;
    }

    /**
     * @return the wrapped OpenWireMessage instance.
     */
    public OpenWireMessage getOpenWireMessage() {
        return this.message;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return message.getMessageIdAsString();
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        message.setMessageId(id);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return message.getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        message.setTimestamp(timestamp);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return message.getCorrelationIdAsBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        message.setCorrelationIdAsBytes(correlationID);
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        message.setCorrelationId(correlationID);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return message.getCorrelationId();
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return message.getReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        message.setReplyTo(OpenWireDestination.transform(replyTo));
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return message.getDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        message.setDestination(OpenWireDestination.transform(destination));
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return message.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        message.setPersistent(deliveryMode == DeliveryMode.PERSISTENT);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return message.getRedeliveryCounter() > 0;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        message.setRedelivered(true);
    }

    @Override
    public String getJMSType() throws JMSException {
        return message.getType();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        message.setType(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return message.getExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        message.setExpiration(expiration);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return message.getPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        message.setPriority((byte) priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        message.clearProperties();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return message.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);

        if (value == null) {
            return false;
        }

        Boolean rc = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }

        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Byte rc = (Byte) TypeConversionSupport.convert(value, Byte.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Short rc = (Short) TypeConversionSupport.convert(value, Short.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float) TypeConversionSupport.convert(value, Float.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double) TypeConversionSupport.convert(value, Double.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object value = null;

        // Always go first to the OpenWire Message field before checking in the
        // application properties for any other versions.
        if (name.equals("JMSXUserID")) {
            value = message.getUserId();
            if (value == null) {
                value = getObjectProperty(name);
            }
        } else {
            value = getObjectProperty(name);
        }
        if (value == null) {
            return null;
        }
        String rc = (String) TypeConversionSupport.convert(value, String.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
        }
        return rc;
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        return message.getPropertyNames();
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.call();
            } catch (Exception e) {
                throw ExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void clearBody() throws JMSException {
        message.clearBody();
    }

    /**
     * @return the acknowledge callback instance set on this message.
     */
    public Callable<Void> getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    /**
     * Sets the Callable instance that is invoked when the client calls the JMS Message
     * acknowledge method.
     *
     * @param acknowledgeCallback
     *        the acknowledgeCallback to set on this message.
     */
    public void setAcknowledgeCallback(Callable<Void> acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }
}
