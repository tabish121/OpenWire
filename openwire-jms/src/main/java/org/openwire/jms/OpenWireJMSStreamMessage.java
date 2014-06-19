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
package org.openwire.jms;

import io.openwire.commands.OpenWireStreamMessage;

import javax.jms.JMSException;
import javax.jms.StreamMessage;

/**
 * Wrapper class that provides StreamMessage compliant mappings to the OpenWireStreamMessage
 */
public class OpenWireJMSStreamMessage extends OpenWireJMSMessage implements StreamMessage {

    private final OpenWireStreamMessage message;

    /**
     * Creates a new instance that wraps a new OpenWireMessage instance.
     */
    public OpenWireJMSStreamMessage() {
        this(new OpenWireStreamMessage());
    }

    /**
     * Creates a new instance that wraps the given OpenWireMessage
     *
     * @param message
     *        the OpenWireMessage to wrap.
     */
    public OpenWireJMSStreamMessage(OpenWireStreamMessage message) {
        this.message = message;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public byte readByte() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public short readShort() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public char readChar() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int readInt() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long readLong() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public float readFloat() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double readDouble() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String readString() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Object readObject() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeByte(byte value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeShort(short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeChar(char value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeInt(int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeLong(long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeFloat(float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeDouble(double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeString(String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeObject(Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void reset() throws JMSException {
        message.reset();
    }
}
