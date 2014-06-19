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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Test;

public class OpenWireBytesMessageTest {

    @Test
    public void testGetDataStructureType() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        assertEquals(msg.getDataStructureType(), CommandTypes.OPENWIRE_BYTES_MESSAGE);
    }

    @Test
    public void testGetBodyLength() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        int len = 10;
        try {
            for (int i = 0; i < len; i++) {
                msg.writeLong(5L);
            }
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
        try {
            msg.reset();
            assertTrue(msg.getBodyLength() == (len * 8));
        } catch (Throwable e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBoolean() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(msg.readBoolean());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadByte() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeByte((byte) 2);
            msg.reset();
            assertTrue(msg.readByte() == 2);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUnsignedByte() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeByte((byte) 2);
            msg.reset();
            assertTrue(msg.readUnsignedByte() == 2);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadShort() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeShort((short) 3000);
            msg.reset();
            assertTrue(msg.readShort() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUnsignedShort() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeShort((short) 3000);
            msg.reset();
            assertTrue(msg.readUnsignedShort() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadChar() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeChar('a');
            msg.reset();
            assertTrue(msg.readChar() == 'a');
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadInt() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeInt(3000);
            msg.reset();
            assertTrue(msg.readInt() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadLong() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeLong(3000);
            msg.reset();
            assertTrue(msg.readLong() == 3000);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadFloat() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeFloat(3.3f);
            msg.reset();
            assertTrue(msg.readFloat() == 3.3f);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadDouble() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeDouble(3.3d);
            msg.reset();
            assertTrue(msg.readDouble() == 3.3d);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadUTF() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            String str = "this is a test";
            msg.writeUTF(str);
            msg.reset();
            assertTrue(msg.readUTF().equals(str));
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBytesbyteArray() {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            byte[] data = new byte[50];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            msg.writeBytes(data);
            msg.reset();
            byte[] test = new byte[data.length];
            msg.readBytes(test);
            for (int i = 0; i < test.length; i++) {
                assertTrue(test[i] == i);
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testWriteObject() throws JMSException {
        OpenWireBytesMessage msg = new OpenWireBytesMessage();
        try {
            msg.writeObject("fred");
            msg.writeObject(Boolean.TRUE);
            msg.writeObject(Character.valueOf('q'));
            msg.writeObject(Byte.valueOf((byte) 1));
            msg.writeObject(Short.valueOf((short) 3));
            msg.writeObject(Integer.valueOf(3));
            msg.writeObject(Long.valueOf(300L));
            msg.writeObject(new Float(3.3f));
            msg.writeObject(new Double(3.3));
            msg.writeObject(new byte[3]);
        } catch (MessageFormatException mfe) {
            fail("objectified primitives should be allowed");
        }
        try {
            msg.writeObject(new Object());
            fail("only objectified primitives are allowed");
        } catch (MessageFormatException mfe) {
        }
    }

    @Test
    public void testClearBody() throws JMSException {
        OpenWireBytesMessage bytesMessage = new OpenWireBytesMessage();
        try {
            bytesMessage.writeInt(1);
            bytesMessage.clearBody();
            bytesMessage.writeInt(1);
            bytesMessage.reset();
            bytesMessage.readInt();
        } catch (MessageNotReadableException mnwe) {
        } catch (MessageNotWriteableException mnwe) {
            fail("Should not receive an exceptions in this test.");
        }
    }

    @Test
    public void testReset() throws JMSException {
        OpenWireBytesMessage message = new OpenWireBytesMessage();
        try {
            message.writeDouble(24.5);
            message.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        message.reset();
        try {
            assertEquals(message.readDouble(), 24.5, 0);
            assertEquals(message.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
    }
}
