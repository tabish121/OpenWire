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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Test;

/**
 *
 */
public class OpenWireStreamMessageTest {

    @Test
    public void testGetDataStructureType() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        assertEquals(msg.getDataStructureType(), CommandTypes.OPENWIRE_STREAM_MESSAGE);
    }

    @Test
    public void testReadBoolean() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(msg.readBoolean());
            msg.reset();
            assertTrue(msg.readString().equals("true"));
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testreadByte() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            byte test = (byte) 4;
            msg.writeByte(test);
            msg.reset();
            assertTrue(msg.readByte() == test);
            msg.reset();
            assertTrue(msg.readShort() == test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Byte(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadShort() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            short test = (short) 4;
            msg.writeShort(test);
            msg.reset();
            assertTrue(msg.readShort() == test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Short(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadChar() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            char test = 'z';
            msg.writeChar(test);
            msg.reset();
            assertTrue(msg.readChar() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Character(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadInt() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            int test = 4;
            msg.writeInt(test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Integer(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadLong() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            long test = 4L;
            msg.writeLong(test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Long.valueOf(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg = new OpenWireStreamMessage();
            msg.writeObject(new Long("1"));
            // reset so it's readable now
            msg.reset();
            assertEquals(new Long("1"), msg.readObject());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadFloat() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            float test = 4.4f;
            msg.writeFloat(test);
            msg.reset();
            assertTrue(msg.readFloat() == test);
            msg.reset();
            assertTrue(msg.readDouble() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Float(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadDouble() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            double test = 4.4d;
            msg.writeDouble(test);
            msg.reset();
            assertTrue(msg.readDouble() == test);
            msg.reset();
            assertTrue(msg.readString().equals(new Double(test).toString()));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadString() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            byte testByte = (byte) 2;
            msg.writeString(new Byte(testByte).toString());
            msg.reset();
            assertTrue(msg.readByte() == testByte);
            msg.clearBody();
            short testShort = 3;
            msg.writeString(new Short(testShort).toString());
            msg.reset();
            assertTrue(msg.readShort() == testShort);
            msg.clearBody();
            int testInt = 4;
            msg.writeString(new Integer(testInt).toString());
            msg.reset();
            assertTrue(msg.readInt() == testInt);
            msg.clearBody();
            long testLong = 6L;
            msg.writeString(new Long(testLong).toString());
            msg.reset();
            assertTrue(msg.readLong() == testLong);
            msg.clearBody();
            float testFloat = 6.6f;
            msg.writeString(new Float(testFloat).toString());
            msg.reset();
            assertTrue(msg.readFloat() == testFloat);
            msg.clearBody();
            double testDouble = 7.7d;
            msg.writeString(new Double(testDouble).toString());
            msg.reset();
            assertTrue(msg.readDouble() == testDouble);
            msg.clearBody();
            msg.writeString("true");
            msg.reset();
            assertTrue(msg.readBoolean());
            msg.clearBody();
            msg.writeString("a");
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException e) {
            }
            msg.clearBody();
            msg.writeString("777");
            msg.reset();
            try {
                msg.readBytes(new byte[3]);
                fail("Should have thrown exception");
            } catch (MessageFormatException e) {
            }

        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBigString() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            // Test with a 1Meg String
            StringBuffer bigSB = new StringBuffer(1024 * 1024);
            for (int i = 0; i < 1024 * 1024; i++) {
                bigSB.append('a' + i % 26);
            }
            String bigString = bigSB.toString();

            msg.writeString(bigString);
            msg.reset();
            assertEquals(bigString, msg.readString());

        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBytes() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            byte[] test = new byte[50];
            for (int i = 0; i < test.length; i++) {
                test[i] = (byte) i;
            }
            msg.writeBytes(test);
            msg.reset();
            byte[] valid = new byte[test.length];
            msg.readBytes(valid);
            for (int i = 0; i < valid.length; i++) {
                assertTrue(valid[i] == test[i]);
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readString();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadObject() {
        OpenWireStreamMessage msg = new OpenWireStreamMessage();
        try {
            byte testByte = (byte) 2;
            msg.writeByte(testByte);
            msg.reset();
            assertTrue(((Byte) msg.readObject()).byteValue() == testByte);
            msg.clearBody();

            short testShort = 3;
            msg.writeShort(testShort);
            msg.reset();
            assertTrue(((Short) msg.readObject()).shortValue() == testShort);
            msg.clearBody();

            int testInt = 4;
            msg.writeInt(testInt);
            msg.reset();
            assertTrue(((Integer) msg.readObject()).intValue() == testInt);
            msg.clearBody();

            long testLong = 6L;
            msg.writeLong(testLong);
            msg.reset();
            assertTrue(((Long) msg.readObject()).longValue() == testLong);
            msg.clearBody();

            float testFloat = 6.6f;
            msg.writeFloat(testFloat);
            msg.reset();
            assertTrue(((Float) msg.readObject()).floatValue() == testFloat);
            msg.clearBody();

            double testDouble = 7.7d;
            msg.writeDouble(testDouble);
            msg.reset();
            assertTrue(((Double) msg.readObject()).doubleValue() == testDouble);
            msg.clearBody();

            char testChar = 'z';
            msg.writeChar(testChar);
            msg.reset();
            assertTrue(((Character) msg.readObject()).charValue() == testChar);
            msg.clearBody();

            byte[] data = new byte[50];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            msg.writeBytes(data);
            msg.reset();
            byte[] valid = (byte[]) msg.readObject();
            assertTrue(valid.length == data.length);
            for (int i = 0; i < valid.length; i++) {
                assertTrue(valid[i] == data[i]);
            }
            msg.clearBody();
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(((Boolean) msg.readObject()).booleanValue());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testClearBody() throws JMSException {
        OpenWireStreamMessage streamMessage = new OpenWireStreamMessage();
        streamMessage.writeObject(new Long(2));
        streamMessage.clearBody();
        streamMessage.writeObject(new Long(2));
        streamMessage.reset();
        assertNotNull(streamMessage.readObject());
    }

    @Test
    public void testReset() throws JMSException {
        OpenWireStreamMessage streamMessage = new OpenWireStreamMessage();
        try {
            streamMessage.writeDouble(24.5);
            streamMessage.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        streamMessage.reset();
        try {
            assertEquals(streamMessage.readDouble(), 24.5, 0);
            assertEquals(streamMessage.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
    }

    @Test
    public void testWriteObject() {
        try {
            OpenWireStreamMessage message = new OpenWireStreamMessage();
            message.clearBody();
            message.writeObject("test");
            message.writeObject(new Character('a'));
            message.writeObject(new Boolean(false));
            message.writeObject(new Byte((byte) 2));
            message.writeObject(new Short((short) 2));
            message.writeObject(new Integer(2));
            message.writeObject(new Long(2l));
            message.writeObject(new Float(2.0f));
            message.writeObject(new Double(2.0d));
        } catch (Exception e) {
            fail(e.getMessage());
        }
        try {
            OpenWireStreamMessage message = new OpenWireStreamMessage();
            message.clearBody();
            message.writeObject(new Object());
            fail("should throw an exception");
        } catch (MessageFormatException e) {
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
