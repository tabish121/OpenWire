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
package io.openwire.codec.v1;

import io.openwire.codec.BaseDataStreamMarshaller;
import io.openwire.codec.BooleanStream;
import io.openwire.codec.OpenWireFormat;
import io.openwire.commands.DataStructure;
import io.openwire.commands.JournalTransaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JournalTransactionMarshaller extends BaseDataStreamMarshaller {

    /**
     * Return the type of Data Structure we marshal
     *
     * @return short representation of the type data structure
     */
    @Override
    public byte getDataStructureType() {
        return JournalTransaction.DATA_STRUCTURE_TYPE;
    }

    /**
     * @return a new object instance
     */
    @Override
    public DataStructure createObject() {
        return new JournalTransaction();
    }

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o
     *        the object to un-marshal
     * @param dataIn
     *        the data input stream to build the object from
     * @throws IOException
     */
    @Override
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);

        JournalTransaction info = (JournalTransaction) o;
        info.setTransactionId((io.openwire.commands.TransactionId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setType(dataIn.readByte());
        info.setWasPrepared(bs.readBoolean());
    }

    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    @Override
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
        JournalTransaction info = (JournalTransaction) o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
        rc += tightMarshalNestedObject1(wireFormat, info.getTransactionId(), bs);
        bs.writeBoolean(info.getWasPrepared());

        return rc + 1;
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o
     *        the instance to be marshaled
     * @param dataOut
     *        the output stream
     * @throws IOException
     *         thrown if an error occurs
     */
    @Override
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);

        JournalTransaction info = (JournalTransaction) o;
        tightMarshalNestedObject2(wireFormat, info.getTransactionId(), dataOut, bs);
        dataOut.writeByte(info.getType());
        bs.readBoolean();
    }

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o
     *        the object to un-marshal
     * @param dataIn
     *        the data input stream to build the object from
     * @throws IOException
     */
    @Override
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);

        JournalTransaction info = (JournalTransaction) o;
        info.setTransactionId((io.openwire.commands.TransactionId) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setType(dataIn.readByte());
        info.setWasPrepared(dataIn.readBoolean());
    }

    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    @Override
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {
        JournalTransaction info = (JournalTransaction) o;

        super.looseMarshal(wireFormat, o, dataOut);
        looseMarshalNestedObject(wireFormat, info.getTransactionId(), dataOut);
        dataOut.writeByte(info.getType());
        dataOut.writeBoolean(info.getWasPrepared());
    }
}
