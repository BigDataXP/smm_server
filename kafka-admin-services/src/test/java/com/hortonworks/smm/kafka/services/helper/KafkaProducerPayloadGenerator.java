/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */


package com.hortonworks.smm.kafka.services.helper;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducerPayloadGenerator {

    private Type type;

    public enum Type {
        SHORT,
        BYTES,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BYTE_BUFFER,
        BYTE_ARRAY
    }

    public KafkaProducerPayloadGenerator(Type type) {
        this.type = type;
    }

    public String getSerializerClass() {
        switch (type) {
            case SHORT:
                return ShortSerializer.class.getName();
            case BYTES:
                return BytesSerializer.class.getName();
            case INT:
                return IntegerSerializer.class.getName();
            case LONG:
                return LongSerializer.class.getName();
            case FLOAT:
                return FloatSerializer.class.getName();
            case DOUBLE:
                return DoubleSerializer.class.getName();
            case STRING:
                return StringSerializer.class.getName();
            case BYTE_BUFFER:
                return ByteBufferSerializer.class.getName();
            case BYTE_ARRAY:
                return ByteArraySerializer.class.getName();
            default:
                throw new RuntimeException(String.format("Can't determine a serializer for type : %s", type));
        }
    }

    public String getDeserializerClass() {
        switch (type) {
            case SHORT:
                return ShortDeserializer.class.getName();
            case BYTES:
                return BytesDeserializer.class.getName();
            case INT:
                return IntegerDeserializer.class.getName();
            case LONG:
                return LongDeserializer.class.getName();
            case FLOAT:
                return FloatDeserializer.class.getName();
            case DOUBLE:
                return DoubleDeserializer.class.getName();
            case STRING:
                return StringDeserializer.class.getName();
            case BYTE_BUFFER:
                return ByteBufferDeserializer.class.getName();
            case BYTE_ARRAY:
                return ByteArrayDeserializer.class.getName();
            default:
                throw new RuntimeException(String.format("Can't determine a deserializer for type : %s", type));
        }
    }

    public Object getNext() {
        switch (type) {
            case SHORT:
                return new Long(ThreadLocalRandom.current().nextInt()).shortValue();
            case BYTES:
                return new Bytes(generateRandomByteArray());
            case INT:
                return ThreadLocalRandom.current().nextInt();
            case LONG:
                return ThreadLocalRandom.current().nextLong();
            case FLOAT:
                return ThreadLocalRandom.current().nextFloat();
            case DOUBLE:
                return ThreadLocalRandom.current().nextDouble();
            case STRING:
                return UUID.randomUUID().toString();
            case BYTE_BUFFER:
                return ByteBuffer.wrap(generateRandomByteArray());
            case BYTE_ARRAY:
                return generateRandomByteArray();
            default:
                throw new RuntimeException(String.format("Can't generate data for type : %s", type));
        }
    }

    private byte[] generateRandomByteArray() {
        return UUID.randomUUID().toString().getBytes();
    }
}
