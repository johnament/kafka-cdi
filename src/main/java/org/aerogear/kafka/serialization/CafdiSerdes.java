/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.serialization;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;

/**
 * Extension to normal Kafka Serdes, for (de)serialization of payloads and keys on kafka records.
 */
public class CafdiSerdes extends Serdes {

    private static final Logger logger = LoggerFactory.getLogger(CafdiSerdes.class);

    /*
     * A serde for nullable {@code JsonObject} type.
     */
    static public Serde<JsonObject> JsonObject() {
        return new JsonObjectSerde();
    }

    /*
     * A generic serde for objects of type T.
     */
    static public <T> Serde<T> Generic(Class<T> type) {
        return new GenericSerde(type);
    }

    static public <T> Serde<T> serdeFrom(Class<T> type) {
        if (JsonObject.class.isAssignableFrom(type)) {
            return (Serde<T>) JsonObject();
        }

        // look up default Kafka SerDes
        // if the class type is not supported an exception is thrown
        try {
            return Serdes.serdeFrom(type);
        }
        // If an exception is thrown, use custom generic serdes
        catch (IllegalArgumentException e) {
            logger.warn("Class type is not supported. Using generic serdes");
            return (Serde<T>) Generic(type);
        }
    }

    static public final class JsonObjectSerde extends WrapperSerde<JsonObject> {
        public JsonObjectSerde() {
            super(new JsonObjectSerializer(), new JsonObjectDeserializer());
        }
    }

    static public final class GenericSerde<T> extends WrapperSerde<T> {
        public GenericSerde(Class<T> type) {
            super(new GenericSerializer<T>(type), new GenericDeserializer<T>(type));
        }
    }
}