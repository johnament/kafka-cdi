/**
 * Copyright (C) 2017 Matthias Wessendorf.
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
package net.wessendorf.kafka.serialization;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import javax.json.JsonObject;

/**
 * Extension to normal Kafka Serdes, for (de)serialization of payloads and keys on kafka records.
 */
public class CafdiSerdes extends Serdes {
    /*
     * A serde for nullable {@code JsonObject} type.
     */
    static public Serde<JsonObject> JsonObject() {
        return new JsonObjectSerde();
    }


    static public final class JsonObjectSerde extends WrapperSerde<JsonObject> {
        public JsonObjectSerde() {
            super(new JsonObjectSerializer(), new JsonObjectDeserializer());
        }
    }

    static public <T> Serde<T> serdeFrom(Class<T> type) {
        if (JsonObject.class.isAssignableFrom(type)) {
            return (Serde<T>) JsonObject();
        }

        // delegate to look up default Kafka SerDes:
        return Serdes.serdeFrom(type);
    }

}
