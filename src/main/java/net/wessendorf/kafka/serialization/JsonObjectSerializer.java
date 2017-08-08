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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class JsonObjectSerializer implements Serializer<JsonObject> {

    public JsonObjectSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, JsonObject data) {
        if (data == null)
            return null;

        final ByteArrayOutputStream baso = new ByteArrayOutputStream();
        final JsonWriter writer = Json.createWriter(baso);

        try {
            writer.writeObject(data);
            writer.close();
            baso.flush();
        } catch (Exception e) {
            throw new SerializationException("Error serializing JsonObject data");
        }

        return baso.toByteArray();
    }

    @Override
    public void close() {

    }
}
