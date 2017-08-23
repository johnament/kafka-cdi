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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.ByteArrayInputStream;
import java.util.Map;

public class JsonObjectDeserializer implements Deserializer<JsonObject> {

    public JsonObjectDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public JsonObject deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        final ByteArrayInputStream bias = new ByteArrayInputStream(data);

        try(JsonReader reader = Json.createReader(bias)) {
            return reader.readObject();
        } catch(Exception e) {
            throw new SerializationException("Unable to deserialize JsonObject", e);
        }
    }

    @Override
    public void close() {

    }
}