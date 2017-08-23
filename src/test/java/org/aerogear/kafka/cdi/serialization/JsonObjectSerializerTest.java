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
package org.aerogear.kafka.cdi.serialization;

import com.google.common.base.Objects;
import org.aerogear.kafka.serialization.JsonObjectSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.ByteArrayInputStream;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JsonObjectSerializerTest {

    private Serializer<JsonObject> serializer;

    @Before
    public void setup() {
        serializer = new JsonObjectSerializer();
        serializer.configure(Collections.<String, Object>emptyMap(), false);
    }

    @Test
    public void serializeNull() {
        assertNull(serializer.serialize("test-topic", null));
    }

    @Test
    public void serialize() throws Exception {
        String message = "{\"username\":\"octo\",\"age\":21}";

        JsonObject object = Json.createObjectBuilder()
                .add("username", "octo")
                .add("age", 21)
                .build();

        byte[] bytes = serializer.serialize("test-topic", object);

        final ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
        final JsonReader reader = Json.createReader(bias);
        Object deserialized = reader.readObject();
        assertEquals(message, deserialized.toString());
    }


    public static class User {
        private String username;
        private int age;

        public User() {
        }

        public User(String username, int age) {
            this.username = username;
            this.age = age;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;

            final User other = (User) obj;
            return Objects.equal(this.username, other.username)
                    && Objects.equal(this.age, other.age);

        }

        public String getUsername() {
            return username;
        }

        public int getAge() {
            return age;
        }
    }
}