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
import org.aerogear.kafka.serialization.JsonObjectDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;

import javax.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class JsonObjectDeserializerTest {

    private Deserializer<JsonObject> deserializer;
    private User javaOctocat;

    @Before
    public void setUp() {
        deserializer = new JsonObjectDeserializer();
        javaOctocat = new User("octocat", 209);
    }

    @Test
    public void deserializeNull() {
        assertNull(deserializer.deserialize("test-topic", null));
    }

    @Test(expected = org.apache.kafka.common.errors.SerializationException.class)
    public void deserializeEmpty() {
        assertNull(deserializer.deserialize("test-topic", new byte[0]));
    }

    @Test(expected = org.apache.kafka.common.errors.SerializationException.class)
    public void deserializeWrongObject() {
        assertNull(deserializer.deserialize("test-topic", javaOctocat.toString().getBytes()));
    }

    @Test
    public void deserializeKey() {
        Map<String, Object> props = new HashMap<>();
        deserializer.configure(props, true);

        JsonObject octo = deserializer.deserialize("test-topic", "{\"username\":\"octo\",\"age\":21}".getBytes());

        assertNotNull(octo);
        assertEquals("octo", octo.getString("username"));
        assertEquals(21, octo.getJsonNumber("age").intValue());
    }

    @Test
    public void deserializeValue() {
        HashMap<String, Object> props = new HashMap<>();
        deserializer.configure(props, false);

        JsonObject octo = deserializer.deserialize("test-topic", "{\"username\":\"octo\",\"age\":21}".getBytes());

        assertNotNull(octo);
        assertEquals("octo", octo.getString("username"));
        assertEquals(21, octo.getJsonNumber("age").intValue());
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