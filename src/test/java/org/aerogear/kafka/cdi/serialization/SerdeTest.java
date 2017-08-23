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

import org.aerogear.kafka.serialization.CafdiSerdes;
import org.aerogear.kafka.serialization.GenericSerializer;
import org.aerogear.kafka.serialization.JsonObjectDeserializer;
import org.aerogear.kafka.serialization.JsonObjectSerializer;
import org.aerogear.kafka.serialization.GenericDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerdeTest {
    private User octocat;
    Serializer<User> userSerializer;
    Deserializer<User> userDeserializer;

    @Before
    public void setUp() {
        octocat = new User("octocat", 209);
    }

    @Test
    public void testGenericSerializeDeserialize() {
        Serializer<User> userSerializer = new GenericSerializer<>(User.class);
        Deserializer<User> userDeserializer = new GenericDeserializer<>(User.class);

        byte[] serializedOctocat = userSerializer.serialize("topic-name", octocat);
        User deserializedOctocat = userDeserializer.deserialize("topic-name", serializedOctocat);

        assertEquals(octocat, deserializedOctocat);
    }

    @Test
    public void testJsonSerializeDeserialize() {
        Serializer<JsonObject> userSerializer = new JsonObjectSerializer();
        Deserializer<JsonObject> userDeserializer = new JsonObjectDeserializer();

        JsonObject octoObject = Json.createObjectBuilder()
                .add("username", octocat.getUsername())
                .add("age", octocat.getAge())
                .build();


        byte[] serializedOctocat = userSerializer.serialize("topic-name", octoObject);
        JsonObject deserializedOctocat = userDeserializer.deserialize("topic-name", serializedOctocat);

        assertEquals(octoObject, deserializedOctocat);
    }

    @Test
    public void testSerdeFromMethod() {
        assertTrue(CafdiSerdes.serdeFrom(String.class) instanceof Serde);
        Assert.assertEquals(CafdiSerdes.serdeFrom(JsonObject.class).serializer().getClass().getName(), "org.aerogear.kafka.serialization.JsonObjectSerializer");
        Assert.assertEquals(CafdiSerdes.serdeFrom(String.class).serializer().getClass().getName(), "org.apache.kafka.common.serialization.StringSerializer");
        Assert.assertEquals(CafdiSerdes.serdeFrom(User.class).serializer().getClass().getName(), "org.aerogear.kafka.serialization.GenericSerializer");
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