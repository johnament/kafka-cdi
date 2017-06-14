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
package net.wessendorf.kafka.cdi.extension;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static net.wessendorf.kafka.cdi.extension.VerySimpleEnvironmentResolver.simpleBootstrapServerResolver;
import static org.assertj.core.api.Assertions.assertThat;

public class ResolverTest {

    @Before
    public void setup() {

        System.setProperty("SINGLE", "localhost:9092");
        System.setProperty("MY_HOST", "localhost");
        System.setProperty("MY_PORT", "9092");

    }

    @Test
    public void resolveSingleExpression() {

    final String resolvedURI = simpleBootstrapServerResolver("#{SINGLE}");
    assertThat(resolvedURI).isEqualTo("localhost:9092");

    }

    @Test
    public void resolveDoubleExpression() {

        final String resolvedURI = simpleBootstrapServerResolver("#{MY_HOST}:#{MY_PORT}");
        assertThat(resolvedURI).isEqualTo("localhost:9092");
    }

    @Test
    public void resolveNoExpression() {
        final String resolvedURI = simpleBootstrapServerResolver("localhost:9092");
        assertThat(resolvedURI).isEqualTo("localhost:9092");
    }

    @Test(expected = RuntimeException.class)
    public void canNotResolve() {
        simpleBootstrapServerResolver("#{LOL}");
    }
}
