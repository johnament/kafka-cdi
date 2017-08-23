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
package org.aerogear.kafka.cdi.extension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class VerySimpleEnvironmentResolver {

    //#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}

    public static String simpleBootstrapServerResolver(final String rawExpression) {

        if (rawExpression.startsWith("#{")) {

            final List<String> expressionParts = Arrays.asList(rawExpression.split(":"));

            final List<String> variables = expressionParts.stream()
                    .map(expression -> (expression.substring(2, expression.length() - 1)))
                    .collect(Collectors.toList());

            // check if we have host:port or just one variable:
            if (variables.size() == 1) {
                return resolve(variables.get(0));
            } else if (variables.size() == 2) {
                // we have host:port
                final StringBuilder sb = new StringBuilder()
                        .append(resolve(variables.get(0)))
                        .append(":")
                        .append(resolve(variables.get(1)));

                return sb.toString();
            }
        }
        return rawExpression;

    }

    private static String resolve(final String variable) {

        String value = System.getProperty(variable);
        if (value == null) {
            value = System.getenv(variable);
        }
        if (value == null) {
            throw new RuntimeException("Could not resolve: " + variable);
        }
        return value;
    }
}
