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
package net.wessendorf.beans;

import net.wessendorf.beans.mock.MessageReceiver;
import net.wessendorf.kafka.cdi.annotation.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static net.wessendorf.kafka.cdi.ServiceInjectionTest.TOPIC_NAME;

public class KafkaMessageListener {

    @Inject
    private MessageReceiver receiver;

    private final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

    @Consumer(
            topics = TOPIC_NAME,
            groupId = TOPIC_NAME+"_annotation",
            consumerRebalanceListener = MyConsumerRebalanceListener.class
    )
    public void onMessage(final String simplePayload) {
        logger.trace("Got {} ", simplePayload);
        receiver.ack();
    }
}
