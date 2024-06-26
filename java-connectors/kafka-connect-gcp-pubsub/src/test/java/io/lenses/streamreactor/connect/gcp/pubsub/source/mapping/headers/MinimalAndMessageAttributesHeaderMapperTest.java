/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.headers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;

@ExtendWith(MockitoExtension.class)
class MinimalAndMessageAttributesHeaderMapperTest {

  private static final String PUBLISH_TIME = "1955-11-12T10:04:00Z";
  private static final Instant PUBLISH_TIME_INSTANT = Instant.parse(PUBLISH_TIME);

  private static final Map<String, String> HEADERS_MAP = Map.of("attr1", "value1", "attr2", "value2");

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private PubSubMessageData pubSubMessageData;

  @Mock
  private PubsubMessage pubsubMessage;

  private MinimalAndMessageAttributesHeaderMapper minimalAndMessageAttributesHeaderMapping;

  @BeforeEach
  void setup() {
    minimalAndMessageAttributesHeaderMapping = new MinimalAndMessageAttributesHeaderMapper();
  }

  @Test
  void testMapHeaders() {

    when(pubsubMessage.getPublishTime()).thenReturn(Timestamp.newBuilder().setSeconds(PUBLISH_TIME_INSTANT
        .getEpochSecond()).build());
    when(pubsubMessage.getAttributesMap()).thenReturn(HEADERS_MAP);
    when(pubSubMessageData.getMessage()).thenReturn(pubsubMessage);
    when(pubSubMessageData.getSourcePartition().getProjectId()).thenReturn("test-project");
    when(pubSubMessageData.getSourcePartition().getTopicId()).thenReturn("test-topic");
    when(pubSubMessageData.getSourcePartition().getSubscriptionId()).thenReturn("test-subscription");

    Map<String, String> result = minimalAndMessageAttributesHeaderMapping.mapHeaders(pubSubMessageData);

    assertEquals(
        ImmutableMap.builder()
            .put("PublishTimestamp", String.valueOf(PUBLISH_TIME_INSTANT.getEpochSecond()))
            .putAll(HEADERS_MAP)
            .put("GCPProjectId", "test-project") // Include expected values from mapExtra
            .put("PubSubTopicId", "test-topic")
            .put("PubSubSubscriptionId", "test-subscription")
            .build(),
        result);
  }

}
