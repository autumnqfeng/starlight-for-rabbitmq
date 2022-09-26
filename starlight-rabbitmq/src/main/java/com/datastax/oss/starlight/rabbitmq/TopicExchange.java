/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.starlight.rabbitmq;

import com.datastax.oss.starlight.rabbitmq.metadata.BindingMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.BindingSetMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.api.MessageId;
import org.apache.qpid.server.model.LifetimePolicy;

public class TopicExchange extends AbstractExchange {

  public TopicExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.topic, durable, lifetimePolicy);
  }

  @Override
  public CompletableFuture<Void> bind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String bindingKey,
      GatewayConnection connection) {
    Set<String> matchTopics = new HashSet<>();
    vhost
        .getExchanges()
        .get(exchange)
        .getRoutingKeys()
        .forEach(
            routing -> {
              if (matchRoutingKey(routing, bindingKey)) {
                matchTopics.add(getTopicName(connection.getNamespace(), name, routing).toString());
              }
            });

    BindingSetMetadata bindings = vhost.getExchanges().get(exchange).getBindings().get(queue);
    bindings.getKeys().add(bindingKey);
    Map<String, BindingMetadata> subscriptions =
        vhost.getSubscriptions().computeIfAbsent(queue, q -> new HashMap<>());
    for (BindingMetadata bindingMetadata : subscriptions.values()) {
      if (bindingMetadata.getLastMessageId() == null
          && bindingMetadata.getExchange().equals(exchange)) {
        // There's already an active subscription
        matchTopics.remove(bindingMetadata.getTopic());
      }
    }

    List<CompletableFuture> completableFutures = new ArrayList<>();
    matchTopics.forEach(
        topic -> {
          String subscriptionName = (topic + "-" + UUID.randomUUID()).replace("/", "_");
          completableFutures.add(
              connection
                  .getGatewayService()
                  .getPulsarAdmin()
                  .topics()
                  .createSubscriptionAsync(topic, subscriptionName, MessageId.latest)
                  .thenAccept(
                      it ->
                          subscriptions.put(
                              subscriptionName,
                              new BindingMetadata(exchange, topic, subscriptionName))));
        });
    return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
  }

  public static boolean matchRoutingKey(String routingKey, String bindKey) {
    if (Strings.isBlank(routingKey) && Strings.isBlank(bindKey)) {
      return true;
    }
    if (Strings.isBlank(routingKey) || Strings.isBlank(bindKey)) {
      return false;
    }
    String[] routingKeys = routingKey.split("\\.");
    String[] bindKeys = bindKey.split("\\.");
    if (routingKeys.length != bindKeys.length) {
      return false;
    }

    boolean result = false;
    for (int i = 0; i < routingKeys.length; i++) {
      result = routingKeys[i].equals(bindKeys[i]) || "*".equals(bindKeys[i]);
    }
    return result;
  }

  @Override
  public CompletableFuture<Void> unbind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String bindingKey,
      GatewayConnection connection) {
    Map<String, BindingSetMetadata> bindings = vhost.getExchanges().get(exchange).getBindings();
    Set<String> keys = bindings.get(queue).getKeys();
    keys.remove(bindingKey);
    if (keys.isEmpty()) {
      bindings.remove(queue);
    }

    Set<String> routingKeys = vhost.getExchanges().get(exchange).getRoutingKeys();
    Map<String, BindingMetadata> queueSubscriptions = vhost.getSubscriptions().get(queue);
    List<CompletableFuture> completableFutures = new ArrayList<>();

    routingKeys.forEach(
        routingKey -> {
          if (matchRoutingKey(routingKey, bindingKey)) {
            routingKeys.remove(routingKey);
            String topic = getTopicName(vhost.getNamespace(), exchange, routingKey).toString();
            queueSubscriptions.forEach(
                (qu, sub) -> {
                  if (sub.getLastMessageId() != null
                      && sub.getExchange().equals(exchange)
                      && sub.getTopic().equals(topic)) {
                    completableFutures.add(
                        connection
                            .getGatewayService()
                            .getPulsarAdmin()
                            .topics()
                            .getLastMessageIdAsync(sub.getTopic())
                            .thenAccept(
                                lastMessageId ->
                                    sub.setLastMessageId(lastMessageId.toByteArray())));
                  }
                });
          }
        });

    return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
  }
}
