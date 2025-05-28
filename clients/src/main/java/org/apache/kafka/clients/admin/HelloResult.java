package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

/**
 * The result of the {@link Admin#hello()} call.
 */
public class HelloResult {
    final KafkaFuture<String> future;

    HelloResult(KafkaFuture<String> future) {
        this.future = future;
    }

    public KafkaFuture<String> hello() {
        return future;
    }
}
