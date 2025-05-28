package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.HelloResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.EnumMap;
import java.util.Map;

public class HelloResponse extends AbstractResponse {

    private final HelloResponseData data;

    public HelloResponse(HelloResponseData data) {
        super(ApiKeys.HELLO);
        this.data = data;
    }

    @Override
    public HelloResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) { }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return new EnumMap<>(Errors.class);
    }

    public static HelloResponse parse(Readable readable, short version) {
        return new HelloResponse(new HelloResponseData(readable, version));
    }

}
