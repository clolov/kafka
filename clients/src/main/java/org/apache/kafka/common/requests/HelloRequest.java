package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.HelloRequestData;
import org.apache.kafka.common.message.HelloResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class HelloRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<HelloRequest> {
        private final HelloRequestData data;

        public Builder(HelloRequestData data) {
            super(ApiKeys.HELLO);
            this.data = data;
        }

        @Override
        public HelloRequest build(short version) {
            return new HelloRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final HelloRequestData data;

    public HelloRequest(HelloRequestData data, short version) {
        super(ApiKeys.HELLO, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new HelloResponse(new HelloResponseData());
    }

    public static HelloRequest parse(Readable readable, short version) {
        return new HelloRequest(new HelloRequestData(readable, version), version);
    }

    @Override
    public HelloRequestData data() {
        return data;
    }

}
