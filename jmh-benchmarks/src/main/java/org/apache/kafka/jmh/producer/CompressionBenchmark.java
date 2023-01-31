package org.apache.kafka.jmh.producer;

import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.kafka.common.compress.ZstdFactory;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.jmh.record.BaseRecordBatchBenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
public class CompressionBenchmark extends BaseRecordBatchBenchmark {

    byte[] dictionary;

    @Override
    protected CompressionType compressionType() {
        return CompressionType.ZSTD;
    }

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        ZstdDictTrainer trainer = new ZstdDictTrainer(100 * 110 * 1024, 110 * 1024);
        try (OutputStream stream = ZstdFactory.wrapForOutput(new ByteBufferOutputStream(ByteBuffer.allocate(512)), Optional.empty(), Optional.of(trainer))) {
            System.out.println(String.format("\nbatchBuffers.length: %d", batchBuffers.length));
            for (ByteBuffer batchBuffer : batchBuffers) {
                System.out.println(batchBuffer.array().length);
                stream.write(batchBuffer.array());
            }
        }
        dictionary = trainer.trainSamples();
        System.out.println(String.format("dictionary.length: %d", dictionary.length));
    }

    @Benchmark
    public void measureCompressionThroughput() throws IOException {
        try (OutputStream stream = ZstdFactory.wrapForOutput(new ByteBufferOutputStream(ByteBuffer.allocate(512)), Optional.of(dictionary), Optional.empty())) {
            for (ByteBuffer batchBuffer : batchBuffers) {
                stream.write(batchBuffer.array());
            }
        }
    }

    // Use blackhole <- we don't leak resources
    // Close stream (done)

    // Pick a random value for each record, but let it be an integer (same type) <-- (done)
    // Pick 3 random values for keys and on each record pick one of those randomly <-- we don't use keys
    // Headers should be similar to keys (3/4 values) <-- (done)
    // Double-check whether we are filling up the dictionary <-- (done)
}
