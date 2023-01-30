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
        ZstdDictTrainer trainer = new ZstdDictTrainer(100 * 16 * 1024, 16 * 1024);
        OutputStream stream = ZstdFactory.wrapForOutput(new ByteBufferOutputStream(ByteBuffer.allocate(512)), Optional.empty(), Optional.of(trainer));
        for (ByteBuffer batchBuffer : batchBuffers) {
            stream.write(batchBuffer.array());
        }
        dictionary = trainer.trainSamples();
    }

    @Benchmark
    public void measureCompressionThroughput() throws IOException {
        OutputStream stream = ZstdFactory.wrapForOutput(new ByteBufferOutputStream(ByteBuffer.allocate(512)));
        for (ByteBuffer batchBuffer : batchBuffers) {
            stream.write(batchBuffer.array());
        }
    }
}
