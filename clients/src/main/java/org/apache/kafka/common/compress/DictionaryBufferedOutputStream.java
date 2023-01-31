package org.apache.kafka.common.compress;

import com.github.luben.zstd.ZstdDictTrainer;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

public class DictionaryBufferedOutputStream extends FilterOutputStream {
    protected byte buf[];

    protected int count;

    protected Optional<ZstdDictTrainer> maybeTrainer;

    protected long total;

    public DictionaryBufferedOutputStream(OutputStream out, int size, Optional<ZstdDictTrainer> maybeTrainer) {
        super(out);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buf = new byte[size];
        this.maybeTrainer = maybeTrainer;
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            maybeTrainer.map(trainer -> {
                boolean isSuccess = trainer.addSample(buf);
                if (!isSuccess) {
                    System.out.println(String.format("Trying to write %d, already at %d", buf.length, total));
                } else {
                    total += buf.length;
                }
                return isSuccess;
            });
            out.write(buf, 0, count);
            count = 0;
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    @Override
    public synchronized void write(byte b[], int off, int len) throws IOException {
        if (len >= buf.length) {
            flushBuffer();
            maybeTrainer.map(trainer -> {
                boolean isSuccess = trainer.addSample(b);
                if (!isSuccess) {
                    System.out.println(String.format("Trying to write %d, already at %d", b.length, total));
                } else {
                    total += b.length;
                }
                return isSuccess;
            });
            out.write(b, off, len);
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    @Override
    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
}
