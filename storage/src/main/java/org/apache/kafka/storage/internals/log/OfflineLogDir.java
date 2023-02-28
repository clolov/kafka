package org.apache.kafka.storage.internals.log;

public class OfflineLogDir {
    private final String logDir;
    private final OfflineLogDirState state;

    public OfflineLogDir(String logDir, OfflineLogDirState state) {
        this.logDir = logDir;
        this.state = state;
    }

    public String getLogDir() {
        return this.logDir;
    }

    public OfflineLogDirState getState() {
        return this.state;
    }
}
