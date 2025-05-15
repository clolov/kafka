package kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DiskCheckerManager {
    private static final Logger LOG = LoggerFactory.getLogger(DiskCheckerManager.class);

    private final int interval = 1000;
    private final List<File> dirs;
    private final DiskChecker diskChecker;
    private Runnable startupDataPlane;
    private Runnable shutdownDataPlane;
    private boolean wasShutdown;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checkTask;

    public DiskCheckerManager(List<String> logDirs,
                              final DiskChecker diskChecker,
                              Runnable startupDataPlane,
                              Runnable shutdownDataPlane) {
        this.dirs = logDirs.stream().map(File::new).toList();
        this.diskChecker = diskChecker;
        this.startupDataPlane = startupDataPlane;
        this.shutdownDataPlane = shutdownDataPlane;
        this.wasShutdown = false;
    }

    public void init() throws DiskChecker.DiskErrorException {
        checkDirs();
    }

    // start the daemon for disk monitoring
    public void start() {
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.checkTask = this.executor.scheduleAtFixedRate(this::checkDirs, interval, interval, TimeUnit.MILLISECONDS);
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
        LOG.info("Shutting down DiskCheckerManager");
        if (null != checkTask) {
            if (checkTask.cancel(true) && LOG.isDebugEnabled()) {
                LOG.debug("Failed to cancel check task in DiskCheckerManager");
            }
        }
        if (null != executor) {
            executor.shutdown();
        }
    }

    private void checkDirs() {
        for (File dir : dirs) {
            try {
                diskChecker.checkDir(dir);
                if (wasShutdown) {
                    this.startupDataPlane.run();
                    this.wasShutdown = false;
                }
            } catch (DiskChecker.DiskOutOfSpaceException e) {
                // Here we will set the value reference of the value passed to us
                if (!wasShutdown) {
                    this.shutdownDataPlane.run();
                    this.wasShutdown = true;
                }
            } catch (DiskChecker.DiskWarnThresholdException e) {
                // noop
            } catch (DiskChecker.DiskErrorException e) {
                // Here we will notify the LogDirFailureChannel
            }
        }
    }
}