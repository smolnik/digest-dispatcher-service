package net.adamsmolnik.util;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.inject.Singleton;

/**
 * @author ASmolnik
 *
 */
@Singleton
public class Scheduler {

    private final int corePoolSize = 20;

    private final ScheduledExecutorService tasksExecutor = Executors.newScheduledThreadPool(corePoolSize);

    public <T> T scheduleAndWaitFor(Supplier<Optional<T>> task, int period, int timeout, TimeUnit unit) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<T> resultRef = new AtomicReference<>();
        AtomicReference<SchedulerException> exceptionCaught = new AtomicReference<>();
        Object taskGuard = new Object();
        ScheduledFuture<?> sf = tasksExecutor.scheduleWithFixedDelay(() -> {
            try {
                synchronized (taskGuard) {
                    if (latch.getCount() > 0) {
                        Optional<T> resultAsOptinal = task.get();
                        if (resultAsOptinal.isPresent()) {
                            resultRef.set(resultAsOptinal.get());
                            latch.countDown();
                        }
                    }
                }
            } catch (Exception ex) {
                exceptionCaught.set(new SchedulerException(ex));
                latch.countDown();
            }

        }, 0, period, unit);
        try {
            latch.await(timeout, unit);
            sf.cancel(true);
            SchedulerException caughtException = exceptionCaught.get();
            if (caughtException != null) {
                throw caughtException;
            }

            T result = resultRef.get();
            if (result != null) {
                return result;
            }
        } catch (CancellationException cex) {
            // deliberately ignored - has already been cancelled
        } catch (InterruptedException ex) {
            throw new SchedulerException(ex);
        } finally {
            sf.cancel(true);
        }
        throw new TimeoutException("Possibly timeout happened after " + timeout + " " + unit);
    }

    public void schedule(Runnable task, int delay, TimeUnit unit) {
        tasksExecutor.schedule(task, delay, unit);
    }

    public void shutdown() {
        tasksExecutor.shutdownNow();
    }

}
