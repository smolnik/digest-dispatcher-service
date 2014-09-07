package net.adamsmolnik.control.fallback;

import java.util.concurrent.TimeUnit;

/**
 * @author ASmolnik
 *
 */
public interface ServerInstance {

    String getId();

    String getPublicIpAddress();

    void scheduleCleanup(int delay, TimeUnit unit);

}
