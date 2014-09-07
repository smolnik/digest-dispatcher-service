package net.adamsmolnik.util;

import javax.inject.Singleton;

/**
 * @author ASmolnik
 *
 */
@Singleton
public class Log {

    public void info(String logItem) {
        System.out.println(logItem);
    }

    public void err(String logItem) {
        System.out.println(logItem);
    }

    public void err(Throwable t) {
        t.printStackTrace();
    }

}
