package net.adamsmolnik.util;

/**
 * @author ASmolnik
 *
 */
public class TimeoutException extends SchedulerException {

    private static final long serialVersionUID = 17565637819347061L;

    public TimeoutException(Throwable throwable) {
        super(throwable);
    }

    public TimeoutException(String message) {
        super(message);
    }

}
