package net.adamsmolnik.util;

/**
 * @author ASmolnik
 *
 */
public class SchedulerException extends RuntimeException {

    private static final long serialVersionUID = -6737830428408607061L;

    public SchedulerException(Throwable throwable) {
        super(throwable);
    }

    public SchedulerException(String message) {
        super(message);
    }

}
