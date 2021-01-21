package com.rnctech.common;

/**
 * Created by Zilin on 12/17/2020.
 */

public class NRException extends RuntimeException {
    public NRException() {
    }

    public NRException(String message) {
        super(message);
    }

    public NRException(String message, Throwable cause) {
        super(message, cause);
    }

    public NRException(Throwable cause) {
        super(cause);
    }

    public NRException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
