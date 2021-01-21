package com.rnctech.common;

/**
 * Created by Zilin on 12/17/2020.
 */

public class NRStackTrace {
    public NRStackTrace() {}
    public NRStackTrace(StackTraceElement element) {
        this(element.getClassName(), element.getMethodName(), element.getFileName(), element.getLineNumber());
    }

    public NRStackTrace(String declaringClass, String methodName, String fileName, int lineNumber) {
        this.declaringClass = declaringClass;
        this.methodName = methodName;
        this.fileName = fileName;
        this.lineNumber = lineNumber;
    }

    public String declaringClass;
    public String methodName;
    public String fileName;
    public int lineNumber;
}
