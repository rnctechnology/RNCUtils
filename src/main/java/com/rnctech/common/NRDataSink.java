package com.rnctech.common;

import org.apache.avro.Schema;
import java.io.InputStream;
import java.util.stream.Stream;

/*
 * Author Zilin Chen
 * @2020.12
 */

public interface NRDataSink {
	
    public enum STATUS {INITIALIZED, STARTED, PROGRESSING, COMPLETED, FAILED, CANCELLED, COMPLETED_WITH_ERRORS, COMPLETED_WITH_WARNINGS};
    public enum MODE {APPEND, UPDATE, COMPLETE}
    
    void updateStatus(String key, STATUS status, String message, StackTraceElement[] trace);

    void writeData(String key, Schema schema, Stream<Object[]> data, long ts);
    
    void writeData(String key, Schema schema, Iterable<Object[]> data, long ts);
    
    default void writeFile(String fileName, String fileInfo, InputStream data, long ts) {}
}


