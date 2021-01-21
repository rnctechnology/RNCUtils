package com.rnctech.common;

import java.io.InputStream;
import java.util.*;

/**
 * APIs for communication with configuration service
 * Created by Zilin on 1/7/2021.
 */

public interface NRConfigAPI extends AutoCloseable {
	
	public enum PROPTYPE {USER, SOURCE, SINK, SYSTEM}

    Map<String, Set<String>> getAllProperties();

    String getProperty(String propName);

    default Map<String, String> getAllSetting() {return Collections.emptyMap();}

    default Map<String, String> getAllUserProperties(String userId) {return Collections.emptyMap();}

    Map<String, String> getSourceProperties(String sourceName);

    void updateProperties(Map<String, String> properties);
    
    void updateProperties(String name, String propVal);

    String getStatus(String id);

    void setStatus(String id, String status);
    
    default byte[] getModel(String context, String modelname) {return null;}
    
    default void uploadModel(String context, String modelname, InputStream data) {}

}
