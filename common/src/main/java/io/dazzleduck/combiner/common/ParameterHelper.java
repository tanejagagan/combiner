package io.dazzleduck.combiner.common;

import java.util.Map;

public class ParameterHelper {
    public static String getConnectionUrl(Map<String, String> parameters) {
        return parameters.get(Constants.URL_KEY);
    }
}
