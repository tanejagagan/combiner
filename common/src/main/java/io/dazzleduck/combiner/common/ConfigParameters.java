package io.dazzleduck.combiner.common;

import java.util.Map;

public class ConfigParameters {
    public static final String BATCH_SIZE_KEY = "batchSize";
    public final static int ARROW_DEFAULT_BATCH_SIZE = 1024 * 8;
    public final static int ARROW_MAX_BATCH_SIZE = 1024 * 64 * 1;
    public final static int ARROW_MIN_BATCH_SIZE = 256;

    public static int getArrowBatchSize(Map<String, String > parameters) {

        if(parameters == null) {
            return ARROW_DEFAULT_BATCH_SIZE;
        }
        var bSizeString = parameters
                .getOrDefault(ConfigParameters.BATCH_SIZE_KEY, String.valueOf(ARROW_DEFAULT_BATCH_SIZE));
        return Math.max(ARROW_MIN_BATCH_SIZE,
                Math.min(Integer.parseInt(bSizeString), ARROW_MAX_BATCH_SIZE));
    }

}
