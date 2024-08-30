package io.dazzleduck.combiner.connector.spark;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class CombinerContainerTestUtil {

    public static final String COMBINER_IMAGE_NAME = "server:latest";

    public static GenericContainer<?> createContainer(String alias, Network network) {
        return new GenericContainer<>(COMBINER_IMAGE_NAME)
                .withExposedPorts(8080)
                .withNetwork(network)
                .withNetworkAliases(alias);
    }

    public static String getURL(GenericContainer<?> combiner){
        return String.format("http://localhost:%s/v1/q/parquet",
                combiner.getMappedPort(8080));
    }
}
