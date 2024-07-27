package io.dazzleduck.combiner.connector.spark;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class CombinerContainerTestUtil {

    public static GenericContainer<?> createContainer(String alias, Network network) {
        return new GenericContainer<>("combiner:latest")
                .withExposedPorts(8080)
                .withNetwork(network)
                .withNetworkAliases(alias);
    }

    public static String getURL(GenericContainer<?> combiner){
        return String.format("http://localhost:%s/v1/q/parquet",
                combiner.getMappedPort(8080));
    }
}
