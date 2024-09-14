package io.dazzleduck.combiner.catalog;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CatalogImplTest {

    private static SparkSession sparkSession;

    @BeforeAll
    public static void  beforeAll() {
        sparkSession =  SparkSession
                .builder()
                .master("local").getOrCreate();
    }

    @AfterAll
    public static void afterAll() {
        sparkSession.stop();
    }

    @Test
    public void createDeleteNamespace() throws IOException {
        int result = withNewCatalog( "test", (dir, catalog) -> {
            var toCreate1 = new String[] {"test1"};
            var toCreate2 = new String[] {"test2"};
            catalog.createNamespace(toCreate2,
                    Collections.emptyMap());
            catalog.createNamespace(toCreate1, Collections.emptyMap());
            assertEquals( 2, catalog.listNamespaces().length);
            catalog.dropNamespace(toCreate2, false);
            assertEquals(1, catalog.listNamespaces().length);
            return 0;
        });
    }

    @Test
    public void createDeleteTable() throws IOException {
        int result = withNewCatalog("test", (dir, catalog) -> {
            var namespace = new String[]{"ns1"};
            catalog.createNamespace(namespace, Collections.emptyMap());
            var tableIdentifier = Identifier.of(namespace, "test_table");
            try {
                catalog.createTable(tableIdentifier, new Column[0], new Transform[0],
                            Collections.emptyMap());
            } catch (TableAlreadyExistsException | NoSuchNamespaceException e) {
                throw new RuntimeException(e);
            }
            assertEquals(1, catalog.listTables(namespace).length);
            catalog.dropTable(tableIdentifier);
            assertEquals(0, catalog.listTables(namespace).length);
            return 0;
        });
    }

    private static <T> T withTempDir( Function<String, T > fn ) throws IOException {
        var prefix = "temp";
        var tempDirWithPrefix = Files.createTempDirectory(prefix);
        return fn.apply(tempDirWithPrefix.toString());
    }

    private static <T> T withNewCatalog( String name, BiFunction<String, CatalogImpl,  T > fn ) throws IOException {
        var prefix = "temp";
        var tempDirWithPrefix = Files.createTempDirectory(prefix).toString();
        var catalog = new CatalogImpl();
        catalog.initialize(name,
                new CaseInsensitiveStringMap(Map.of("path", tempDirWithPrefix)));
        return fn.apply(tempDirWithPrefix, catalog);
    }
}
