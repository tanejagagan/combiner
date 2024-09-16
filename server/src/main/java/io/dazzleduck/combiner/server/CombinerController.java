package io.dazzleduck.combiner.server;


import io.dazzleduck.combiner.common.Constants;
import io.dazzleduck.combiner.common.model.QueryGenerator;
import io.dazzleduck.combiner.common.ConfigParameters;
import io.dazzleduck.combiner.server.model.QueryObject;
import io.dazzleduck.combiner.server.model.RuntimeSqlException;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.server.types.files.StreamedFile;
import jakarta.inject.Named;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Controller(ServerConstants.QUERY_PATH)
public class CombinerController {

    private ExecutorService ioExecutor;

    private static Logger logger = LoggerFactory.getLogger(CombinerController.class);

    public CombinerController(@Named("io") ExecutorService executorService) {
        this.ioExecutor = executorService;
    }

    @Get(ServerConstants.SERIES_PATH)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public CompletableFuture<StreamedFile> series(
            @Header(Constants.QUERY_HEADER) String queryId,
            @NotBlank
            @Max(1024)
            @Positive int size,
            @Max(ConfigParameters.ARROW_MAX_BATCH_SIZE)
            @Min(ConfigParameters.ARROW_MIN_BATCH_SIZE)
            @Nullable
            @QueryValue("1024")
            @Positive Integer batchSize) throws IOException {
        var sql = String.format("SELECT * FROM generate_series(%s)", size);
        var bSize = batchSize != null ? batchSize : ConfigParameters.ARROW_DEFAULT_BATCH_SIZE;
        return executeAndScheduleCollect(sql, ConfigParameters.getArrowBatchSize(new HashMap<>()));
    }

    @Post( uri = ServerConstants.PARQUET_PATH,
            consumes = MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public CompletableFuture<StreamedFile> execute(
            @Header("X-QueryId") String queryId,
            @Valid @Body QueryObject queryObject) throws IOException {
        try {
            var sql = QueryGenerator.DUCK_DB.generate(queryObject.toCommonQueryObject(),
                    ServerConstants.PARQUET_FORMAT);
            var batchSize = ConfigParameters.getArrowBatchSize(queryObject.getParameters());
            return executeAndScheduleCollect(sql, batchSize);
        } catch (RuntimeException sqlGenerationException) {
            return CompletableFuture.failedFuture(new RuntimeSqlException(sqlGenerationException));
        }

    }


    private CompletableFuture<StreamedFile> executeAndScheduleCollect(String sql, int bSize) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var output = new PipedOutputStream();
                var input = new PipedInputStream(output, ServerConstants.DEFAULT_NETWORK_BUFFER);
                var streamedFile = new StreamedFile(input, MediaType.APPLICATION_OCTET_STREAM_TYPE);
                var collector = SQLExecutor.executeAndProvideCollect(sql, output, bSize);
                ioExecutor.execute(() -> collector.executeCollect());
                return streamedFile;
            } catch (SQLException e ){
                logger.error("exception processing sql ", e);
                throw new RuntimeSqlException(e);
            } catch (IOException e) {
                logger.error("exception processing sql ", e);
                throw new RuntimeException("bad sql: " + sql, e);
            }
        }, ioExecutor);
    }
}