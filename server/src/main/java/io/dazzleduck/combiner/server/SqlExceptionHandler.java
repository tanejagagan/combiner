package io.dazzleduck.combiner.server;

import io.dazzleduck.combiner.server.model.RuntimeSqlException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Singleton;

@Produces
@Singleton
@Requires(classes = {RuntimeSqlException.class, ExceptionHandler.class})
public class SqlExceptionHandler implements ExceptionHandler<RuntimeSqlException, HttpResponse> {

    @Override
    public HttpResponse handle(HttpRequest request, RuntimeSqlException exception) {
        var msg = exception.getMessage();
        return HttpResponse.badRequest(msg);
    }
}
