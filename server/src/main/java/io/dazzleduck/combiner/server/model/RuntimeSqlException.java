package io.dazzleduck.combiner.server.model;

import java.sql.SQLException;

public class RuntimeSqlException extends RuntimeException {
    private final SQLException sqlException;
    private final RuntimeException runtimeException;


    public RuntimeSqlException(SQLException sqlException) {
        this.sqlException = sqlException;
        this.runtimeException = null;
    }

    public RuntimeSqlException(RuntimeException runtimeException) {
        this.sqlException = null;
        this.runtimeException = runtimeException;
    }

}
