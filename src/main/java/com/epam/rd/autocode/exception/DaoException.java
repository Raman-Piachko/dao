package com.epam.rd.autocode.exception;

public class DaoException extends RuntimeException {
    public DaoException(String message, Exception e) {
        super(message, e);
    }
}