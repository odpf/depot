package com.gotocompany.depot.maxcompute.exception;

public class MaxComputeTableOperationException extends RuntimeException {

    public MaxComputeTableOperationException(String message, Exception e) {
        super(message, e);
    }

}
