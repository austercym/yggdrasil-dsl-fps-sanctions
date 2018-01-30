package com.orwellg.yggdrasil.fps.sanctions.exception;

public class CustomerAccountDataException extends Exception {
	
	private static final long serialVersionUID = 1L;

	public CustomerAccountDataException() {}

    // Constructor that accepts a message
    public CustomerAccountDataException(String message)
    {
        super(message);
    }
}
