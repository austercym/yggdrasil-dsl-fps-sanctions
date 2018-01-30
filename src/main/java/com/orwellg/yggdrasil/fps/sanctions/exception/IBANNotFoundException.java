package com.orwellg.yggdrasil.fps.sanctions.exception;

public class IBANNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public IBANNotFoundException() {}

    // Constructor that accepts a message
    public IBANNotFoundException(String message)
    {
        super(message);
    }
}
