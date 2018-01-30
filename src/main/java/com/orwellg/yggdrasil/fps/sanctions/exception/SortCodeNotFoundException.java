package com.orwellg.yggdrasil.fps.sanctions.exception;

public class SortCodeNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public SortCodeNotFoundException() {}

    // Constructor that accepts a message
    public SortCodeNotFoundException(String message)
    {
        super(message);
    }
}
