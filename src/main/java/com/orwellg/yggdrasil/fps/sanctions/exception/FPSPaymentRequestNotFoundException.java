package com.orwellg.yggdrasil.fps.sanctions.exception;

public class FPSPaymentRequestNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public FPSPaymentRequestNotFoundException() {}

    // Constructor that accepts a message
    public FPSPaymentRequestNotFoundException(String trn)
    {
        super (String.format("Not found fps payment request for payment id %s",trn));

    }
}
