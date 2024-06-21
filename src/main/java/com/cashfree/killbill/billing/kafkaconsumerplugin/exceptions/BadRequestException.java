package com.cashfree.killbill.billing.kafkaconsumerplugin.exceptions;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class BadRequestException extends Exception {
    public BadRequestException(String message) {
        super(message);
    }
}
