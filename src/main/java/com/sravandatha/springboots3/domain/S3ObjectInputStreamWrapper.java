package com.sravandatha.springboots3.domain;

import java.io.InputStream;

public record S3ObjectInputStreamWrapper(InputStream inputStream, String eTag) {
}
