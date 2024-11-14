package com.snowflake.dlsync;

import lombok.extern.slf4j.Slf4j;
import net.snowflake.client.jdbc.internal.org.bouncycastle.util.encoders.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class Util {
    public static String getMd5Hash(String content) {
        try {
            return Hex.toHexString(MessageDigest.getInstance("MD5").digest(content.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            log.error("Hashing error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
