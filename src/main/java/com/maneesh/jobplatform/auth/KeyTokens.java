package com.maneesh.jobplatform.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

public final class KeyTokens {
    private static final SecureRandom RANDOM = new SecureRandom();
    private KeyTokens() {}
    public static String generate() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return "jpk_" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
    public static boolean valid(String token) { return token != null && token.matches("jpk_[A-Za-z0-9_-]{43}"); }
    public static String hash(String token) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.US_ASCII))); }
        catch (NoSuchAlgorithmException error) { throw new IllegalStateException("SHA-256 unavailable", error); }
    }
}
