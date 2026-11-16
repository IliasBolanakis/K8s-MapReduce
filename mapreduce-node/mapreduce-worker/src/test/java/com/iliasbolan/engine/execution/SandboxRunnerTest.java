package com.iliasbolan.engine.execution;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link SandboxRunner} MinIO credential fallback resolution.
 */
class SandboxRunnerTest {

    @Test
    void testResolveEnvFallback_PrefersPrimaryAccessAndSecretKeys() {
        Map<String, String> env = Map.of(
                "MINIO_ACCESS_KEY", "custom-access",
                "MINIO_ROOT_USER", "root-user",
                "MINIO_SECRET_KEY", "custom-secret",
                "MINIO_ROOT_PASSWORD", "root-password"
        );

        assertEquals("custom-access",
                SandboxRunner.resolveEnvFallback(env, "MINIO_ACCESS_KEY", "MINIO_ROOT_USER", "minioadmin"));
        assertEquals("custom-secret",
                SandboxRunner.resolveEnvFallback(env, "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD", "minioadmin"));
    }

    @Test
    void testResolveEnvFallback_FallsBackToRootCredentialsWhenPrimaryMissingOrEmpty() {
        Map<String, String> env = new HashMap<>();
        env.put("MINIO_ACCESS_KEY", "");
        env.put("MINIO_ROOT_USER", "root-user");
        env.put("MINIO_ROOT_PASSWORD", "root-password");

        assertEquals("root-user",
                SandboxRunner.resolveEnvFallback(env, "MINIO_ACCESS_KEY", "MINIO_ROOT_USER", "minioadmin"));
        assertEquals("root-password",
                SandboxRunner.resolveEnvFallback(env, "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD", "minioadmin"));
    }

    @Test
    void testResolveEnvFallback_DefaultsToMinioAdminWhenBothUnset() {
        Map<String, String> env = Map.of();

        assertEquals("minioadmin",
                SandboxRunner.resolveEnvFallback(env, "MINIO_ACCESS_KEY", "MINIO_ROOT_USER", "minioadmin"));
        assertEquals("minioadmin",
                SandboxRunner.resolveEnvFallback(env, "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD", "minioadmin"));
    }
}
