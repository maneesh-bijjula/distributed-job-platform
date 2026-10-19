package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.auth.*;
import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class KeyTokensTest {
    @Test void generatedKeysAreUniqueWellFormedAndStoredAsDigests() {
        var seen=new HashSet<String>();
        for(int i=0;i<100;i++) {
            String token=KeyTokens.generate();
            assertThat(KeyTokens.valid(token)).isTrue();
            assertThat(token).hasSize(47);
            assertThat(seen.add(token)).isTrue();
            assertThat(KeyTokens.hash(token)).hasSize(64).isNotEqualTo(token);
        }
    }
    @ParameterizedTest @NullAndEmptySource @ValueSource(strings={"bad","jpk_short","Bearer jpk_invalid","jpk_\n"})
    void malformedTokensNeverReachStorage(String token) {
        var repository=mock(AccessRepository.class);
        assertThat(new ApiKeyService(repository).authenticate(token)).isEmpty();
        verifyNoInteractions(repository);
    }
    @Test void missingOrMalformedBootstrapCredentialsCannotInitializeAnEmptyService() {
        var repository=mock(AccessRepository.class);
        var service=new ApiKeyService(repository);
        assertThatThrownBy(() -> service.bootstrap("")).isInstanceOf(IllegalStateException.class).hasMessageContaining("No active operator");
        assertThatThrownBy(() -> service.bootstrap("private invalid credential")).isInstanceOf(IllegalStateException.class)
                .hasMessageNotContaining("private invalid credential");
    }
}
