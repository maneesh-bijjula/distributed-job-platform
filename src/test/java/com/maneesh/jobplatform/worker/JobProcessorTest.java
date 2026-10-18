package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.jobs.JobType;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class JobProcessorTest {
    private final JobProcessor processor = new JobProcessor(new ObjectMapper());

    @Test
    void countsWordsAcrossUnicodeWhitespace() {
        var result = processor.process(JobType.WORD_COUNT, "  hello\tworld\n你好\u2003friend  ");
        assertThat(result.get("wordCount").asLong()).isEqualTo(4);
    }

    @Test
    void countsUnicodeCodePointsRatherThanUtf16Units() {
        var result = processor.process(JobType.WORD_COUNT, "hi 😀");
        assertThat(result.get("characterCount").asInt()).isEqualTo(4);
        assertThat(result.get("wordCount").asInt()).isEqualTo(2);
    }

    @Test
    void hashesUtf8UsingKnownSha256Vector() {
        var result = processor.process(JobType.SHA256, "abc");
        assertThat(result.get("sha256").asText())
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void hashesUnicodeReproducibly() {
        var first = processor.process(JobType.SHA256, "你好 😀");
        assertThat(first).isEqualTo(processor.process(JobType.SHA256, "你好 😀"));
        assertThat(first.get("sha256").asText()).hasSize(64);
        assertThat(first).isNotEqualTo(processor.process(JobType.SHA256, "你好"));
    }

    @Test
    void uppercasesIndependentOfMachineLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertThat(processor.process(JobType.UPPERCASE, "idiom straße").get("text").asText())
                    .isEqualTo("IDIOM STRASSE");
        } finally {
            Locale.setDefault(previous);
        }
    }
}
