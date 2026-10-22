package com.maneesh.jobplatform.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.HashSet;
import java.util.List;

public record BatchJobRequest(@NotNull @Size(min=1,max=100) List<@NotNull @Valid Item> items) {
    public record Item(@NotBlank @Pattern(regexp="[A-Za-z0-9._:-]{1,128}") String idempotencyKey,
                       @NotNull @Valid CreateJobRequest job) {}

    @JsonIgnore
    @AssertTrue(message="Each batch item must have a distinct idempotencyKey")
    public boolean isKeySetUnique() {
        if(items==null) return true;
        var keys=new HashSet<String>();
        for(var item:items) if(item!=null && item.idempotencyKey()!=null && !keys.add(item.idempotencyKey())) return false;
        return true;
    }
}
