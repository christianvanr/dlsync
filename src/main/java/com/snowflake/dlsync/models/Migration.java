package com.snowflake.dlsync.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Migration {
    private String objectName;
    private Long version;
    private String author;
    private String content;
    private String rollback;
    private String verify;
}
