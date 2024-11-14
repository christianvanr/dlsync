package com.snowflake.dlsync.models;

import java.sql.Date;

public class ScriptEvent {
    private String id;
    private String objectName;
    private Integer version;
    private String author;
    private String status;
    private String log;
    private Long changeSyncId;
    private String createdBy;
    private Date createdTs;
}
