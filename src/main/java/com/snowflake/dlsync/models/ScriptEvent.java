package com.snowflake.dlsync.models;

import java.sql.Date;

public class ScriptEvent {
    private String id;
    private String scriptId;
    private String objectName;
    private String scriptHash;
    private String status;
    private String log;
    private Long changeSyncId;
    private String createdBy;
    private Date createdTs;
}
