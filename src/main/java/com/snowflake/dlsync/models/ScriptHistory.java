package com.snowflake.dlsync.models;

import lombok.Data;

import java.sql.Date;

@Data
public class ScriptHistory {
    private String scriptId;
    private String objectName;
    private String objectType;
    private String rollbackScript;
    private String scriptHash;
    private String deployedHash;
    private Long changeSyncId;
    private String createdBy;
    private Date createdTs;
    private String updatedBy;
    private Date updatedTs;

}
