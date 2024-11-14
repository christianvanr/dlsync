package com.snowflake.dlsync.models;

import lombok.Data;

import java.sql.Date;

@Data
public class ScriptHash {
    private String objectName;
    private Integer version;
    private String author;
    private String rollbackScript;
    private String hash;
    private String deployedHash;
    private Long changeSyncId;
    private String createdBy;
    private Date createdTs;
    private String updatedBy;
    private Date updatedTs;

}
