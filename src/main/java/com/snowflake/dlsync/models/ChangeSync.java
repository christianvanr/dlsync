package com.snowflake.dlsync.models;

import java.sql.Timestamp;

public class ChangeSync {
    private Long id;
    private ChangeType changeType;
    private Status status;
    private String log;
    private Long changeCount;
    private Timestamp startTime;
    private Timestamp endTime;
}
