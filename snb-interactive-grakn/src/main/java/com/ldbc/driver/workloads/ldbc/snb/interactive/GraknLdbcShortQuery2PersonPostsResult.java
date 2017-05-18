package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 *
 */
public class GraknLdbcShortQuery2PersonPostsResult extends LdbcShortQuery2PersonPostsResult{
    public GraknLdbcShortQuery2PersonPostsResult(long messageId, String messageContent, LocalDateTime messageCreationDate, long originalPostId, long originalPostAuthorId, String originalPostAuthorFirstName, String originalPostAuthorLastName) {
        super(messageId, messageContent, messageCreationDate.toEpochSecond(ZoneOffset.UTC), originalPostId, originalPostAuthorId, originalPostAuthorFirstName, originalPostAuthorLastName);
    }
}
