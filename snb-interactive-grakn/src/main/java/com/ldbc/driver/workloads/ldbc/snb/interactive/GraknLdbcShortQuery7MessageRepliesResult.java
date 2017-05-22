package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 *
 */
public class GraknLdbcShortQuery7MessageRepliesResult extends LdbcShortQuery7MessageRepliesResult {
    public GraknLdbcShortQuery7MessageRepliesResult(long commentId, String commentContent, LocalDateTime commentCreationDate, long replyAuthorId, String replyAuthorFirstName, String replyAuthorLastName, boolean replyAuthorKnowsOriginalMessageAuthor) {
        super(commentId, commentContent, commentCreationDate.toEpochSecond(ZoneOffset.UTC), replyAuthorId, replyAuthorFirstName, replyAuthorLastName, replyAuthorKnowsOriginalMessageAuthor);
    }
}
