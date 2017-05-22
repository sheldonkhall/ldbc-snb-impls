package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 *
 */
public class GraknLdbcShortQuery3PersonFriendsResult extends LdbcShortQuery3PersonFriendsResult {
    public GraknLdbcShortQuery3PersonFriendsResult(long personId, String firstName, String lastName, LocalDateTime friendshipCreationDate) {
        super(personId, firstName, lastName, friendshipCreationDate.toEpochSecond(ZoneOffset.UTC));
    }
}
