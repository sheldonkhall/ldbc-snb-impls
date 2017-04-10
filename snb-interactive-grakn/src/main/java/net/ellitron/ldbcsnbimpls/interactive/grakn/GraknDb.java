package net.ellitron.ldbcsnbimpls.interactive.grakn;

import ai.grakn.GraknGraph;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.IOException;
import java.util.Map;

/**
 * @author Felix Chapman
 */
public class GraknDb extends Db {

    private static GraknDbConnectionState connectionState = null;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        connectionState = new GraknDbConnectionState(properties);

        registerOperationHandler(LdbcShortQuery1PersonProfile.class, GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, GraknShortQueryHandlers.LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, GraknShortQueryHandlers.LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, GraknShortQueryHandlers.LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, GraknShortQueryHandlers.LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, GraknShortQueryHandlers.LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, GraknShortQueryHandlers.LdbcShortQuery7MessageRepliesHandler.class);
    }

    @Override
    protected void onClose() throws IOException {
        connectionState.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
