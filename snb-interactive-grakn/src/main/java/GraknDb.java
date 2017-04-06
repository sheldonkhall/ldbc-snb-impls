import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;

import java.io.IOException;
import java.util.Map;

/**
 * @author Felix Chapman
 */
public class GraknDb extends Db {

    private static GraknDbConnectionState connectionState = null;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        GraknSession session = Grakn.session(properties.get("uri"), properties.get("keyspace"));
        connectionState = new GraknDbConnectionState(session);

        registerOperationHandler(LdbcShortQuery1PersonProfile.class, GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler.class);
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
