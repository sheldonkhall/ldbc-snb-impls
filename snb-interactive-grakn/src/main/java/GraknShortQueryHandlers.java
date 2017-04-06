import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;

/**
 * Created by miko on 06/04/2017.
 */


public class GraknShortQueryHandlers {
    public static class GraknLdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState>
    {
        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     DbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException
        {}
    }
}

