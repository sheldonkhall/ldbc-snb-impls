import ai.grakn.GraknSession;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;


/**
 * Created by miko on 06/04/2017.
 * Ruined by felix on 06/04/2017.
 */


public class GraknShortQueryHandlers {
    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, GraknDbConnectionState>
    {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException
        {
            GraknSession session = dbConnectionState.session();
            String query =
                    "match" +
                            "$person isa person has ID " +
                            "'" + operation.personId() + "' " +
                            "has first-name $first-name" +
                            "has last-name $last-name" +
                            "has birth-day $birthday" +
                            "has location-ip $location-ip" +
                            "has browser-used $browser-used" +
                            "has gender $gender" +
                            "has creation-date $creation-date;" +
                            "($person, $placeId) isa located-in;";

            // Execute the query and get the results

            // Parse the result text
        }
    }
}

