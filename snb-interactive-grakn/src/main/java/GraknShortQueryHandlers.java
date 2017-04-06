import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;

import java.util.List;
import java.util.Map;

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
            GraknGraph graph = dbConnectionState.graph();

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
                            "($person, $place) isa located-in;" +
                            "$place has ID $placeID";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();
        }
    }
}

