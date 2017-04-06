import ai.grakn.GraknGraph;

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

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
            if (results.size() > 0) {
                Map<String, Concept> fres = results.get(0);
                LdbcShortQuery1PersonProfileResult result =
                        new LdbcShortQuery1PersonProfileResult(
                                (String) fres.get("first-name").asResource().getValue(),
                                (String) fres.get("last-name").asResource().getValue(),
                                (Long) fres.get("birthday").asResource().getValue(),
                                (String) fres.get("location-ip").asResource().getValue(),
                                (String) fres.get("browser-used").asResource().getValue(),
                                (Long) fres.get("placeID").asResource().getValue(),
                                (String) fres.get("gender").asResource().getValue(),
                                (Long) fres.get("creation-date").asResource().getValue() );

                resultReporter.report(0, result, operation);
            } else {
                resultReporter.report(0, null, operation);
            }
        }
    }
}

