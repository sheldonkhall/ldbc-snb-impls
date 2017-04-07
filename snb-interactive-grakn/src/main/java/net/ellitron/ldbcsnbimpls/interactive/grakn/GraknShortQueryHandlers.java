package net.ellitron.ldbcsnbimpls.interactive.grakn;

import ai.grakn.GraknGraph;

import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

/**
 * Created by miko on 06/04/2017.
 * Ruined by felix on 06/04/2017.
 */


public class GraknShortQueryHandlers {

    // TODO Move the data conversion somewhere smarter

    public static long dateStringToLong(String date, boolean withTime) {
        SimpleDateFormat f;

        if (withTime) {
            f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        }
        else {
            f = new SimpleDateFormat("yyyy-MM-dd");
        }
        f.setTimeZone(TimeZone.getTimeZone("GMT"));


        Long convertedDate = null;

        try { convertedDate =
                f.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return convertedDate;
    }

    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknGraph graph = dbConnectionState.graph();

            String query =
                    "match" +
                            "$person isa person has person-id " +
                            operation.personId() +
                            " has first-name $first-name" +
                            " has last-name $last-name" +
                            " has birth-day $birthday" +
                            " has location-ip $location-ip" +
                            " has browser-used $browser-used" +
                            " has gender $gender" +
                            " has creation-date $creation-date;" +
                            " (located: $person, region: $place) isa is-located-in;" +
                            " $place has place-id $placeID;";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();
            if (results.size() > 0) {
                Map<String, Concept> fres = results.get(0);

                LdbcShortQuery1PersonProfileResult result =
                        new LdbcShortQuery1PersonProfileResult(
                                (String) fres.get("first-name").asResource().getValue(),
                                (String) fres.get("last-name").asResource().getValue(),
                                dateStringToLong((String) fres.get("birthday").asResource().getValue(), false),
                                (String) fres.get("location-ip").asResource().getValue(),
                                (String) fres.get("browser-used").asResource().getValue(),
                                (Long) fres.get("placeID").asResource().getValue(),
                                (String) fres.get("gender").asResource().getValue(),
                                dateStringToLong((String) fres.get("creation-date").asResource().getValue(), true));

                resultReporter.report(0, result, operation);

            } else {
                resultReporter.report(0, null, operation);
            }
        }
    }

    /*
    public static class LdbcShortQuery2PersonPostsHandler implements
            OperationHandler<LdbcShortQuery2PersonPosts, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknGraph graph = dbConnectionState.graph();


        }
    }
    */

    public static class LdbcShortQuery3PersonFriendsHandler implements
            OperationHandler<LdbcShortQuery3PersonFriends, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknGraph graph = dbConnectionState.graph();

            String query = "match " +
                    "$person isa person has person-id " +  operation.personId() + "; " +
                    "($person, $friend) isa knows has creation-date $date; " +
                    "$friend has person-id $friendId has first-name $fname has last-name $lname; " +
                    "order by desc $date $id;";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

            if (results.size() > 0) {
                Comparator<Map<String, Concept>> ugly = Comparator.<Map<String, Concept>>comparingLong(map -> dateStringToLong(resource(map, "date"), true))
                       .thenComparingLong(map -> resource(map, "friendId"));
                List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                        .sorted(ugly)
                        .map(map -> new LdbcShortQuery3PersonFriendsResult(resource(map, "friendId"), resource(map, "fname"), resource(map, "lname"), dateStringToLong(resource(map, "date"), true)))
                        .collect(Collectors.toList());
                resultReporter.report(0, result, operation);
            } else {
                resultReporter.report(0, null, operation);
            }
        }

        private <T> T resource(Map<String, Concept> result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }


    public static class LdbcShortQuery4MessageContentHandler implements
            OperationHandler<LdbcShortQuery4MessageContent, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknGraph graph = dbConnectionState.graph();

            String query = "match" +
                    "$m isa message has message-id " + operation.messageId() + ";" +
                    "$m has creation-date $date has content $content;";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

            if (results.size() > 0) {
                Map<String, Concept> fres = results.get(0);
                LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                        (String) fres.get("content").asResource().getValue(),
                        dateStringToLong((String) fres.get("date").asResource().getValue(), true)
                );

                resultReporter.report(0, result, operation);

            } else {
                resultReporter.report(0, null, operation);

            }

        }


    }

    public static class LdbcShortQuery5MessageCreatorHandler implements
            OperationHandler<LdbcShortQuery5MessageCreator, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknGraph graph = dbConnectionState.graph();

            String query = "match " +
                    " $m isa message has message-id " + operation.messageId() + ";" +
                    " (product: $m , creator: $person) isa has-creator;" +
                    " $person has first-name $fname has last-name $lname has person-id $pID;";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

            if (results.size() > 0) {
                Map<String, Concept> fres = results.get(0);
                LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                        (Long) fres.get("pID").asResource().getValue(),
                        (String) fres.get("fname").asResource().getValue(),
                        (String) fres.get("lname").asResource().getValue()
                );

                resultReporter.report(0, result, operation);

            } else {
                resultReporter.report(0, null, operation);

            }

        }
    }


    // TODO: The following requires a rule to properly work
    public static class LdbcShortQuery6MessageForumHandler implements
            OperationHandler<LdbcShortQuery6MessageForum, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            GraknGraph graph = dbConnectionState.graph();

            String query = "match " +
                    "$m isa message has message-id " + operation.messageId() + "; " +
                    "(contained: $m , container: $forum) isa container-of;" +
                    "$forum has forum-id $fid, has title $title; " +
                    "(moderated: $forum, moderator: $mod) isa has-moderator; " +
                    "$mod has person-id $modid, has first-name $fname, has last-name $lname;";

            List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

            if (results.size() > 0) {
                Map<String, Concept> fres = results.get(0);
                LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                        (Long) fres.get("fid").asResource().getValue(),
                        (String) fres.get("title").asResource().getValue(),
                        (Long) fres.get("modid").asResource().getValue(),
                        (String) fres.get("fname").asResource().getValue(),
                        (String) fres.get("lname").asResource().getValue()
                );

                resultReporter.report(0, result, operation);

            } else {
                resultReporter.report(0, null, operation);

            }

        }

        }

    /*
    public static class LdbcShortQuery7MessageRepliesHandler implements
            OperationHandler<LdbcShortQuery7MessageReplies, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

        }
    }*/
}

