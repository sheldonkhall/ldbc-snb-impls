package net.ellitron.ldbcsnbimpls.interactive.grakn;

import ai.grakn.GraknGraph;

import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.MatchQuery;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by miko on 06/04/2017.
 * Ruined by felix on 06/04/2017.
 */


public class GraknShortQueryHandlers {

    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query =
                        "match" +
                                "$person isa person has person-id " +
                                operation.personId() + "; " +
                                "($person, $first-name) isa has-first-name; " +
                                "($person, $last-name) isa has-last-name; " +
                                "($person, $birth-day) isa has-birth-day; " +
                                "($person, $location-ip) isa has-location-ip; " +
                                "($person, $browser-used) isa has-browser-used; " +
                                "($person, $gender) isa has-gender; " +
                                "($person, $creation-date) isa has-creation-date; " +
                                "(located: $person, region: $place) isa is-located-in; " +
                                "($place, $placeID) isa key-place-id;";

                List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();
                if (results.size() > 0) {
                    Map<String, Concept> fres = results.get(0);

                    LdbcShortQuery1PersonProfileResult result =
                            new LdbcShortQuery1PersonProfileResult(
                                    (String) fres.get("first-name").asResource().getValue(),
                                    (String) fres.get("last-name").asResource().getValue(),
                                    (Long) fres.get("birth-day").asResource().getValue(),
                                    (String) fres.get("location-ip").asResource().getValue(),
                                    (String) fres.get("browser-used").asResource().getValue(),
                                    (Long) fres.get("placeID").asResource().getValue(),
                                    (String) fres.get("gender").asResource().getValue(),
                                    (Long) fres.get("creation-date").asResource().getValue());

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);
                }
            }
        }
    }


    public static class LdbcShortQuery2PersonPostsHandler implements
            OperationHandler<LdbcShortQuery2PersonPosts, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$person isa person has person-id " + operation.personId() + "; " +
                        "(creator: $person, product: $message) isa has-creator; " +
                        "($message, $date) isa has-creation-date; " +
                        "($message, $messageId) isa key-message-id; " +
                        "(reply: $message, original: $originalPost) isa reply-of; " +
                        "($originalPost, $opId) isa key-message-id;" +
                        "(product: $originalPost, creator: $person2) isa has-creator; " +
                        "($person2, $authorId) isa key-person-id; " +
                        "($person2, $fname) isa has-first-name;" +
                        "($person2, $lname) isa has-last-name; ";

                List<Map<String, Concept>> results = graph.graql().infer(true).<MatchQuery>parse(query).execute();


                    Comparator<Map<String, Concept>> ugly = Comparator.<Map<String, Concept>>comparingLong(map -> resource(map, "date")).reversed()
                            .thenComparingLong(map -> resource(map, "messageId")).reversed();

                    List<LdbcShortQuery2PersonPostsResult> result = results.stream()
                            .sorted(ugly).limit(10)
                            .map(map -> new LdbcShortQuery2PersonPostsResult(resource(map, "messageId"),
                                    resource(map, "content"),
                                    resource(map, "date"),
                                    resource(map, "opId"),
                                    resource(map, "authorId"),
                                    resource(map, "fname"),
                                    resource(map, "lname")))
                            .collect(Collectors.toList());

                    resultReporter.report(0, result, operation);


            }

        }

        private <T> T resource(Map<String, Concept> result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }


    public static class LdbcShortQuery3PersonFriendsHandler implements
            OperationHandler<LdbcShortQuery3PersonFriends, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$person isa person has person-id " + operation.personId() + "; " +
                        "(friend: $person, friend: $friend) isa knows has creation-date $date; " +
                        "$friend has person-id $friendId has first-name $fname has last-name $lname; ";


                List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();


                    Comparator<Map<String, Concept>> ugly = Comparator.<Map<String, Concept>>comparingLong(map -> resource(map, "date")).reversed()
                            .thenComparingLong(map -> resource(map, "friendId"));

                    List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                            .sorted(ugly)
                            .map(map -> new LdbcShortQuery3PersonFriendsResult(resource(map, "friendId"),
                                    resource(map, "fname"),
                                    resource(map, "lname"),
                                    resource(map, "date")))
                            .collect(Collectors.toList());

                    resultReporter.report(0, result, operation);

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
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match" +
                        "$m isa message has message-id " + operation.messageId() +
                        " has creation-date $date; $m has content $content or $m has image-file $content;";

                List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();


                if (results.size() == 1) {
                    Map<String, Concept> fres = results.get(0);

                    String cr = (String) fres.get("content").asResource().getValue();

                    LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                            cr,
                            (Long) fres.get("date").asResource().getValue()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }


    }

    public static class LdbcShortQuery5MessageCreatorHandler implements
            OperationHandler<LdbcShortQuery5MessageCreator, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        " $m isa message has message-id " + operation.messageId() + ";" +
                        " (product: $m , creator: $person) isa has-creator;" +
                        " $person has first-name $fname has last-name $lname has person-id $pID;";

                List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

                if (results.size() == 1) {
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
    }


    // TODO: The following requires a rule to properly work
    public static class LdbcShortQuery6MessageForumHandler implements
            OperationHandler<LdbcShortQuery6MessageForum, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$m isa message has message-id " + operation.messageId() + "; " +
                        "(contained: $m , container: $forum) isa container-of;" +
                        "$forum has forum-id $fid, has title $title; " +
                        "(moderated: $forum, moderator: $mod) isa has-moderator; " +
                        "$mod has person-id $modid, has first-name $fname, has last-name $lname;";

                List<Map<String, Concept>> results = graph.graql().infer(true).<MatchQuery>parse(query).execute();

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

        }


    public static class LdbcShortQuery7MessageRepliesHandler implements
            OperationHandler<LdbcShortQuery7MessageReplies, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match $m isa message has message-id " + operation.messageId() + " ;" +
                        "(product: $m, creator: $author1) isa has-creator; " +
                        "(original: $m, reply: $comment) isa reply-of; " +
                        "$comment has message-id $cid has content $content has creation-date $date; " +
                        "(product: $comment, creator: $author2) isa has-creator; " +
                        "$author2 has person-id $pid, has first-name $fname, has last-name $lname;";

                List<Map<String, Concept>> results = graph.graql().<MatchQuery>parse(query).execute();

                    Comparator<Map<String, Concept>> ugly = Comparator.<Map<String, Concept>>comparingLong(map -> resource(map, "date")).reversed()
                            .thenComparingLong(map -> resource(map, "pid"));

                    List<LdbcShortQuery7MessageRepliesResult> result = results.stream()
                            .sorted(ugly)
                            .map(map -> new LdbcShortQuery7MessageRepliesResult(resource(map, "cid"),
                                    resource(map, "content"),
                                    resource(map, "date"),
                                    resource(map, "pid"),
                                    resource(map, "fname"),
                                    resource(map, "lname"),
                                    checkIfFriends(conceptId(map, "author1"), conceptId(map, "author2"), graph)))
                            .collect(Collectors.toList());

                    resultReporter.report(0, result, operation);

            }

        }

        private boolean checkIfFriends(String author1, String author2, GraknGraph graph) {
            String query = "match $x id '" + author1 + "'; $y id '" + author2 + "';" +
                    "(friend: $x, friend:  $y) isa knows; ask;";
            return graph.graql().<AskQuery>parse(query).execute();
        }

        private String conceptId(Map<String, Concept> result, String name) {
            return result.get(name).getId().toString();
        }

        private <T> T resource(Map<String, Concept> result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }
}