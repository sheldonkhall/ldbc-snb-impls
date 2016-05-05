/* 
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

import net.ellitron.ldbcsnbimpls.snb.Entity;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.Organization;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import org.apache.commons.configuration.BaseConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.Map;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.util.DbHelper;

/**
 * An implementation of the LDBC SNB interactive workload[1] for Neo4j. Queries
 * are executed against a running Neo4j server. Configuration parameters for
 * this implementation (that are supplied via the LDBC driver) are listed
 * below.
 * <p>
 * Configuration Parameters:
 * <ul>
 * <li>host - IP address of the Neo4j web server (default: 127.0.0.1).</li>
 * <li>port - port of the Neo4j web server (default: 7474).</li>
 * </ul>
 * <p>
 * References:<br>
 * [1]: Prat, Arnau (UPC) and Boncz, Peter (VUA) and Larriba, Josep Lluís (UPC)
 * and Angles, Renzo (TALCA) and Averbuch, Alex (NEO) and Erling, Orri (OGL)
 * and Gubichev, Andrey (TUM) and Spasić, Mirko (OGL) and Pham, Minh-Duc (VUA)
 * and Martínez, Norbert (SPARSITY). "LDBC Social Network Benchmark (SNB) -
 * v0.2.2 First Public Draft Release". http://www.ldbcouncil.org/.
 * <p>
 * TODO:<br>
 * <ul>
 * </ul>
 * <p>
 * 
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDb extends Db {

  private Neo4jDbConnectionState connectionState = null;

  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connectionState;
  }

  @Override
  protected void onClose() throws IOException {
    connectionState.close();
  }

  @Override
  protected void onInit(Map<String, String> properties,
      LoggingService loggingService) throws DbException {

    /*
     * Extract parameters from properties map.
     */
    String host;
    if (properties.containsKey("host")) {
      host = properties.get("host");
    } else {
      host = "127.0.0.1";
    }

    String port;
    if (properties.containsKey("port")) {
      port = properties.get("port");
    } else {
      port = "7474";
    }

    connectionState = new Neo4jDbConnectionState(host, port);

    /*
     * Register operation handlers with the benchmark.
     */
    registerOperationHandler(LdbcShortQuery1PersonProfile.class,
        LdbcShortQuery1PersonProfileHandler.class);
    registerOperationHandler(LdbcShortQuery2PersonPosts.class,
        LdbcShortQuery2PersonPostsHandler.class);
    registerOperationHandler(LdbcShortQuery3PersonFriends.class,
        LdbcShortQuery3PersonFriendsHandler.class);
    registerOperationHandler(LdbcShortQuery4MessageContent.class,
        LdbcShortQuery4MessageContentHandler.class);
    registerOperationHandler(LdbcShortQuery5MessageCreator.class,
        LdbcShortQuery5MessageCreatorHandler.class);
    registerOperationHandler(LdbcShortQuery6MessageForum.class,
        LdbcShortQuery6MessageForumHandler.class);
    registerOperationHandler(LdbcShortQuery7MessageReplies.class,
        LdbcShortQuery7MessageRepliesHandler.class);

    registerOperationHandler(LdbcUpdate1AddPerson.class,
        LdbcUpdate1AddPersonHandler.class);
    registerOperationHandler(LdbcUpdate2AddPostLike.class,
        LdbcUpdate2AddPostLikeHandler.class);
    registerOperationHandler(LdbcUpdate3AddCommentLike.class,
        LdbcUpdate3AddCommentLikeHandler.class);
    registerOperationHandler(LdbcUpdate4AddForum.class,
        LdbcUpdate4AddForumHandler.class);
    registerOperationHandler(LdbcUpdate5AddForumMembership.class,
        LdbcUpdate5AddForumMembershipHandler.class);
    registerOperationHandler(LdbcUpdate6AddPost.class,
        LdbcUpdate6AddPostHandler.class);
    registerOperationHandler(LdbcUpdate7AddComment.class,
        LdbcUpdate7AddCommentHandler.class);
    registerOperationHandler(LdbcUpdate8AddFriendship.class,
        LdbcUpdate8AddFriendshipHandler.class);
  }

  /**
   * ------------------------------------------------------------------------
   * Complex Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Given a start Person, find up to 20 Persons with a given first name that
   * the start Person is connected to (excluding start Person) by at most 3
   * steps via Knows relationships. Return Persons, including summaries of the
   * Persons workplaces and places of study. Sort results ascending by their
   * distance from the start Person, for Persons within the same distance sort
   * ascending by their last name, and for Persons with same last name
   * ascending by their identifier.[1]
   */
  public static class LdbcQuery1Handler
      implements OperationHandler<LdbcQuery1, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery1Handler.class);

    @Override
    public void executeOperation(LdbcQuery1 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find (most recent) Posts and Comments from all of
   * that Person’s friends, that were created before (and including) a given
   * date. Return the top 20 Posts/Comments, and the Person that created each
   * of them. Sort results descending by creation date, and then ascending by
   * Post identifier.[1]
   */
  public static class LdbcQuery2Handler
      implements OperationHandler<LdbcQuery2, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery2Handler.class);

    @Override
    public void executeOperation(LdbcQuery2 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find Persons that are their friends and friends of
   * friends (excluding start Person) that have made Posts/Comments in both of
   * the given Countries, X and Y, within a given period. Only Persons that are
   * foreign to Countries X and Y are considered, that is Persons whose
   * Location is not Country X or Country Y. Return top 20 Persons, and their
   * Post/Comment counts, in the given countries and period. Sort results
   * descending by total number of Posts/Comments, and then ascending by Person
   * identifier.[1]
   */
  public static class LdbcQuery3Handler
      implements OperationHandler<LdbcQuery3, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery3Handler.class);

    @Override
    public void executeOperation(LdbcQuery3 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find Tags that are attached to Posts that were
   * created by that Person’s friends. Only include Tags that were attached to
   * friends’ Posts created within a given time interval, and that were never
   * attached to friends’ Posts created before this interval. Return top 10
   * Tags, and the count of Posts, which were created within the given time
   * interval, that this Tag was attached to. Sort results descending by Post
   * count, and then ascending by Tag name.[1]
   */
  public static class LdbcQuery4Handler
      implements OperationHandler<LdbcQuery4, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery4Handler.class);

    @Override
    public void executeOperation(LdbcQuery4 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find the Forums which that Person’s friends and
   * friends of friends (excluding start Person) became Members of after a
   * given date. Return top 20 Forums, and the number of Posts in each Forum
   * that was Created by any of these Persons. For each Forum consider only
   * those Persons which joined that particular Forum after the given date.
   * Sort results descending by the count of Posts, and then ascending by Forum
   * identifier.[1]
   */
  public static class LdbcQuery5Handler
      implements OperationHandler<LdbcQuery5, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery5Handler.class);

    @Override
    public void executeOperation(LdbcQuery5 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person and some Tag, find the other Tags that occur together
   * with this Tag on Posts that were created by start Person’s friends and
   * friends of friends (excluding start Person). Return top 10 Tags, and the
   * count of Posts that were created by these Persons, which contain both this
   * Tag and the given Tag. Sort results descending by count, and then
   * ascending by Tag name.[1]
   */
  public static class LdbcQuery6Handler
      implements OperationHandler<LdbcQuery6, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery6Handler.class);

    @Override
    public void executeOperation(LdbcQuery6 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find (most recent) Likes on any of start Person’s
   * Posts/Comments. Return top 20 Persons that Liked any of start Person’s
   * Posts/Comments, the Post/Comment they liked most recently, creation date
   * of that Like, and the latency (in minutes) between creation of
   * Post/Comment and Like. Additionally, return a flag indicating whether the
   * liker is a friend of start Person. In the case that a Person Liked
   * multiple Posts/Comments at the same time, return the Post/Comment with
   * lowest identifier. Sort results descending by creation time of Like, then
   * ascending by Person identifier of liker.[1]
   */
  public static class LdbcQuery7Handler
      implements OperationHandler<LdbcQuery7, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery7Handler.class);

    @Override
    public void executeOperation(LdbcQuery7 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find (most recent) Comments that are replies to
   * Posts/Comments of the start Person. Only consider immediate (1-hop)
   * replies, not the transitive (multi-hop) case. Return the top 20 reply
   * Comments, and the Person that created each reply Comment. Sort results
   * descending by creation date of reply Comment, and then ascending by
   * identifier of reply Comment.[1]
   */
  public static class LdbcQuery8Handler
      implements OperationHandler<LdbcQuery8, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery8Handler.class);

    @Override
    public void executeOperation(LdbcQuery8 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find the (most recent) Posts/Comments created by
   * that Person’s friends or friends of friends (excluding start Person). Only
   * consider the Posts/Comments created before a given date (excluding that
   * date). Return the top 20 Posts/Comments, and the Person that created each
   * of those Posts/Comments. Sort results descending by creation date of
   * Post/Comment, and then ascending by Post/Comment identifier.[1]
   */
  public static class LdbcQuery9Handler
      implements OperationHandler<LdbcQuery9, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery9Handler.class);

    @Override
    public void executeOperation(LdbcQuery9 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find that Person’s friends of friends (excluding
   * start Person, and immediate friends), who were born on or after the 21st
   * of a given month (in any year) and before the 22nd of the following month.
   * Calculate the similarity between each of these Persons and start Person,
   * where similarity for any Person is defined as follows:
   * <ul>
   * <li>common = number of Posts created by that Person, such that the Post
   * has a Tag that start Person is Interested in</li>
   * <li>uncommon = number of Posts created by that Person, such that the Post
   * has no Tag that start Person is Interested in</li>
   * <li>similarity = common - uncommon</li>
   * </ul>
   * Return top 10 Persons, their Place, and their similarity score. Sort
   * results descending by similarity score, and then ascending by Person
   * identifier.[1]
   */
  public static class LdbcQuery10Handler
      implements OperationHandler<LdbcQuery10, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery10Handler.class);

    @Override
    public void executeOperation(LdbcQuery10 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find that Person’s friends and friends of friends
   * (excluding start Person) who started Working in some Company in a given
   * Country, before a given date (year). Return top 10 Persons, the Company
   * they worked at, and the year they started working at that Company. Sort
   * results ascending by the start date, then ascending by Person identifier,
   * and lastly by Organization name descending.[1]
   */
  public static class LdbcQuery11Handler
      implements OperationHandler<LdbcQuery11, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery11Handler.class);

    @Override
    public void executeOperation(LdbcQuery11 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given a start Person, find the Comments that this Person’s friends made in
   * reply to Posts, considering only those Comments that are immediate (1-hop)
   * replies to Posts, not the transitive (multi-hop) case. Only consider Posts
   * with a Tag in a given TagClass or in a descendent of that TagClass. Count
   * the number of these reply Comments, and collect the Tags (with valid tag
   * class) that were attached to the Posts they replied to. Return top 20
   * Persons with at least one reply, the reply count, and the collection of
   * Tags. Sort results descending by Comment count, and then ascending by
   * Person identifier.[1]
   */
  public static class LdbcQuery12Handler
      implements OperationHandler<LdbcQuery12, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery12Handler.class);

    @Override
    public void executeOperation(LdbcQuery12 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given two Persons, find the shortest path between these two Persons in the
   * subgraph induced by the Knows relationships. Return the length of this
   * path. -1 should be returned if no path is found, and 0 should be returned
   * if the start person is the same as the end person.[1]
   */
  public static class LdbcQuery13Handler
      implements OperationHandler<LdbcQuery13, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery13Handler.class);

    @Override
    public void executeOperation(LdbcQuery13 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * Given two Persons, find all (unweighted) shortest paths between these two
   * Persons, in the subgraph induced by the Knows relationship. Then, for each
   * path calculate a weight. The nodes in the path are Persons, and the weight
   * of a path is the sum of weights between every pair of consecutive Person
   * nodes in the path. The weight for a pair of Persons is calculated such
   * that every reply (by one of the Persons) to a Post (by the other Person)
   * contributes 1.0, and every reply (by ones of the Persons) to a Comment (by
   * the other Person) contributes 0.5. Return all the paths with shortest
   * length, and their weights. Sort results descending by path weight. The
   * order of paths with the same weight is unspecified.[1]
   */
  public static class LdbcQuery14Handler
      implements OperationHandler<LdbcQuery14, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(LdbcQuery14 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * ------------------------------------------------------------------------
   * Short Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Given a start Person, retrieve their first name, last name, birthday, IP
   * address, browser, and city of residence.[1]
   */
  public static class LdbcShortQuery1PersonProfileHandler implements
      OperationHandler<LdbcShortQuery1PersonProfile, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (n:Person {id:{id}})-[:IS_LOCATED_IN]-(p:Place)"
          + " RETURN"
          + "   n.firstName AS firstName,"
          + "   n.lastName AS lastName,"
          + "   n.birthday AS birthday,"
          + "   n.locationIP AS locationIp,"
          + "   n.browserUsed AS browserUsed,"
          + "   n.gender AS gender,"
          + "   n.creationDate AS creationDate,"
          + "   p.id AS cityId";
      String parameters = "{ \"id\" : \"" + operation.personId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      if (table.get("firstName").length > 0) {
        LdbcShortQuery1PersonProfileResult result =
            new LdbcShortQuery1PersonProfileResult(
                table.get("firstName")[0],
                table.get("lastName")[0],
                Long.decode(table.get("birthday")[0]),
                table.get("locationIp")[0],
                table.get("browserUsed")[0],
                Long.decode(table.get("cityId")[0]),
                table.get("gender")[0],
                Long.decode(table.get("creationDate")[0]));

        resultReporter.report(0, result, operation);
      } else {
        resultReporter.report(0, null, operation);
      }
    }
  }

  /**
   * Given a start Person, retrieve the last 10 Messages (Posts or Comments)
   * created by that user. For each message, return that message, the original
   * post in its conversation, and the author of that post. If any of the
   * Messages is a Post, then the original Post will be the same Message, i.e.,
   * that Message will appear twice in that result. Order results descending by
   * message creation date, then descending by message identifier.[1]
   */
  public static class LdbcShortQuery2PersonPostsHandler implements
      OperationHandler<LdbcShortQuery2PersonPosts, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (:Person {id:{id}})<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..]->(p:Post)"
          + " MATCH (p)-[:HAS_CREATOR]->(c)"
          + " RETURN"
          + "   m.id as messageId,"
          + "   CASE has(m.content)"
          + "     WHEN true THEN m.content"
          + "     ELSE m.imageFile"
          + "   END AS messageContent,"
          + "   m.creationDate AS messageCreationDate,"
          + "   p.id AS originalPostId,"
          + "   c.id AS originalPostAuthorId,"
          + "   c.firstName as originalPostAuthorFirstName,"
          + "   c.lastName as originalPostAuthorLastName"
          + " ORDER BY messageCreationDate DESC LIMIT {limit}";
      String parameters = "{ "
          + "\"id\" : \"" + operation.personId() + "\", "
          + "\"limit\" : " + operation.limit() + " }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();
      for (int i = 0; i < table.get("messageId").length; i++) {
        result.add(new LdbcShortQuery2PersonPostsResult(
            Long.decode(table.get("messageId")[i]),
            table.get("messageContent")[i],
            Long.decode(table.get("messageCreationDate")[i]),
            Long.decode(table.get("originalPostId")[i]),
            Long.decode(table.get("originalPostAuthorId")[i]),
            table.get("originalPostAuthorFirstName")[i],
            table.get("originalPostAuthorLastName")[i]));
      }

      resultReporter.report(0, result, operation);
    }
  }

  /**
   * Given a start Person, retrieve all of their friends, and the date at which
   * they became friends. Order results descending by friendship creation date,
   * then ascending by friend identifier.[1]
   */
  public static class LdbcShortQuery3PersonFriendsHandler implements
      OperationHandler<LdbcShortQuery3PersonFriends, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (n:Person {id:{id}})-[r:KNOWS]-(friend)"
          + " RETURN"
          + "   friend.id AS personId,"
          + "   friend.firstName AS firstName,"
          + "   friend.lastName AS lastName,"
          + "   r.creationDate AS friendshipCreationDate"
          + " ORDER BY friendshipCreationDate DESC, toInt(personId) ASC";
      String parameters = "{ \"id\" : \"" + operation.personId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
      for (int i = 0; i < table.get("personId").length; i++) {
        result.add(new LdbcShortQuery3PersonFriendsResult(
            Long.decode(table.get("personId")[i]),
            table.get("firstName")[i],
            table.get("lastName")[i],
            Long.decode(table.get("friendshipCreationDate")[i])));
      }

      resultReporter.report(0, result, operation);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its content and creation
   * date.[1]
   */
  public static class LdbcShortQuery4MessageContentHandler implements
      OperationHandler<LdbcShortQuery4MessageContent, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})"
          + " RETURN"
          + "   CASE has(m.content)"
          + "     WHEN true THEN m.content"
          + "     ELSE m.imageFile"
          + "   END AS messageContent,"
          + "   m.creationDate as messageCreationDate";
      String parameters = "{ \"id\" : \"" + operation.messageId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      if (table.get("messageContent").length > 0) {
        LdbcShortQuery4MessageContentResult result =
            new LdbcShortQuery4MessageContentResult(
                table.get("messageContent")[0],
                Long.decode(table.get("messageCreationDate")[0]));

        resultReporter.report(0, result, operation);
      } else {
        resultReporter.report(0, null, operation);
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its author.[1]
   */
  public static class LdbcShortQuery5MessageCreatorHandler implements
      OperationHandler<LdbcShortQuery5MessageCreator, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})-[:HAS_CREATOR]->(p:Person)"
          + " RETURN"
          + "   p.id AS personId,"
          + "   p.firstName AS firstName,"
          + "   p.lastName AS lastName";
      String parameters = "{ \"id\" : \"" + operation.messageId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      if (table.get("personId").length > 0) {
        LdbcShortQuery5MessageCreatorResult result =
            new LdbcShortQuery5MessageCreatorResult(
                Long.decode(table.get("personId")[0]),
                table.get("firstName")[0],
                table.get("lastName")[0]);

        resultReporter.report(0, result, operation);
      } else {
        resultReporter.report(0, null, operation);
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve the Forum that contains it and
   * the Person that moderates that forum. Since comments are not directly
   * contained in forums, for comments, return the forum containing the
   * original post in the thread which the comment is replying to.[1]
   */
  public static class LdbcShortQuery6MessageForumHandler implements
      OperationHandler<LdbcShortQuery6MessageForum, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)"
          + " RETURN"
          + "   f.id AS forumId,"
          + "   f.title AS forumTitle,"
          + "   mod.id AS moderatorId,"
          + "   mod.firstName AS moderatorFirstName,"
          + "   mod.lastName AS moderatorLastName";
      String parameters = "{ \"id\" : \"" + operation.messageId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      if (table.get("forumId").length > 0) {
        LdbcShortQuery6MessageForumResult result =
            new LdbcShortQuery6MessageForumResult(
                Long.decode(table.get("forumId")[0]),
                table.get("forumTitle")[0],
                Long.decode(table.get("moderatorId")[0]),
                table.get("moderatorFirstName")[0],
                table.get("moderatorLastName")[0]);

        resultReporter.report(0, result, operation);
      } else {
        resultReporter.report(0, null, operation);
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve the (1-hop) Comments that
   * reply to it. In addition, return a boolean flag indicating if the author
   * of the reply knows the author of the original message. If author is same
   * as original author, return false for "knows" flag. Order results
   * descending by creation date, then ascending by author identifier.[1]
   */
  public static class LdbcShortQuery7MessageRepliesHandler implements
      OperationHandler<LdbcShortQuery7MessageReplies, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)"
          + " OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)"
          + " RETURN"
          + "   c.id AS commentId,"
          + "   c.content AS commentContent,"
          + "   c.creationDate AS commentCreationDate,"
          + "   p.id AS replyAuthorId,"
          + "   p.firstName AS replyAuthorFirstName,"
          + "   p.lastName AS replyAuthorLastName,"
          + "   CASE r"
          + "     WHEN null THEN false"
          + "     ELSE true"
          + "   END AS replyAuthorKnowsOriginalMessageAuthor"
          + " ORDER BY commentCreationDate DESC, replyAuthorId ASC";
      String parameters = "{ \"id\" : \"" + operation.messageId() + "\" }";

      // Execute the query and get the results.
      driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      List<Map<String, String[]>> results = driver.execAndCommit();

      Map<String, String[]> table = results.get(0);
      List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();
      for (int i = 0; i < table.get("commentId").length; i++) {
        result.add(new LdbcShortQuery7MessageRepliesResult(
            Long.decode(table.get("commentId")[i]),
            table.get("commentContent")[i],
            Long.decode(table.get("commentCreationDate")[i]),
            Long.decode(table.get("replyAuthorId")[i]),
            table.get("replyAuthorFirstName")[i],
            table.get("replyAuthorLastName")[i],
            Boolean.valueOf(
                table.get("replyAuthorKnowsOriginalMessageAuthor")[i])));
      }

      resultReporter.report(0, result, operation);
    }
  }

  /**
   * ------------------------------------------------------------------------
   * Update Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Add a Person to the social network. [1]
   * <p>
   * TODO:
   * <ul>
   * <li>This query involves creating many relationships of different types.
   * This is currently done using multiple cypher queries, but it may be
   * possible to combine them in some way to amortize per query overhead and
   * thus increase performance.</li>
   * </ul>
   */
  public static class LdbcUpdate1AddPersonHandler implements
      OperationHandler<LdbcUpdate1AddPerson, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);
    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      // Create the person node.
      String statement =
          "   CREATE (p:Person {props})";
      String parameters = "{ \"props\" : {"
          + " \"id\" : \"" + operation.personId() + "\","
          + " \"firstName\" : \"" + operation.personFirstName() + "\","
          + " \"lastName\" : \"" + operation.personLastName() + "\","
          + " \"gender\" : \"" + operation.gender() + "\","
          + " \"birthday\" : " + operation.birthday().getTime() + ","
          + " \"creationDate\" : " + operation.creationDate().getTime() + ","
          + " \"locationIP\" : \"" + operation.locationIp() + "\","
          + " \"browserUsed\" : \"" + operation.browserUsed() + "\","
          + " \"speaks\" : "
          + DbHelper.listToJsonArray(operation.languages()) + ","
          + " \"emails\" : "
          + DbHelper.listToJsonArray(operation.emails())
          + " } }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      // Add isLocatedIn and hasInterest relationships.
      statement =
          "   MATCH (p:Person {id:{personId}}),"
          + "       (c:Place {id:{cityId}})"
          + " OPTIONAL MATCH (t:Tag)"
          + " WHERE t.id IN {tagIds}"
          + " WITH p, c, collect(t) AS tagSet"
          + " CREATE (p)-[:IS_LOCATED_IN]->(c)"
          + " FOREACH(t IN tagSet| CREATE (p)-[:HAS_INTEREST]->(t))";
      parameters = "{ "
          + " \"personId\" : \"" + operation.personId() + "\","
          + " \"cityId\" : \"" + operation.cityId() + "\","
          + " \"tagIds\" : " + DbHelper.listToJsonArray(operation.tagIds())
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      // Add studyAt relationships.
      if (operation.studyAt().size() > 0) {
        StringBuilder matchBldr = new StringBuilder();
        StringBuilder createBldr = new StringBuilder();
        StringBuilder paramBldr = new StringBuilder();

        matchBldr.append("MATCH (p:Person {id:{personId}}), ");
        createBldr.append("CREATE ");
        paramBldr.append("{\"personId\" : \"" + operation.personId() + "\", ");

        for (int i = 0; i < operation.studyAt().size(); i++) {
          Organization org = operation.studyAt().get(i);
          if (i > 0) {
            matchBldr.append(", ");
            createBldr.append(", ");
            paramBldr.append(", ");
          }
          matchBldr.append(
              String.format("(u%d:Organisation {id:{uId%d}})", i, i));
          createBldr.append(
              String.format("(p)-[:STUDY_AT {classYear:{cY%d}}]->(u%d)", i, i));
          paramBldr.append(
              String.format("\"uId%d\" : \"%d\"", i, org.organizationId()));
          paramBldr.append(", ");
          paramBldr.append(
              String.format("\"cY%d\" : %d", i, org.year()));
        }

        paramBldr.append("}");

        statement = matchBldr.toString() + " " + createBldr.toString();
        parameters = paramBldr.toString();

        driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      }

      // Add workAt relationships.
      if (operation.workAt().size() > 0) {
        StringBuilder matchBldr = new StringBuilder();
        StringBuilder createBldr = new StringBuilder();
        StringBuilder paramBldr = new StringBuilder();

        matchBldr.append("MATCH (p:Person {id:{personId}}), ");
        createBldr.append("CREATE ");
        paramBldr.append("{\"personId\" : \"" + operation.personId() + "\", ");

        for (int i = 0; i < operation.workAt().size(); i++) {
          Organization org = operation.workAt().get(i);
          if (i > 0) {
            matchBldr.append(", ");
            createBldr.append(", ");
            paramBldr.append(", ");
          }
          matchBldr.append(
              String.format("(c%d:Organisation {id:{cId%d}})", i, i));
          createBldr.append(
              String.format("(p)-[:WORK_AT {workFrom:{wF%d}}]->(c%d)", i, i));
          paramBldr.append(
              String.format("\"cId%d\" : \"%d\"", i, org.organizationId()));
          paramBldr.append(", ");
          paramBldr.append(
              String.format("\"wF%d\" : %d", i, org.year()));
        }

        paramBldr.append("}");

        statement = matchBldr.toString() + " " + createBldr.toString();
        parameters = paramBldr.toString();

        driver.enqueue(new Neo4jCypherStatement(statement, parameters));
      }

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Like to a Post of the social network.[1]
   */
  public static class LdbcUpdate2AddPostLikeHandler implements
      OperationHandler<LdbcUpdate2AddPostLike, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (p:Person {id:{personId}}),"
          + "       (m:Post {id:{postId}})"
          + " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
      String parameters = "{ "
          + " \"personId\" : \"" + operation.personId() + "\","
          + " \"postId\" : \"" + operation.postId() + "\","
          + " \"creationDate\" : " + operation.creationDate().getTime()
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

  }

  /**
   * Add a Like to a Comment of the social network.[1]
   */
  public static class LdbcUpdate3AddCommentLikeHandler implements
      OperationHandler<LdbcUpdate3AddCommentLike, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (p:Person {id:{personId}}),"
          + "       (m:Comment {id:{commentId}})"
          + " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
      String parameters = "{ "
          + " \"personId\" : \"" + operation.personId() + "\","
          + " \"commentId\" : \"" + operation.commentId() + "\","
          + " \"creationDate\" : " + operation.creationDate().getTime()
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Forum to the social network.[1]
   */
  public static class LdbcUpdate4AddForumHandler implements
      OperationHandler<LdbcUpdate4AddForum, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForumHandler.class);

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      // Create the forum node.
      String statement =
          "   CREATE (f:Forum {props})";
      String parameters = "{ \"props\" : {"
          + " \"id\" : \"" + operation.forumId() + "\","
          + " \"title\" : \"" + operation.forumTitle() + "\","
          + " \"creationDate\" : " + operation.creationDate().getTime()
          + " } }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      // Add hasModerator and hasTag relationships.
      statement =
          "   MATCH (f:Forum {id:{forumId}}),"
          + "       (p:Person {id:{moderatorId}})"
          + " OPTIONAL MATCH (t:Tag)"
          + " WHERE t.id IN {tagIds}"
          + " WITH f, p, collect(t) as tagSet"
          + " CREATE (f)-[:HAS_MODERATOR]->(p)"
          + " FOREACH (t IN tagSet| CREATE (f)-[:HAS_TAG]->(t))";
      parameters = "{ "
          + " \"forumId\" : \"" + operation.forumId() + "\","
          + " \"moderatorId\" : \"" + operation.moderatorPersonId() + "\","
          + " \"tagIds\" : " + DbHelper.listToJsonArray(operation.tagIds())
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Forum membership to the social network.[1]
   */
  public static class LdbcUpdate5AddForumMembershipHandler implements
      OperationHandler<LdbcUpdate5AddForumMembership, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate5AddForumMembershipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (f:Forum {id:{forumId}}),"
          + "       (p:Person {id:{personId}})"
          + " CREATE (f)-[:HAS_MEMBER {joinDate:{joinDate}}]->(p)";
      String parameters = "{ "
          + " \"forumId\" : \"" + operation.forumId() + "\","
          + " \"personId\" : \"" + operation.personId() + "\","
          + " \"joinDate\" : " + operation.joinDate().getTime()
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Post to the social network.[1]
   */
  public static class LdbcUpdate6AddPostHandler implements
      OperationHandler<LdbcUpdate6AddPost, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate6AddPostHandler.class);

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      // Create the post node.
      String statement =
          "   CREATE (m:Post:Message {props})";
      String parameters;
      if (operation.imageFile().length() > 0) {
        parameters = "{ \"props\" : {"
            + " \"id\" : \"" + operation.postId() + "\","
            + " \"imageFile\" : \"" + operation.imageFile() + "\","
            + " \"creationDate\" : " + operation.creationDate().getTime() + ","
            + " \"locationIP\" : \"" + operation.locationIp() + "\","
            + " \"browserUsed\" : \"" + operation.browserUsed() + "\","
            + " \"language\" : \"" + operation.language() + "\","
            + " \"length\" : " + operation.length()
            + " } }";
      } else {
        parameters = "{ \"props\" : {"
            + " \"id\" : \"" + operation.postId() + "\","
            + " \"creationDate\" : " + operation.creationDate().getTime() + ","
            + " \"locationIP\" : \"" + operation.locationIp() + "\","
            + " \"browserUsed\" : \"" + operation.browserUsed() + "\","
            + " \"language\" : \"" + operation.language() + "\","
            + " \"content\" : \"" + operation.content() + "\","
            + " \"length\" : " + operation.length()
            + " } }";
      }

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      // Add hasCreator, containerOf, isLocatedIn, and hasTag relationships.
      statement =
          "   MATCH (m:Post {id:{postId}}),"
          + "       (p:Person {id:{authorId}}),"
          + "       (f:Forum {id:{forumId}}),"
          + "       (c:Place {id:{countryId}})"
          + " OPTIONAL MATCH (t:Tag)"
          + " WHERE t.id IN {tagIds}"
          + " WITH m, p, f, c, collect(t) as tagSet"
          + " CREATE (m)-[:HAS_CREATOR]->(p),"
          + "        (m)<-[:CONTAINER_OF]-(f),"
          + "        (m)-[:IS_LOCATED_IN]->(c)"
          + " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";
      parameters = "{ "
          + " \"postId\" : \"" + operation.postId() + "\","
          + " \"authorId\" : \"" + operation.authorPersonId() + "\","
          + " \"forumId\" : \"" + operation.forumId() + "\","
          + " \"countryId\" : \"" + operation.countryId() + "\","
          + " \"tagIds\" : " + DbHelper.listToJsonArray(operation.tagIds())
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Comment replying to a Post/Comment to the social network.[1]
   */
  public static class LdbcUpdate7AddCommentHandler implements
      OperationHandler<LdbcUpdate7AddComment, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate7AddCommentHandler.class);

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      // Create the comment node.
      String statement =
          "   CREATE (c:Comment:Message {props})";
      String parameters = "{ \"props\" : {"
          + " \"id\" : \"" + operation.commentId() + "\","
          + " \"creationDate\" : " + operation.creationDate().getTime() + ","
          + " \"locationIP\" : \"" + operation.locationIp() + "\","
          + " \"browserUsed\" : \"" + operation.browserUsed() + "\","
          + " \"content\" : \"" + operation.content() + "\","
          + " \"length\" : " + operation.length()
          + " } }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      Long replyOfId;
      if (operation.replyToCommentId() != -1) {
        replyOfId = operation.replyToCommentId();
      } else {
        replyOfId = operation.replyToPostId();
      }

      // Add hasCreator, containerOf, isLocatedIn, and hasTag relationships.
      statement =
          "   MATCH (m:Comment {id:{commentId}}),"
          + "       (p:Person {id:{authorId}}),"
          + "       (r:Message {id:{replyOfId}}),"
          + "       (c:Place {id:{countryId}})"
          + " OPTIONAL MATCH (t:Tag)"
          + " WHERE t.id IN {tagIds}"
          + " WITH m, p, r, c, collect(t) as tagSet"
          + " CREATE (m)-[:HAS_CREATOR]->(p),"
          + "        (m)-[:REPLY_OF]->(r),"
          + "        (m)-[:IS_LOCATED_IN]->(c)"
          + " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";
      parameters = "{ "
          + " \"commentId\" : \"" + operation.commentId() + "\","
          + " \"authorId\" : \"" + operation.authorPersonId() + "\","
          + " \"replyOfId\" : \"" + replyOfId + "\","
          + " \"countryId\" : \"" + operation.countryId() + "\","
          + " \"tagIds\" : " + DbHelper.listToJsonArray(operation.tagIds())
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a friendship relation to the social network.[1]
   */
  public static class LdbcUpdate8AddFriendshipHandler implements
      OperationHandler<LdbcUpdate8AddFriendship, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate8AddFriendshipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Neo4jTransactionDriver driver = dbConnectionState.getTxDriver();

      String statement =
          "   MATCH (p1:Person {id:{person1Id}}),"
          + "       (p2:Person {id:{person2Id}})"
          + " CREATE (p1)-[:KNOWS {creationDate:{creationDate}}]->(p2)";
      String parameters = "{ "
          + " \"person1Id\" : \"" + operation.person1Id() + "\","
          + " \"person2Id\" : \"" + operation.person2Id() + "\","
          + " \"creationDate\" : " + operation.creationDate().getTime()
          + " }";

      driver.enqueue(new Neo4jCypherStatement(statement, parameters));

      driver.execAndCommit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }
}
