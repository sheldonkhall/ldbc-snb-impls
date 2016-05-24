/* 
 * Copyright (C) 2016 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torc.util;

import net.ellitron.ldbcsnbimpls.interactive.core.Entity;
import net.ellitron.torc.util.UInt128;

import org.apache.tinkerpop.gremlin.structure.T;
import org.docopt.Docopt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.ellitron.ldbcsnbimpls.interactive.core.SnbEntity;
import net.ellitron.ldbcsnbimpls.interactive.torc.TorcEntity;
import net.ellitron.torc.TorcGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * A utility for loading dataset files generated by the LDBC SNB Data
 * Generator[1] into TorcDB[2].
 * <p>
 * TODO:<br>
 * <ul>
 * </ul>
 * <p>
 * [1]: git@github.com:ldbc/ldbc_snb_datagen.git<br>
 * [2]: git@github.com:ellitron/torc.git<br>
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class GraphLoader {

  private static final String doc =
      "GraphLoader: A utility for loading dataset files generated by the\n"
      + "LDBC SNB Data Generator into TorcDB."
      + "\n"
      + "Usage:\n"
      + "  GraphLoader [options] SOURCE\n"
      + "  GraphLoader (-h | --help)\n"
      + "  GraphLoader --version\n"
      + "\n"
      + "Arguments:\n"
      + "  SOURCE  Directory containing SNB dataset files.\n"
      + "\n"
      + "Options:\n"
      + "  -C --coordLoc     RAMCloud coordinator locator string\n"
      + "                    [default: tcp:host=127.0.0.1,port=12246].\n"
      + "  --masters         Number of RAMCloud master servers to use to\n"
      + "                    store the graph [default: 1].\n"
      + "  --graphName       The name to give the graph in RAMCloud\n"
      + "                    [default: default].\n"
      + "  --txSize          How many vertices/edges to load in a single\n"
      + "                    transaction. TorcDB transactions are buffered\n"
      + "                    locally before commit, and written in batch at\n"
      + "                    commit time. Setting this number appropriately\n"
      + "                    can therefore help to ammortize the RAMCloud\n"
      + "                    communication costs and increase loading\n"
      + "                    performance, although the right setting will\n"
      + "                    depend on system setup. [default: 128].\n"
      + "  --txRetries       The number of times to retry a transaction\n"
      + "                    before giving up. Transactions to RAMCloud may\n"
      + "                    fail due to timeouts, or sometimes conflicts\n"
      + "                    on objects (i.e. in the case of multithreaded\n"
      + "                    loading) [default: 100].\n"
      + "  --reportInterval  Number of seconds between reporting status to\n"
      + "                    the screen. [default: 10].\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
      + "\n";

  /*
   * Used for parsing dates in the original dataset files output by the data
   * generator, and converting them to milliseconds since Jan. 1 9170. We store
   * dates in this form in TorcDB.
   */
  private static final SimpleDateFormat birthdayDateFormat;
  private static final SimpleDateFormat creationDateDateFormat;

  static {
    birthdayDateFormat =
        new SimpleDateFormat("yyyy-MM-dd");
    birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    creationDateDateFormat =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  public static void main(String[] args)
      throws FileNotFoundException, IOException, ParseException {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("GraphLoader 1.0").parse(args);

    // Open a new TorcGraph with the supplied configuration.
    Map<String, String> config = new HashMap<>();
    config.put(TorcGraph.CONFIG_COORD_LOCATOR,
        (String) opts.get("-C"));
    config.put(TorcGraph.CONFIG_GRAPH_NAME,
        (String) opts.get("--graphName"));
    config.put(TorcGraph.CONFIG_NUM_MASTER_SERVERS,
        (String) opts.get("--masters"));

    Graph graph = TorcGraph.open(config);

    String inputDir = (String) opts.get("SOURCE");

    int txSize = Integer.decode((String) opts.get("--txSize"));
    int txRetries = Integer.decode((String) opts.get("--txRetries"));
    long reportInterval = Long.decode((String) opts.get("--reportInterval"));

    /*
     * Load SNB entities.
     */
    for (SnbEntity snbEntity : SnbEntity.values()) {
      System.out.println(
          String.format("Loading %s vertices...", snbEntity.name));

      String fileName = snbEntity.name + "_0_0.csv";
      long idSpace = TorcEntity.valueOf(snbEntity).idSpace;
      String label = TorcEntity.valueOf(snbEntity).label;

      Path path = Paths.get(inputDir + "/" + fileName);
      BufferedReader inFile =
          Files.newBufferedReader(path, StandardCharsets.UTF_8);

      // Keep track of the lines we've processed.
      long linesProcessed = 0;

      // First line of the file contains the column headers.
      String[] fieldNames = inFile.readLine().split("\\|");
      linesProcessed++;

      long startReportIntervalMillis = System.currentTimeMillis();
      long startLoadMillis = System.currentTimeMillis();
      boolean hasLinesLeft = true;
      while (hasLinesLeft) {
        /*
         * Buffer txSize lines at a time from the input file and keep it around
         * until commit time. If the commit succeeds we can forget about it,
         * otherwise we'll use it again to retry the transaction.
         */
        String line;
        List<String> lineBuffer = new ArrayList<>(txSize);
        while ((line = inFile.readLine()) != null) {
          lineBuffer.add(line);
          if (lineBuffer.size() == txSize) {
            break;
          }
        }

        // Catch when we've read all the lines in the file.
        if (line == null) {
          hasLinesLeft = false;
        }

        /*
         * Parse the lines in the buffer and write them into the database.
         * Repeat this whole process if the commit fails (in other words,
         * repeat it over and over until the commit succeeds).
         */
        int txFailCount = 0;
        while (true) {
          for (int i = 0; i < lineBuffer.size(); i++) {
            /*
             * Here we parse the line into a map of the entity's properties.
             * Date-type fields (birthday, creationDate, ...) need to be
             * converted to the number of milliseconds since January 1, 1970,
             * 00:00:00 GMT. This is the format expected to be returned for
             * these fields by LDBC SNB benchmark queries, although the format
             * in the dataset files are things like "1989-12-04" and
             * "2010-03-17T23:32:10.447+0000". We could do this conversion
             * "live" during the benchmark, but that would detract from the
             * performance numbers' reflection of true database performance
             * since it would add to the client-side query processing overhead.
             */
            String[] fieldValues = lineBuffer.get(i).split("\\|");
            Map<Object, Object> propMap = new HashMap<>();
            for (int j = 0; j < fieldValues.length; j++) {
              if (fieldNames[j].equals("id")) {
                propMap.put(T.id,
                    new UInt128(idSpace, Long.decode(fieldValues[j])));
              } else if (fieldNames[j].equals("birthday")) {
                propMap.put(fieldNames[j], String.valueOf(
                    birthdayDateFormat.parse(fieldValues[j]).getTime()));
              } else if (fieldNames[j].equals("creationDate")) {
                propMap.put(fieldNames[j], String.valueOf(
                    creationDateDateFormat.parse(fieldValues[j]).getTime()));
              } else {
                propMap.put(fieldNames[j], fieldValues[j]);
              }
            }

            // Don't forget to add the label!
            propMap.put(T.label, label);

            List<Object> keyValues = new ArrayList<>();
            propMap.forEach((key, val) -> {
              keyValues.add(key);
              keyValues.add(val);
            });

            graph.addVertex(keyValues.toArray());
          }

          if ((System.currentTimeMillis() - startReportIntervalMillis) / 1000l
              > reportInterval) {
            long loadTimeSeconds =
                (System.currentTimeMillis() - startLoadMillis) / 1000l;

            System.out.println(String.format(
                "  %d vertices loaded, %d vertices per second, %d minutes "
                + "elapsed",
                linesProcessed - 1, (linesProcessed - 1) / loadTimeSeconds,
                loadTimeSeconds / 60l));

            startReportIntervalMillis = System.currentTimeMillis();
          }

          try {
            graph.tx().commit();
            linesProcessed += lineBuffer.size();

            break;
          } catch (Exception e) {
            /*
             * The transaction failed due to either a conflict or a timeout. In
             * this case we want to retry the transaction, but only up to the
             * txRetries limit.
             */
            txFailCount++;

            if (txFailCount > txRetries) {
              throw new RuntimeException(String.format("ERROR: Transaction "
                  + "failed %d times (file lines [%d,%d]), aborting...",
                  txFailCount, linesProcessed + 1,
                  linesProcessed + lineBuffer.size()));
            }
          }
        }
      }

      inFile.close();

      long loadTimeSeconds =
          (System.currentTimeMillis() - startLoadMillis) / 1000l;
      System.out.println(String.format(
          "  %d vertices loaded, %d vertices per second, %d minutes "
          + "elapsed",
          linesProcessed - 1, (linesProcessed - 1) / loadTimeSeconds,
          loadTimeSeconds / 60l));
    }
  }
}