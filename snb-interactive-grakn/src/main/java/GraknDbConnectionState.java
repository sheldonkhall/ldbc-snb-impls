import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import com.ldbc.driver.DbConnectionState;

import java.io.IOException;

/**
 * @author Felix Chapman
 */
public class GraknDbConnectionState extends DbConnectionState {

    private final GraknSession session;
    private final GraknGraph graph;

    GraknDbConnectionState(GraknSession session) {
        this.session = session;
        this.graph = session.open(GraknTxType.READ);
    }

    @Override
    public void close() throws IOException {
        graph.close();
        session.close();
    }

    GraknGraph graph() {
        return graph;
    }
}
