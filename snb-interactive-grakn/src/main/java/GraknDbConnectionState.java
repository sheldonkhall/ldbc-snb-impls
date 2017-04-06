import ai.grakn.GraknSession;
import com.ldbc.driver.DbConnectionState;

import java.io.IOException;

/**
 * @author Felix Chapman
 */
public class GraknDbConnectionState extends DbConnectionState {

    private final GraknSession session;

    GraknDbConnectionState(GraknSession session) {
        this.session = session;
    }

    @Override
    public void close() throws IOException {
        session.close();
    }

    GraknSession session() {
        return session;
    }
}
