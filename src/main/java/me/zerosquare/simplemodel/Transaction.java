package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.sql.Connection;
import java.sql.SQLException;

public class Transaction {

    @FunctionalInterface
    public interface TransactionExecutor {
        void execute() throws Exception;
    }

    public static void execute(TransactionExecutor executor) throws Exception {
        // prepare no auto commit connection
        Connection conn = Connector.makeDBConnection();

        boolean autoCommitWas = conn.getAutoCommit();

        conn.setAutoCommit(false);

        Connector.prepareCustomConnection(conn, (c, success) -> {
            try {
                if (success) {
                    c.commit();
                } else {
                    c.rollback();
                }

                c.setAutoCommit(autoCommitWas);
            } catch (SQLException e) {
                Logger.e("fail to commit/rollback transaction - %s", Logger.getExceptionString(e));
            }
        });

        try {
            // in this execution, prepared custom connection will be used
            executor.execute();
        } catch (Exception e) {
            throw e;
        }
    }
}
