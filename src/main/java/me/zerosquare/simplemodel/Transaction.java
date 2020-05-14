package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.sql.Connection;

public class Transaction {

    @FunctionalInterface
    public interface TransactionExecutor {
        void execute() throws Exception;
    }

    /**
     * Any SQL statement execution in transactionExecutor will use transaction connection
     * Successful execution of transactionExecutor will commit changes.
     * To rollback transaction, throw any Exception
     * @param transactionExecutor
     * @throws Exception
     */
    public static void execute(TransactionExecutor transactionExecutor) throws Exception {
        Logger.i("begin transaction");

        Connection conn = null;
        boolean autoCommitWas = false;

        try {
            // prepare no auto commit connection
            conn = Connector.makeDBConnection();

            autoCommitWas = conn.getAutoCommit();

            conn.setAutoCommit(false);

            Connector.enableCustomConnection(conn, null);

            // all statements will be executed by prepared custom connection
            transactionExecutor.execute();

            conn.commit();

            Logger.d("transaction committed");
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }

            Logger.e("transaction rolled back - %s", Logger.getExceptionString(e));

            throw e;
        } finally {
            if (conn != null) {
                conn.setAutoCommit(autoCommitWas);
            }

            Connector.disableCustomConnection();

            Connector.tryClose(conn);
        }

        Logger.i("end transaction");
    }
}
