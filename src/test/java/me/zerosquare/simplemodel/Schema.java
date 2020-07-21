package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Schema {
    public static void loadSchema(String filename) throws Exception {
        Logger.i("current path is: " + Paths.get("").toAbsolutePath().toString());

        // load schema and create tables
        String schema = new String(Files.readAllBytes(Paths.get(filename)));

        Model.execute(schema, pst -> pst.executeUpdate());
    }

}
