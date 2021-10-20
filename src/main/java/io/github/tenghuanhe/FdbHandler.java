package io.github.tenghuanhe;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class FdbHandler {
    private static final FDB fdb;
    private static final Database db;

    static {
        fdb = FDB.selectAPIVersion(630);
        db = fdb.open();
    }

    public static Database db() {
        return db;
    }

    public static String defaultSchema() {
        return "company";
    }
}
