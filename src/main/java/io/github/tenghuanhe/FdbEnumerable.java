package io.github.tenghuanhe;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.List;

public class FdbEnumerable extends AbstractEnumerable<Object[]> {

    private final Subspace subspace;

    public FdbEnumerable(String table) {
        this.subspace = new Subspace(Tuple.from(FdbHandler.defaultSchema(), table));
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return Linq4j.enumerator(scan());
    }

    private List<Object[]> scan() {
        return FdbHandler.db().run(tx -> {
            List<Object[]> rows = new ArrayList<>();
            for (KeyValue kv : tx.getRange(subspace.range())) {
                Object[] row = Tuple.fromBytes(kv.getValue()).getItems().toArray();
                rows.add(row);
            }
            return rows;
        });
    }
}
