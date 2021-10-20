package io.github.tenghuanhe;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class FdbTable extends AbstractTable implements ScannableTable {

    private final String table;
    private final RelDataType dataType;

    public FdbTable(String table, RelDataType dataType) {
        this.table = table;
        this.dataType = dataType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return new FdbEnumerable(table);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(dataType);
    }
}
