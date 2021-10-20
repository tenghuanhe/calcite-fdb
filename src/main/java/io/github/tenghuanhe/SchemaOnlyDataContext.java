package io.github.tenghuanhe;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

/**
 * A simple data context only with schema information.
 */
public final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
        this.schema = calciteSchema.plus();
    }

    @Override
    public SchemaPlus getRootSchema() {
        return schema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(final String name) {
        return null;
    }
}