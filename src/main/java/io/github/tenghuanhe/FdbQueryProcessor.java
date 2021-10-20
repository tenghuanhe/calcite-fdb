package io.github.tenghuanhe;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class FdbQueryProcessor {

    private static final String DEPARTMENT_TABLE_NAME = "department";
    private static final String EMPLOYEE_TABLE_NAME = "employee";
    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    public static void main(String[] args) throws SqlParseException {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        schema.add(DEPARTMENT_TABLE_NAME, department(DEPARTMENT_TABLE_NAME, typeFactory));
        schema.add(EMPLOYEE_TABLE_NAME, employee(EMPLOYEE_TABLE_NAME, typeFactory));

        String sql = "SELECT dept.name, count(*) as cnt" +
                " FROM department as dept inner join employee as emp" +
                " on dept.id = emp.dept" +
                " where gender = 'male'" +
                " group by dept.name" +
                " order by cnt desc" +
                " limit 10";
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]");
        System.out.println(sqlNode.toString());

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema, Collections.emptyList(), typeFactory, config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

        SqlNode validatedNode = validator.validate(sqlNode);

        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter converter = new SqlToRelConverter(NOOP_EXPANDER, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());

        RelNode logicalPlan = converter.convertQuery(validatedNode, false, true).rel;
        System.out.println(RelOptUtil.dumpPlan("[Logical plan]", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));

        RelOptPlanner planner = cluster.getPlanner();
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

        logicalPlan = planner.changeTraits(logicalPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(logicalPlan);
        EnumerableRel physicalPlan = (EnumerableRel) planner.findBestExp();
        System.out.println(RelOptUtil.dumpPlan("[Physical plan]", physicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));

        Enumerable<Object[]> result = EnumerableInterpretable.toBindable(Collections.emptyMap(), null, physicalPlan, EnumerableRel.Prefer.ARRAY).bind(new SchemaOnlyDataContext(schema));
        System.out.println("[Result]");
        result.forEach(row -> System.out.println((Arrays.toString(row))));
    }

    public static FdbTable department(String departmentTableName, RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("id", typeFactory.createJavaType(Long.class).getSqlTypeName());
        builder.add("name", typeFactory.createJavaType(String.class).getSqlTypeName());
        return new FdbTable(departmentTableName, builder.build());
    }

    public static FdbTable employee(String employeeTableName, RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("id", typeFactory.createJavaType(Long.class).getSqlTypeName());
        builder.add("gender", typeFactory.createJavaType(String.class).getSqlTypeName());
        builder.add("name", typeFactory.createJavaType(String.class).getSqlTypeName());
        builder.add("dept", typeFactory.createJavaType(Long.class).getSqlTypeName());
        return new FdbTable(employeeTableName, builder.build());
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }
}
