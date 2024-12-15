package com.lacus.flink.executor;

import com.lacus.core.flink.model.SqlCommandCall;
import com.lacus.flink.config.Configurations;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ExecuteSql {

    public static JobID exeSql(List<String> sqlList, TableEnvironment tEnv) {
        Parser parser = ((TableEnvironmentInternal) tEnv).getParser();
        List<ModifyOperation> modifyOperationList = new ArrayList<>();
        for (String stmt : sqlList) {
            Operation operation = parser.parse(stmt).get(0);
            log.info("operation={}", operation.getClass().getSimpleName());
            switch (operation.getClass().getSimpleName()) {
                case "ShowTablesOperation":
                case "ShowCatalogsOperation":
                case "ShowCreateTableOperation":
                case "ShowCurrentCatalogOperation":
                case "ShowCurrentDatabaseOperation":
                case "ShowDatabasesOperation":
                case "ShowFunctionsOperation":
                case "ShowModulesOperation":
                case "ShowPartitionsOperation":
                case "ShowViewsOperation":
                case "ExplainOperation":
                case "DescribeTableOperation":
                    tEnv.executeSql(stmt).print();
                    break;
                case "SetOperation":
                    SetOperation setOperation = (SetOperation) operation;
                    Configurations.setSingleConfiguration(tEnv, setOperation.getKey().get(), setOperation.getValue().get());
                    break;
                case "BeginStatementSetOperation":
                case "EndStatementSetOperation":
                    System.out.println("stmt：" + stmt);
                    log.info("stmt：{}", stmt);
                    break;
                case "DropTableOperation":
                case "DropCatalogFunctionOperation":
                case "DropTempSystemFunctionOperation":
                case "DropCatalogOperation":
                case "DropDatabaseOperation":
                case "DropViewOperation":
                case "CreateTableOperation":
                case "CreateViewOperation":
                case "CreateDatabaseOperation":
                case "CreateCatalogOperation":
                case "CreateTableASOperation":
                case "CreateCatalogFunctionOperation":
                case "CreateTempSystemFunctionOperation":
                case "AlterTableOperation":
                case "AlterViewOperation":
                case "AlterDatabaseOperation":
                case "AlterCatalogFunctionOperation":
                case "UseCatalogOperation":
                case "UseDatabaseOperation":
                case "LoadModuleOperation":
                case "UnloadModuleOperation":
                case "NopOperation":
                    ((TableEnvironmentInternal) tEnv).executeInternal(parser.parse(stmt).get(0));
                    break;
                case "SinkModifyOperation":
                    modifyOperationList.add((SinkModifyOperation) operation);
                    break;
                default:
                    log.error("不支持的Operation类型 {}", operation.getClass().getSimpleName());
                    throw new RuntimeException("不支持的语法，sql=" + stmt);
            }
        }
        System.out.println("modifyOperationList=" + modifyOperationList);
        TableResult tableResult = ((TableEnvironmentInternal) tEnv)
                .executeInternal(modifyOperationList);
        if (tableResult.getJobClient().orElse(null) != null) {
            return tableResult.getJobClient().get().getJobID();
        }
        throw new RuntimeException("任务运行失败：获取JobID失败");
    }


    /**
     * 执行sql 被com.flink.streaming.core.execute.ExecuteSql 替换
     */
    @Deprecated
    public static void exeSql(List<SqlCommandCall> sqlCommandCallList, TableEnvironment tEnv, StatementSet statementSet) {
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            switch (sqlCommandCall.getSqlCommand()) {
                case SET:
                    Configurations.setSingleConfiguration(tEnv, sqlCommandCall.getOperands()[0],
                            sqlCommandCall.getOperands()[1]);
                    break;
                case INSERT_INTO:
                case INSERT_OVERWRITE:
                    statementSet.addInsertSql(sqlCommandCall.getOperands()[0]);
                    break;
                case SELECT:
                case SHOW_CATALOGS:
                case SHOW_DATABASES:
                case SHOW_MODULES:
                case SHOW_TABLES:
                    break;
                // 兼容sql-client.sh的用法
                case BEGIN_STATEMENT_SET:
                case END:
                    break;
                default:
                    tEnv.executeSql(sqlCommandCall.getOperands()[0]);
                    break;
            }
        }
    }
}
