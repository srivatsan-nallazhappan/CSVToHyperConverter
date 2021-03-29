package com.bigdatatools;

import com.tableau.hyperapi.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tableau.hyperapi.Nullability.*;
import static com.tableau.hyperapi.SqlType.*;

public class HyperTableSchemaBuilder {

    public static TableDefinition getHyperTableDefinition ( String tableName , Path schemaPath, List<String> colNames ) throws IOException {

        Map<String, StructField> colMap = parseJsonSchema(schemaPath);
        List<TableDefinition.Column> colList = colNames.stream().map( colName -> {
                SqlType colType = getHyperSqlType( colMap.get(colName).dataType());
                Nullability nullability = colMap.get(colName).nullable() ? NULLABLE: NOT_NULLABLE;
                return new TableDefinition.Column (colName,colType,nullability);
        }
        ).collect(Collectors.toList());

        return new TableDefinition( new TableName(tableName),colList );

    }

    private static Map<String, StructField> parseJsonSchema(Path schemaFilePath) throws IOException {
        String text = new String(Files.readAllBytes(schemaFilePath), StandardCharsets.UTF_8);
        StructType schema = (StructType) DataType.fromJson(text);
        return Arrays.stream(schema.fields()).collect( Collectors.toMap( StructField::name, Function.identity()));
    }

    private static SqlType getHyperSqlType(DataType type) {
        System.out.println("Column type name is " + type.typeName());
        switch ( type.typeName() )
        {
            case "integer" :
                return integer();
            case "string" :
                return text();
            default :
                System.out.println("Unknown type" + type.typeName());
                return null;
        }
    }

}
