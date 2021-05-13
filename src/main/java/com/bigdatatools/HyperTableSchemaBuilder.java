package com.bigdatatools;

import com.tableau.hyperapi.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
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
                SqlType colType = getHyperSqlType( colName , colMap.get(colName).dataType());
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

    private static SqlType getHyperSqlType(String colName, DataType type) {

        if ( type.typeName().equals("integer") )
            return integer();
        else if ( type.typeName().equals("string") )
            return text();
        else if ( type.typeName().equals("long") )
            return bigInt();
        else if ( type instanceof DecimalType && ((DecimalType)type).precision() <= 18 && type instanceof DecimalType && ((DecimalType)type).scale() >= 0 )
            return numeric(((DecimalType) type).precision(), ((DecimalType) type).scale());
        else {
            System.out.println("Warning: Column " + colName + "is defined in json schema as " + type.typeName() + " , but implicitly converted to text format in hyper");
            return text();
        }
    }

}
