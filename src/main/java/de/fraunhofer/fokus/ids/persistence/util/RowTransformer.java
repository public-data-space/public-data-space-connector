package de.fraunhofer.fokus.ids.persistence.util;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class RowTransformer {

    public List<JsonObject> transform(RowSet<Row> rows) {

        List<JsonObject> list = new ArrayList<>();
        for (Row row : rows) {
            int size = row.size();
            JsonObject jsonObject = new JsonObject();
            for (int i = 0; i < size; i++) {
                String columnName = row.getColumnName(i);
                Object value = row.getValue(i);
                if (value != null) {
                    if (value instanceof LocalDateTime) {
                        jsonObject.put(columnName, ((LocalDateTime) value).atZone(
                                ZoneId.systemDefault()).toInstant());
                    } else if(value instanceof String[]) {
                        jsonObject.put(columnName, new JsonArray(Arrays.asList(row.getStringArray(i))));
                    } else {
                        try {
                            jsonObject.put(columnName, new JsonObject(value.toString()));
                        } catch (DecodeException e){
                            jsonObject.put(columnName, row.getValue(i));
                        }
                    }
                }
            }
            list.add(jsonObject);
        }
        return list;
    }
}
