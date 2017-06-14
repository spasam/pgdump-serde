package com.pasam.hive.serde.pg;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * SerDe to parse pg_dump output (data only).
 *
 * @author Seshu Pasam <hive@pasam.com>
 */
public class PgDumpSerDe
        extends AbstractSerDe {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(
            "yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[[ ][.SSS]][XXX][X]");

    private ObjectInspector inspector;
    private List<TypeInfo> columnTypes;
    private List<Object> row;
    private int numCols;

    @Override
    public void initialize(final Configuration configuration, final Properties properties) throws SerDeException {
        final List<String> columnNames = Arrays.asList(properties.getProperty(serdeConstants.LIST_COLUMNS).split(","));
        this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES));

        this.numCols = columnNames.size();
        this.row = new ArrayList<Object>(numCols);
        for (int i = 0; i < numCols; i++) {
            row.add(null);
        }

        final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);
        if (columnTypes.size() != numCols) {
            throw new SerDeException("Column names and types mis-match");
        }
        for (int i = 0; i < numCols; i++) {
            TypeInfo typeInfo = columnTypes.get(i);
            if (typeInfo == null || !(typeInfo instanceof PrimitiveTypeInfo)) {
                throw new SerDeException("Unsuported type: " + typeInfo);
            }

            ObjectInspector oi =
                    PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
            columnOIs.add(oi);
        }

        this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        throw new SerDeException("Not implemented");
    }

    @Override
    public Object deserialize(final Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        String[] read = rowText.toString().split("\t");
        if (read == null || read.length < numCols) {
            throw new SerDeException("Error reading row: " + blob);
        }

        for (int i = 0; i < numCols; i++) {
            if (read[i] == "\\N") {
                row.set(i, null);
                continue;
            }

            PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) columnTypes.get(i);
            switch (typeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                row.set(i, read[i].charAt(0) == 't' ? Boolean.TRUE : Boolean.FALSE);
                break;
            case BYTE:
                row.set(i, Byte.parseByte(read[i]));
                break;
            case SHORT:
                row.set(i, Short.parseShort(read[i]));
                break;
            case INT:
                row.set(i, Integer.parseInt(read[i]));
                break;
            case LONG:
                row.set(i, Long.parseLong(read[i]));
                break;
            case FLOAT:
                row.set(i, Float.parseFloat(read[i]));
                break;
            case DOUBLE:
                row.set(i, Double.parseDouble(read[i]));
                break;
            case DECIMAL:
                DecimalFormat df = new DecimalFormat();
                df.setParseBigDecimal(true);
                row.set(i, df.parse(read[i], new ParsePosition(0)));
                break;
            case STRING:
                row.set(i, StringEscapeUtils.unescapeJava(read[i]));
                break;
            case TIMESTAMP:
                TemporalAccessor temporalAccessor = DATE_TIME_FORMATTER.parseBest(read[i], ZonedDateTime::from, LocalDateTime::from);
                if (temporalAccessor instanceof ZonedDateTime) {
                    row.set(i, new Timestamp(((ZonedDateTime) temporalAccessor).toEpochSecond() * 1000L));
                } else if (temporalAccessor instanceof LocalDateTime) {
                    row.set(i,
                            new Timestamp(((LocalDateTime) temporalAccessor).atZone(ZoneId.systemDefault()).toEpochSecond() * 1000L));
                } else {
                    row.set(i, null);
                }
                break;
            default:
                row.set(i, null);
            }
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}