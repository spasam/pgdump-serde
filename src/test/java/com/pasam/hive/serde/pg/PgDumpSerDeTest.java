package com.pasam.hive.serde.pg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public final class PgDumpSerDeTest {
    private final PgDumpSerDe serde = new PgDumpSerDe();
    private final Properties props = new Properties();

    @Before
    public void setup() throws Exception {
        props.put(serdeConstants.LIST_COLUMNS,
                "ttint,tsint,tint,tbint,tfloat,tdouble,tdecimal,tbtrue,tbfalse,ttstamp1,ttstamp2,ttstamp3,ttstamp4,ttstamp5,tstring,tnstring,tmstring,testring");
        props.put(serdeConstants.LIST_COLUMN_TYPES,
                "tinyint,smallint,int,bigint,float,double,decimal,boolean,boolean,timestamp,timestamp,timestamp,timestamp,timestamp,string,string,string,string");
    }

    @Test
    public void testDeserialize() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("pg.dump")));

        serde.initialize(null, props);

        final Text in = new Text(reader.readLine());
        final List<?> row = (List<?>) serde.deserialize(in);

        assertEquals(18, row.size());
        assertEquals(new Byte((byte) 1), row.get(0));
        assertEquals(new Short((short) 100), row.get(1));
        assertEquals(new Integer(65536), row.get(2));
        assertEquals(new Long(1234567890), row.get(3));
        assertEquals(new Float(123.45), row.get(4));
        assertEquals(new Double(1234567890.12), row.get(5));
        assertTrue(row.get(6) instanceof BigDecimal);
        assertEquals(Boolean.TRUE, row.get(7));
        assertEquals(Boolean.FALSE, row.get(8));
        assertTrue(row.get(9) instanceof Timestamp);
        assertTrue(row.get(10) instanceof Timestamp);
        assertTrue(row.get(11) instanceof Timestamp);
        assertTrue(row.get(12) instanceof Timestamp);
        assertTrue(row.get(13) instanceof Timestamp);
        assertEquals("Hello Seshu", row.get(14));
        assertEquals(null, row.get(15));
        assertEquals("A\tB\\tC\nD\\nE\\rF\nG\\H\\I\\\\J\n", row.get(16));
        assertEquals("", row.get(17));
    }
}
