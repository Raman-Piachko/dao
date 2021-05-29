package com.epam.rd.autocode.dao.util;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class BigIntegerUtil {

    private BigIntegerUtil() {
    }

    public static BigInteger getBigInteger(ResultSet resultSet, String columnName) throws SQLException {
        String value = resultSet.getString(columnName);
        return value == null ? BigInteger.ZERO : new BigInteger(value);
    }
}