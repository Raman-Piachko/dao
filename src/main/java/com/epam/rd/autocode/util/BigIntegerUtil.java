package com.epam.rd.autocode.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BigIntegerUtil {
    public static BigInteger getBigInteger(ResultSet resultSet, String columnName) throws SQLException {
        BigDecimal value = resultSet.getBigDecimal(columnName);
        return value == null ? BigInteger.ZERO : value.toBigInteger();
    }
}