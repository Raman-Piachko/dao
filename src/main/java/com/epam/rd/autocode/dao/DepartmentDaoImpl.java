package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDaoImpl implements DepartmentDao {
    private static final String SELECT_DEPARTMENT_BY_ID = "select * from DEPARTMENT where ID = %s";
    private static final String SELECT_ALL = "select * from DEPARTMENT";
    private static final String DELETE = "delete from DEPARTMENT where ID = '%s'";
    private static final String UPDATE = "update DEPARTMENT set NAME = '%s', LOCATION = '%s' where ID = '%s'";
    private static final String INSERT = "insert into DEPARTMENT (ID, NAME, LOCATION) values ('%s', '%s', '%s')";
    private static final String ID = "ID";
    private static final String NAME = "NAME";
    private static final String LOCATION = "LOCATION";

    @Override
    public Optional<Department> getById(BigInteger Id) {
            return Optional.ofNullable(getDepartment(String.format(SELECT_DEPARTMENT_BY_ID, Id)));
    }

    @Override
    public List<Department> getAll() {
        List<Department> departments = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SELECT_ALL);
            while (resultSet.next()) {
                Department department = new Department(
                        new BigInteger(resultSet.getString(ID)),
                        resultSet.getString(NAME),
                        resultSet.getString(LOCATION));
                departments.add(department);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return departments;
    }

    @Override
    public Department save(Department department) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            if (getById(department.getId()).isPresent()) {
                statement.executeUpdate(String.format(
                        UPDATE,
                        department.getName(),
                        department.getLocation(),
                        department.getId().toString()));
            } else {
                statement.executeUpdate(String.format(
                        INSERT,
                        department.getId().toString(),
                        department.getName(),
                        department.getLocation())
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return department;
    }

    @Override
    public void delete(Department department) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(String.format(DELETE, department.getId().toString()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Department getDepartment(String query) {
        Department department = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                department = new Department(
                        new BigInteger(resultSet.getString(ID)),
                        resultSet.getString(NAME),
                        resultSet.getString(LOCATION)
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return department;
    }
}