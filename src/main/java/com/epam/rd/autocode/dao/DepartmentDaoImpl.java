package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.exception.DaoException;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.epam.rd.autocode.dao.util.BigIntegerUtil.getBigInteger;

public class DepartmentDaoImpl implements DepartmentDao {

    private static final String SQL_QUERY_SELECT_BY_ID = "SELECT id, name, location FROM department WHERE id = ?";
    private static final String SQL_QUERY_DELETE = "DELETE FROM department WHERE id = ?";
    private static final String SQL_QUERY_SELECT_ALL = "SELECT id, name, location FROM department";
    private static final String SQL_QUERY_UPDATE = "UPDATE department SET name = ?, location = ? WHERE id = ?";
    private static final String SQL_QUERY_INSERT = "INSERT INTO department VALUES (?, ?, ?)";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_LOCATION = "location";

    @Override
    public Optional<Department> getById(BigInteger id) {
        Optional<Department> department = Optional.empty();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_ID)) {
            statement.setLong(1, id.longValue());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                department = Optional.of(createDepartment(resultSet));
            }
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getById", e);
        }
        return department;
    }

    @Override
    public List<Department> getAll() {
        List<Department> departments = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_ALL)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Department department = createDepartment(resultSet);
                departments.add(department);
            }
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getAll", e);
        }
        return departments;
    }

    @Override
    public Department save(Department department) {
        String name = department.getName();
        String location = department.getLocation();
        BigInteger id = department.getId();
        Optional<Department> foundDepartment = getById(department.getId());
        if (foundDepartment.isPresent()) {
            update(id, name, location);
        } else {
            insert(id, name, location);
        }
        return department;
    }

    private void insert(BigInteger id, String name, String location) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_INSERT)) {
            statement.setObject(1, id);
            statement.setString(2, name);
            statement.setString(3, location);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at insert", e);
        }
    }

    private void update(BigInteger id, String name, String location) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_UPDATE)) {
            statement.setString(1, name);
            statement.setString(2, location);
            statement.setObject(3, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at update", e);
        }
    }

    @Override
    public void delete(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_DELETE)) {
            statement.setObject(1, department.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at delete", e);
        }
    }

    private Department createDepartment(ResultSet resultSet) {
        Department department;
        try {
            BigInteger id = getBigInteger(resultSet, COLUMN_ID);
            String name = resultSet.getString(COLUMN_NAME);
            String location = resultSet.getString(COLUMN_LOCATION);
            department = new Department(id, name, location);
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at createDepartment", e);
        }
        return department;
    }
}