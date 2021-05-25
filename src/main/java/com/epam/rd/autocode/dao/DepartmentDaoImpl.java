package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.exception.DaoException;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.epam.rd.autocode.util.BigIntegerUtil.getBigInteger;

public class DepartmentDaoImpl implements DepartmentDao {
    private static final String SQL_QUERY_SELECT_BY_ID = "SELECT * FROM department WHERE id = ?";
    private static final String SQL_QUERY_DELETE = "DELETE FROM department WHERE id = ?";
    private static final String SQL_QUERY_SELECT_ALL = "SELECT * FROM department";
    private static final String SQL_QUERY_UPDATE = "UPDATE department SET name = '%s', location = '%s' WHERE id = '%s'";
    private static final String SQL_QUERY_INSERT = "INSERT INTO department (id, name, location) VALUES ('%s', '%s', '%s')";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_LOCATION = "location";

    @Override
    public Optional<Department> getById(BigInteger id) {
        Optional<Department> department = Optional.empty();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_ID)) {
            statement.setLong(1, id.longValue());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    department = Optional.of(createDepartment(resultSet));
                }
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
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY_SELECT_ALL)) {
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
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            Optional<Department> foundDepartment = getById(department.getId());
            if (foundDepartment.isPresent()) {
                statement.executeUpdate(String.format(
                        SQL_QUERY_UPDATE,
                        department.getName(),
                        department.getLocation(),
                        department.getId().toString()));
            } else {
                statement.executeUpdate(String.format(
                        SQL_QUERY_INSERT,
                        department.getId().toString(),
                        department.getName(),
                        department.getLocation())
                );
            }
            return department;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at save", e);
        }
    }

    @Override
    public void delete(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_DELETE)) {
            statement.setLong(1, department.getId().longValue());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at delete", e);
        }
    }

    private Department createDepartment(ResultSet resultSet) {
        Department department;
        try {
            BigInteger id = getBigInteger(resultSet,COLUMN_ID);
            String name = resultSet.getString(COLUMN_NAME);
            String location = resultSet.getString(COLUMN_LOCATION);
            department = new Department(id, name, location);

        } catch (SQLException e) {
            throw new DaoException("Something went wrong at createDepartment", e);
        }
        return department;
    }
}