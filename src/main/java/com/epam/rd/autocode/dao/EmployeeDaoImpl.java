package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;
import com.epam.rd.autocode.exception.DaoException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.epam.rd.autocode.dao.util.BigIntegerUtil.getBigInteger;

public class EmployeeDaoImpl implements EmployeeDao {

    private static final String SQL_QUERY_SELECT_BY_ID = "SELECT id,  firstname, lastname, middlename, position, " +
            "hiredate, salary, manager, department FROM employee WHERE id = ?";
    private static final String SQL_QUERY_SELECT_ALL = "SELECT id,  firstname, lastname, middlename, position, hiredate," +
            " salary, manager, department FROM employee";
    private static final String SQL_QUERY_SELECT_BY_MANAGER = "SELECT id,  firstname, lastname, middlename, position," +
            " hiredate, salary, manager, department FROM employee WHERE manager = ?";
    private static final String SQL_QUERY_SELECT_BY_DEPARTMENT =
            "SELECT * FROM employee WHERE department = ?";
    private static final String SQL_QUERY_DELETE = "DELETE FROM employee WHERE id = ?";
    private static final String SQL_QUERY_INSERT = "INSERT INTO employee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_QUERY_UPDATE = "UPDATE employee SET "
            + "firstname = ?, lastname = ?, middlename = ?, "
            + "position = ?, manager = ?, hiredate = ?, "
            + "salary = ?, department = ? "
            + "WHERE id = ?";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SALARY = "salary";
    private static final String COLUMN_HIREDATE = "hiredate";
    private static final String COLUMN_FIRSTNAME = "firstname";
    private static final String COLUMN_LASTNAME = "lastname";
    private static final String COLUMN_MIDDLENAME = "middlename";
    private static final String COLUMN_POSITION = "position";
    private static final String COLUMN_MANAGER = "manager";
    private static final String COLUMN_DEPARTMENT = "department";

    @Override
    public Optional<Employee> getById(BigInteger id) {
        Optional<Employee> employee = Optional.empty();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_ID)) {
            statement.setObject(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                employee = Optional.of(createEmployee(resultSet));
            }

            return employee;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getById", e);
        }
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employees = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_ALL)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }
            return employees;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getAll", e);
        }
    }

    @Override
    public Employee save(Employee employee) {
        FullName fullName = employee.getFullName();
        String firstName = fullName.getFirstName();
        String lastName = fullName.getLastName();
        String middleName = fullName.getMiddleName();
        Position position = employee.getPosition();
        LocalDate hiredate = employee.getHired();
        BigDecimal salary = employee.getSalary();
        BigInteger managerId = employee.getManagerId();
        BigInteger departmentId = employee.getDepartmentId();
        BigInteger id = employee.getId();

        Optional<Employee> foundEmployee = getById(employee.getId());

        if (foundEmployee.isPresent()) {
            update(firstName, lastName, middleName, position, hiredate,
                    salary, managerId, departmentId, id);
        } else {
            insert(firstName, lastName, middleName, position, hiredate,
                    salary, managerId, departmentId, id);
        }

        return employee;
    }

    private void insert(String firstName, String lastName, String middleName, Position position, LocalDate hiredate,
                        BigDecimal salary, BigInteger managerId, BigInteger departmentId, BigInteger id) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_INSERT)) {
            statement.setObject(1, id);
            statement.setString(2, firstName);
            statement.setString(3, lastName);
            statement.setString(4, middleName);
            statement.setString(5, position.toString());
            statement.setObject(6, managerId);
            statement.setDate(7, java.sql.Date.valueOf(hiredate));
            statement.setBigDecimal(8, salary);
            statement.setObject(9, departmentId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at insert", e);
        }
    }

    private void update(String firstName, String lastName, String middleName, Position position, LocalDate hiredate,
                        BigDecimal salary, BigInteger managerId, BigInteger departmentId, BigInteger id) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_UPDATE)) {
            statement.setString(1, firstName);
            statement.setString(2, lastName);
            statement.setString(3, middleName);
            statement.setString(4, position.toString());
            statement.setObject(5, managerId);
            statement.setDate(6, java.sql.Date.valueOf(hiredate));
            statement.setBigDecimal(7, salary);
            statement.setObject(8, departmentId);
            statement.setObject(9, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at update", e);
        }
    }

    @Override
    public void delete(Employee employee) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_DELETE)) {
            statement.setObject(1, employee.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at delete", e);
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> employees = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_DEPARTMENT)) {
            statement.setLong(1, department.getId().longValue());
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }

            return employees;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getByDepartment", e);
        }
    }

    @Override
    public List<Employee> getByManager(Employee manager) {
        List<Employee> employees = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_MANAGER)) {
            statement.setObject(1, manager.getId());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }
            return employees;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getByManager", e);
        }
    }

    private Employee createEmployee(ResultSet resultSet) {
        try {
            Position position = getPosition(resultSet);
            FullName fullName = getFullName(resultSet);
            BigInteger id = getBigInteger(resultSet, COLUMN_ID);
            LocalDate hired = getHired(resultSet);
            BigDecimal salary = resultSet.getBigDecimal(COLUMN_SALARY);
            BigInteger managerId = getBigInteger(resultSet, COLUMN_MANAGER);
            BigInteger departmentId = getBigInteger(resultSet, COLUMN_DEPARTMENT);
            return new Employee(id, fullName, position, hired, salary, managerId, departmentId);
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at createEmployee", e);
        }
    }

    private LocalDate getHired(ResultSet resultSet) throws SQLException {
        Date rawDate = resultSet.getDate(COLUMN_HIREDATE);

        return rawDate.toLocalDate();
    }

    private FullName getFullName(ResultSet resultSet) throws SQLException {
        String firstName = resultSet.getString(COLUMN_FIRSTNAME);
        String lastName = resultSet.getString(COLUMN_LASTNAME);
        String middleName = resultSet.getString(COLUMN_MIDDLENAME);

        return new FullName(firstName, lastName, middleName);
    }

    private Position getPosition(ResultSet resultSet) throws SQLException {
        String rawPosition = resultSet.getString(COLUMN_POSITION);

        return Position.valueOf(rawPosition.toUpperCase());
    }
}