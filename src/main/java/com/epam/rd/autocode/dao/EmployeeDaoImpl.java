package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class EmployeeDaoImpl implements EmployeeDao {
    private static final String SELECT_FROM_EMPLOYEE_BY_ID = "select * from EMPLOYEE where ID = %s";
    private static final String SELECT_ALL = "select * from EMPLOYEE";
    private static final String DELETE = "delete from EMPLOYEE where ID = '%s'";
    private static final String ID = "ID";
    private static final String FIRSTNAME = "FIRSTNAME";
    private static final String LASTNAME = "LASTNAME";
    private static final String MIDDLENAME = "MIDDLENAME";
    private static final String POSITION = "POSITION";
    private static final String HIREDATE = "HIREDATE";
    private static final String SALARY = "SALARY";
    private static final String MANAGER = "MANAGER";
    private static final String DEPARTMENT = "DEPARTMENT";
    private static final String UPDATE = "update EMPLOYEE set " +
            "FIRSTNAME = '%s', " +
            "LASTNAME = '%s', " +
            "MIDDLENAME = '%s', " +
            "POSITION = '%s', " +
            "HIREDATE = '%s', " +
            "SALARY = '%s', " +
            "MANAGER = '%s', " +
            "DEPARTMENT = '%s', " +
            "' where ID = '%s'";
    private static final String INSERT = "insert into EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, HIREDATE," +
            " SALARY, MANAGER, DEPARTMENT) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        return Optional.ofNullable(getEmployee(String.format(SELECT_FROM_EMPLOYEE_BY_ID, Id)));
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employees = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SELECT_ALL);

            while (resultSet.next()) {
                BigInteger id = new BigInteger(resultSet.getString(ID));
                FullName fullName = new FullName(
                        resultSet.getString(FIRSTNAME),
                        resultSet.getString(LASTNAME),
                        resultSet.getString(MIDDLENAME)
                );
                Position position = Position.valueOf(resultSet.getString(POSITION));
                LocalDate hiredate = resultSet.getDate(HIREDATE).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal(SALARY);
                BigInteger manager = new BigInteger(String.valueOf(resultSet.getInt(MANAGER)));
                BigInteger department = new BigInteger(String.valueOf(resultSet.getInt(DEPARTMENT)));

                Employee employee = new Employee(id, fullName, position, hiredate, salary, manager, department);
                employees.add(employee);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employees;
    }

    @Override
    public Employee save(Employee employee) {
        String firstName = employee.getFullName().getFirstName();
        String lastName = employee.getFullName().getLastName();
        String middleName = employee.getFullName().getMiddleName();
        Position position = employee.getPosition();
        LocalDate hiredate = employee.getHired();
        BigDecimal salary = employee.getSalary();
        BigInteger managerId = employee.getManagerId();
        BigInteger departmentId = employee.getDepartmentId();
        BigInteger id = employee.getId();

        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            if (getById(employee.getId()).isPresent()) {
                statement.executeUpdate(String.format(UPDATE, firstName, lastName, middleName, position, hiredate,
                        salary, managerId, departmentId, id));
            } else {
                statement.executeUpdate(String.format(INSERT, id, firstName, lastName, middleName, position, hiredate,
                        salary, managerId, departmentId, id));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return getEmployee(String.format(SELECT_FROM_EMPLOYEE_BY_ID, employee.getId()));
    }

    @Override
    public void delete(Employee employee) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(String.format(DELETE, employee.getId().toString()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        return getAll().stream()
                .filter(employee -> employee.getDepartmentId().equals(department.getId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        return getAll().stream()
                .filter(e -> e.getManagerId().equals(employee.getId()))
                .collect(Collectors.toList());
    }

    private Employee getEmployee(String query) {
        Employee employee = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                Position position = getPosition(resultSet);
                FullName fullName = getFullName(resultSet);
                BigInteger id = new BigInteger(resultSet.getString(ID));
                LocalDate hired = getHiredate(resultSet);
                BigDecimal salary = resultSet.getBigDecimal(SALARY);
                BigInteger managerId = BigInteger.valueOf(resultSet.getInt(MANAGER));
                BigInteger depatmentId = BigInteger.valueOf(resultSet.getInt(DEPARTMENT));

                employee = new Employee(id, fullName, position, hired, salary, managerId, depatmentId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }

    private LocalDate getHiredate(ResultSet resultSet) throws SQLException {
        return resultSet.getDate(HIREDATE).toLocalDate();
    }

    private FullName getFullName(ResultSet resultSet) throws SQLException {
        String firstName = resultSet.getString(FIRSTNAME);
        String lastName = resultSet.getString(LASTNAME);
        String middleName = resultSet.getString(MIDDLENAME);
        return new FullName(firstName, lastName, middleName);
    }

    private Position getPosition(ResultSet resultSet) throws SQLException {
        return Position.valueOf(resultSet.getString(POSITION).toUpperCase());
    }
}