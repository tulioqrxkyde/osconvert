/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.osprojects.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public Connection getConnection() {
        System.out.println("Conectando ao Banco de Dados...");
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost/bibliotecaeeepjwm", 
                    "root", "");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
