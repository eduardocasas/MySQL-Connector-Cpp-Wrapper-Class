/* 
 * File:   mysqlconn_wrapper.h
 * Author: Eduardo Casas (www.eduardocasas.com)
 *
 * Created on February 24, 2013, 5:07 PM
 */

#ifndef MYSQLCONN_WRAPPER_H
#define	MYSQLCONN_WRAPPER_H

#include "mysql_connection.h"
	
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

using namespace std;

class MySQLConnWrapper
{
          
    public:
        
        /* Your MySQL server settings */
        MySQLConnWrapper()
        {
            host     = "tcp://127.0.0.1:3306";
            user     = "root";
            password = "";
        };
        ~MySQLConnWrapper();
        void manageException(sql::SQLException& e);
        void connect();
        void switchDb(const string& db_name);
        void prepare(const string& query);
        void setInt(const int& num, const int& data);
        void setString(const int& num, const string& data);
        void execute(const string& query = "");
        bool fetch();
        string print(const string& field);
        string print(const int& index);

    private:
        
        string host;
        string user;
        string password;
        sql::Driver* driver;
        sql::Connection* con;
        sql::Statement* stmt;
        sql::PreparedStatement* prep_stmt;
        sql::ResultSet* res;

};


#endif	/* MYSQLCONN_WRAPPER_H */


