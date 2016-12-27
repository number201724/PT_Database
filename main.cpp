#include <iostream>
#include "database.hpp"


PT_Database database("192.168.200.3", 3306, "root", "", "test");

void test_insert()
{
    try
    {
        PT_DBRecordset pRs(database);
        PT_DBQueryParameter parameters;
        parameters.push_back(PT_DBValue("'test' or 1=1"));
        if(pRs.Open("INSERT INTO test VALUES(?, 0)", parameters))
        {
            std::cout << pRs.GetResults().size() << std::endl;
            std::cout << "query record count:" << pRs.GetRecordCount() << std::endl;
            if(pRs.MoveFirst()) 
            {
                do
                {
                    std::string out;

                    pRs.GetFieldValue("test", out);

                    std::cout << "test:" << out << std::endl;
                }
                while(pRs.MoveNext());
            }
            
        }
    }
    catch(PT_DBException &exception)
    {
        std::cout << "catch exception" << std::endl;
        std::cout << exception.get_sqlstate() << std::endl;
        std::cout << exception.get_error() << std::endl;
        database.Close();
    }
}

void test_query()
{
    try
    {
        PT_DBRecordset pRs(database);
        PT_DBQueryParameter parameters;

        parameters.push_back(PT_DBValue("'test' or 1=1"));
        if(pRs.Open("SELECT * FROM test where test = ?", parameters))
        {
            std::cout << pRs.GetResults().size() << std::endl;
            std::cout << "query record count:" << pRs.GetRecordCount() << std::endl;
            if(pRs.MoveFirst()) 
            {
                do
                {
                    std::string out;

                    pRs.GetFieldValue("test", out);

                    std::cout << "test:" << out << std::endl;
                }
                while(pRs.MoveNext());
            }
            
        }
    }
    catch(PT_DBException &exception)
    {
        std::cout << "catch exception" << std::endl;
        std::cout << exception.get_sqlstate() << std::endl;
        std::cout << exception.get_error() << std::endl;
        database.Close();
    }
}

int main(int argc, char *argv[])
{
    if(!database.Open())
    {
        std::cout << "Connect mysqldb failed" << std::endl;
        return 0;
    }

    std::cout << "mysql_is_open:" << database.IsOpen() << std::endl;
    std::cout << "connected mysqldb" << std::endl;

    test_insert();
    test_query();
    /*
    
    try
    {
        database.beginTransaction();
        database.Execute("INSERT INTO test VALUES('DDDDDDDDDDDDDDDDDDD', 0)");
        database.commitTransaction();
    }
    catch(PT_DBException &exception)
    {
        database.rollbackTransaction();

        std::cout << "catch exception" << std::endl;
        std::cout << exception.get_sqlstate() << std::endl;
        std::cout << exception.get_error() << std::endl;
        database.Close();
    };
    */
    

    return 0;
}
