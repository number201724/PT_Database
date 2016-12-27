#include <iostream>
#include "database.hpp"


int main(int argc, char *argv[])
{
    PT_Database database("192.168.200.3", 3306, "root", "", "test");

    if(!database.Open())
    {
        std::cout << "Connect mysqldb failed" << std::endl;
        return 0;
    }

    std::cout << "mysql_is_open:" << database.IsOpen() << std::endl;
    std::cout << "connected mysqldb" << std::endl;

    try
    {
        PT_DBRecordset pRs(database);
        PT_DBQueryParameter parameters;
        parameters.push_back(PT_DBValue(1));
        if(pRs.Open("SELECT * FROM test") /*&& pRs.GetRecordCount() > 0*/)
        {
            
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

        //pRs.Open("SELECT * FROM test;");
    }
    catch(PT_DBException &exception)
    {
        std::cout << "catch exception" << std::endl;
        std::cout << exception.get_sqlstate() << std::endl;
        std::cout << exception.get_error() << std::endl;
        database.Close();
    }


    /*
    try
    {
        database.beginTransaction();
        database.Execute("INSERT INTO test VALUES('dasdas')");
        database.commitTransaction();
    }
    catch(PT_DBException &exception)
    {
        std::cout << "catch exception" << std::endl;
        std::cout << exception.get_sqlstate() << std::endl;
        std::cout << exception.get_error() << std::endl;
        database.Close();
    };
    */

    return 0;
}
