test : database.o main.o
	g++ -L/usr/local/opt/openssl/lib -o test -lcrypto -lmysqlclient database.o main.o

database.o : database.cpp
	g++ -c -I/usr/local/opt/openssl/include -I/usr/local/include/mysql database.cpp

main.o : main.cpp
	g++ -c -I/usr/local/opt/openssl/include -I/usr/local/include/mysql main.cpp
