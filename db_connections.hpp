#ifndef _DB_CONNECTIONS_INCLUDED_H_
#define _DB_CONNECTIONS_INCLUDED_H_

#include "database.hpp"


class db_connref;
class db_connections;

class db_refexcept
{
public:
    db_refexcept();
    ~db_refexcept();
};

class db_instance : public PT_Database
{
	friend class db_connections;
public:
	db_instance(std::string host, uint16_t port, std::string username, std::string password, std::string databaseName);
	db_instance(std::string address, std::string username, std::string password, std::string databaseName);
	virtual ~db_instance();

	int64_t get_id();
	time_t get_created_time();
	time_t get_updated_time();
private:
	int64_t _id;
	time_t _created_at;
	time_t _updated_at;

	std::list<db_instance*>::iterator head;
};

/*
 * 数据库连接信息配置类
 * */
struct db_connections_config
{
	//是否使用unix fd
	bool use_fd;

	//数据库的unix fd
	std::string str_pipe;

	//数据库ip/域名
	std::string str_host;

	//端口
	uint16_t port;

	//用户名
	std::string str_username;

	//密码
	std::string str_password;

	//数据库名
	std::string str_database;

	//字符集
	std::string str_charset;

	//最小连接数
	int min_connection;

	//最大连接数
	uint32_t max_connection;
};

/*
 * 数据库连接池核心类
 * */
class db_connections
{
public:
	db_connections();
	virtual ~db_connections();
	
	//初始化数据库连接池(使用网络套接字)
	void setConnection(std::string str_host,
			uint16_t port,
			std::string str_username,
			std::string str_password,
			std::string str_database,
			std::string str_charset = "utf8",
			int min_connection = 5,
			int max_connection = 20
	);
	
	//初始化数据库连接池(使用unix fd)
	void setConnection(std::string str_pipe,
			std::string str_username,
			std::string str_password,
			std::string str_database,
			std::string str_charset = "utf8",
			int min_connection = 6,
			int max_connection = 20
	);
	
	/*
	 * 从一个ini配置文件中载入数据库连接池配置信息
	 * [section]
	 * use_pipe = true / false
	 * pipe=/var/mysql.sock
	 * host=localhost
	 * port=3306
	 * charset=utf8
	 * database=mysql
	 * username=root
	 * password=
	 * min_connection=5
	 * max_connection=20
	 * */
	bool loadConfig(std::string str_file,
			std::string str_section
	);
    
    bool loadConfig(Json::Value config);

	/*
	 * 数据库连接池开始建立连接
	 * */
	bool start();

	/*
	 * 数据库连接池销毁
	 * */
	bool stop();
	
	/*
	 * 删除所有数据库连接
	 * */
	void remove_all();

	/*
		添加数据库连接池的最大连接
	*/
	void addMaxConnection(int count);
public:

	/*
	 * 释放一个数据库连接
	 * */
	void release(db_instance *db);

	/*
	 * 引用一个数据库连接
	 * */
	db_instance *refernece();
	/*
		创建一个数据库连接
	*/
	db_instance *createConnection();

	/*
		检查config信息并创建数据库连接
	*/
	db_instance *referneceCreateConnection();

private:
	//配置
	db_connections_config _config;
	
	//活跃的数据库连接
	std::list<db_instance *> _active_connections;

	//非活跃的数据库连接
	std::list<db_instance *> _idle_connections;

	//自增序列
	int64_t _serial;

	//同步锁
	uv_mutex_t _mutex;
};


/*
 * 一个智能的数据库连接对象引用
 * */
class db_connref
{
public:
	db_connref(db_connections &connections) throw(db_refexcept);
    db_connref(db_connections *connections) throw(db_refexcept);
	~db_connref();
	
	//指针访问
	PT_Database *get_ptr();
	PT_Database *operator ->();
	PT_Database *operator *();

	db_instance *get_real();
protected:
	db_connections &_connections;
	db_instance *_database;
};

#endif
