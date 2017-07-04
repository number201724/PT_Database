#include "common.hpp"
#include "database.hpp"
#include "db_connections.hpp"


db_refexcept::db_refexcept(){}
db_refexcept::~db_refexcept(){}

db_instance::db_instance(std::string host, uint16_t port, std::string username,
    std::string password, std::string databaseName) :
PT_Database(host, port, username, password, databaseName)
, _id(0), _created_at(0), _updated_at(0) { }

db_instance::db_instance(std::string address, std::string username, std::string password, std::string databaseName) :
PT_Database(address, username, password, databaseName)
, _id(0), _created_at(0), _updated_at(0) { }

db_instance::~db_instance() { }

int64_t db_instance::get_id()
{
    return _id;
}

time_t db_instance::get_created_time()
{
    return _created_at;
}

time_t db_instance::get_updated_time()
{
    return _updated_at;
}

db_connections::db_connections() : _serial(0)
{
    uv_mutex_init(&_mutex);
}

db_connections::~db_connections()
{
    uv_mutex_destroy(&_mutex);
}

void db_connections::setConnection(std::string str_host,
    uint16_t port,
    std::string str_username,
    std::string str_password,
    std::string str_database,
    std::string str_charset,
    int min_connection,
    int max_connection)
{
    _config.use_fd = false;
    _config.str_host = str_host;
    _config.port = port;
    _config.str_username = str_username;
    _config.str_password = str_password;
    _config.str_database = str_database;
    _config.str_charset = str_charset;
    _config.min_connection = min_connection;
    _config.max_connection = max_connection;
}

void db_connections::setConnection(std::string str_pipe,
    std::string str_username,
    std::string str_password,
    std::string str_database,
    std::string str_charset,
    int min_connection,
    int max_connection
    )
{
    _config.use_fd = true;
    _config.str_pipe = str_pipe;
    _config.str_username = str_username;
    _config.str_password = str_password;
    _config.str_database = str_database;
    _config.str_charset = str_charset;
    _config.min_connection = min_connection;
    _config.max_connection = max_connection;
}

bool db_connections::loadConfig(std::string str_file,
    std::string str_section
    )
{
    ini_t *ini_fd;

    ini_fd = ini_load(str_file.c_str());
    if (ini_fd == NULL)
    {
        return false;
    }

    _config.use_fd = strcmp(ini_getex(ini_fd, str_section.c_str(), "use_pipe", "true"), "true") == 0;
    _config.str_pipe = ini_getex(ini_fd, str_section.c_str(), "pipe", "/var/mysql.sock");
    _config.str_host = ini_getex(ini_fd, str_section.c_str(), "host", "localhost");
    _config.port = atoi(ini_getex(ini_fd, str_section.c_str(), "port", "3306"));
    _config.str_charset = ini_getex(ini_fd, str_section.c_str(), "charset", "utf8");
    _config.str_database = ini_getex(ini_fd, str_section.c_str(), "database", "mysql");
    _config.str_username = ini_getex(ini_fd, str_section.c_str(), "username", "root");
    _config.str_password = ini_getex(ini_fd, str_section.c_str(), "password", "");
    _config.min_connection = atoi(ini_getex(ini_fd, str_section.c_str(), "min_connection", "5"));
    _config.max_connection = atoi(ini_getex(ini_fd, str_section.c_str(), "max_connection", "20"));

    ini_free(ini_fd);

    return true;
}

bool db_connections::loadConfig(Json::Value config)
{
    try
    {
        _config.use_fd = config["use_pipe"].asBool();
        _config.str_pipe = config["pipe"].asString();
        _config.str_host = config["host"].asString();
        _config.port = config["port"].asInt();
        _config.str_charset = config["charset"].asString();
        _config.str_database = config["database"].asString();
        _config.str_username = config["username"].asString();
        _config.str_password = config["password"].asString();
        _config.min_connection = config["min_connection"].asInt();
        _config.max_connection = config["max_connection"].asInt();
    }
    catch(Json::LogicError &err)
    {
        return false;
    }
    return true;
}

db_instance* db_connections::createConnection()
{
    db_instance *db;

    if (_config.use_fd)
    {
        db = new db_instance(_config.str_pipe,
            _config.str_username,
            _config.str_password,
            _config.str_database);
    } else
    {
        db = new db_instance(_config.str_host,
            _config.port,
            _config.str_username,
            _config.str_password,
            _config.str_database);
    }

    db->_created_at = time(NULL);
    db->_id = ++_serial;

    return db;
}

void db_connections::remove_all()
{
    db_instance *db;

    while (!_active_connections.empty())
    {
        db = _active_connections.front();
        delete db;
        _active_connections.pop_front();
    }


    while (!_idle_connections.empty())
    {
        db = _idle_connections.front();
        delete db;
        _idle_connections.pop_front();
    }
}

bool db_connections::start()
{
    bool completed = true;

    for (int i = 0; i < _config.min_connection; i++)
    {
        db_instance *db = createConnection();
        _idle_connections.push_back(db);
    }

    for (auto i = _idle_connections.begin(); i != _idle_connections.end(); i++)
    {
        try
        {
            if (!(*i)->Open())
            {
                completed = false;
                break;
            }

            (*i)->Execute("SET NAMES '" + _config.str_charset + "';");
        } catch (...)
        {
            completed = false;
            break;
        }
    }

    if (!completed)
    {
        remove_all();
        return false;
    }

    return true;
}

bool db_connections::stop()
{
    remove_all();
    return true;
}

void db_connections::addMaxConnection(int count)
{
    _config.max_connection += count;
}

void db_connections::release(db_instance *db)
{
    uv_mutex_lock(&_mutex);

    _active_connections.erase(db->head);
    db->head = _idle_connections.insert(_idle_connections.end(), db);

    uv_mutex_unlock(&_mutex);
}

db_instance *db_connections::referneceCreateConnection()
{
    if (_active_connections.size() + _idle_connections.size() < _config.max_connection)
    {
        db_instance *db = createConnection();

        if (!db->Open())
        {
            delete db;
            return nullptr;
        }

        try
        {
            db->Execute("SET NAMES '" + _config.str_charset + "';");
        } catch (...)
        {
            delete db;
            return nullptr;
        }

        db->head = _active_connections.insert(_active_connections.end(), db);

        return db;
    }

    return nullptr;
}

db_instance* db_connections::refernece()
{
    db_instance *db = nullptr;

    uv_mutex_lock(&_mutex);

    if (_idle_connections.empty())
    {
        db = referneceCreateConnection();
    } else
    {
        db = _idle_connections.front();
        _idle_connections.pop_front();
        db->head = _active_connections.insert(_active_connections.end(), db);
    }

    uv_mutex_unlock(&_mutex);

    if (db)
    {
        db->_updated_at = time(NULL);
    }
    
    return db;
}

db_connref::db_connref(db_connections &connections) throw (db_refexcept) :
_connections(connections), _database(NULL)
{
    _database = _connections.refernece();

    if (_database == NULL)
    {
        throw db_refexcept();
    }
}

db_connref::db_connref(db_connections *connections) throw(db_refexcept) :
    db_connref::db_connref(*connections)
{
    
}

db_connref::~db_connref()
{
    if (_database != NULL)
    {
        _connections.release(reinterpret_cast<db_instance *> (_database));
    }
}

PT_Database* db_connref::get_ptr()
{
    return reinterpret_cast<PT_Database *> (_database);
}

db_instance *db_connref::get_real()
{
    return _database;
}

PT_Database* db_connref::operator*()
{
    return reinterpret_cast<PT_Database *> (_database);
}

PT_Database* db_connref::operator->()
{
    return reinterpret_cast<PT_Database *> (_database);
}
