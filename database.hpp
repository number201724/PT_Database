/*
	Coding by 201724
*/

#ifndef _DATABASE_HPP_INCLUDED_
#define _DATABASE_HPP_INCLUDED_

enum PT_DBValueType
{
	DB_VALUE_TYPE_NULL,
	DB_VALUE_TYPE_INT,
	DB_VALUE_TYPE_UINT,
	DB_VALUE_TYPE_INT64,
	DB_VALUE_TYPE_UINT64,
	DB_VALUE_TYPE_STRING,
	DB_VALUE_TYPE_FLOAT,
	DB_VALUE_TYPE_DOUBLE,
	DB_VALUE_TYPE_BLOB
};

class PT_DBValue
{
private:
	enum PT_DBValueType valueType;
	std::string stringValue;
	int32_t intValue;
	uint32_t uintValue;
	int64_t int64Value;
	uint64_t uint64Value;
	float floatValue;
	double doubleValue;
public:
	PT_DBValue();
	PT_DBValue(std::string value);
	PT_DBValue(int32_t value);
	PT_DBValue(uint32_t value);
	PT_DBValue(int64_t value);
	PT_DBValue(uint64_t value);
	PT_DBValue(float value);
	PT_DBValue(double value);
	PT_DBValue(unsigned char *buffer, size_t length);
	~PT_DBValue();

	int32_t getInt();
	uint32_t getUInt();

	int64_t getInt64();
	uint64_t getUInt64();

	float getFloat();
	double getDouble();

	std::string getString();
	
	bool IsNull();
	bool IsUnsigned();
	
	enum PT_DBValueType getValueType();

	//mysql parameter bind functions
	size_t valueSize();
	void value2bind(MYSQL_BIND *bind);
private:
	//mysql_bind paramters
	my_bool m_mysqlIsNull;
	std::shared_ptr<unsigned char> m_sharedBuffer;
	//MYSQL_BIND m_stBind;
};

class PT_DBException
{
public:
	PT_DBException(int error_code, std::string error_str, std::string sqlstate);
	~PT_DBException();

	const std::string &get_error();
	int get_errno();
	const std::string &get_sqlstate();

private:
	std::string m_errorstr;
	int m_error_code;
	std::string m_sqlstate;
};


struct PT_DatabaseAddress
{
	bool IsNetworkConn;
	std::string address;
	uint16_t port;

	std::string username;
	std::string password;
	std::string databaseName;
};

class PT_Database;
typedef void (*fnTransactionCallback)(PT_Database *database);
class PT_Database
{
private:
	struct PT_DatabaseAddress m_Address;
	MYSQL m_Conn;
	bool m_IsOpen;
public:
	PT_Database(std::string host, uint16_t port, std::string username, std::string password, std::string databaseName);
	PT_Database(std::string address, std::string username, std::string password, std::string databaseName);

	virtual ~PT_Database();

	bool IsOpen();
	bool Open();
	void Close();

	void beginTransaction();
	void commitTransaction();
	void rollbackTransaction();

	uint32_t Execute(std::string sql);
	uint32_t Execute(const char *sql);

	void ParseError(int result);

	//任何异常都会导致rollback
	void transaction(fnTransactionCallback callback);

	MYSQL* getConn();
};

struct PT_DBField
{
	std::string name;
	enum enum_field_types type;
	bool is_unsigned;
	bool is_not_null;
	bool is_primary_key;
	bool is_unique_key;
};

typedef std::vector<PT_DBField> PT_DBFields;
//一行中的多个值
typedef std::vector<PT_DBValue> PT_DBRow;
//一列中的多个值
typedef std::vector<PT_DBRow> PT_DBRows;

class PT_DBResult
{
public:
	//如果没有fields的信息则会获取到这个
	PT_DBFields fields;
	PT_DBRows rows;
	int64_t current_row;
	int64_t affected_rows;

	PT_DBResult();
	~PT_DBResult();

	//ROW移动
	void MoveFirst();
	void MoveNext();
	void MovePrevious();
	void MoveLast();

	bool IsEOF();

	const PT_DBRows& GetRows();
	const PT_DBFields& GetFields();

	//验证当前Row是否有效(current_row)
	bool IsCurrentValidRow();
	//验证field是否有效
	bool IsCurrentValidField(int64_t field_index);

	/*
		 获取Field的数据
	*/
	bool GetFieldValue(const char *name, int32_t &outValue);
	bool GetFieldValue(std::string name, int32_t &outValue);
	bool GetFieldValue(int64_t index, int32_t &outValue);

	bool GetFieldValue(const char *name, uint32_t &outValue);
	bool GetFieldValue(std::string name, uint32_t &outValue);
	bool GetFieldValue(int64_t index, uint32_t &outValue);

	bool GetFieldValue(const char *name, int64_t &outValue);
	bool GetFieldValue(std::string name, int64_t &outValue);
	bool GetFieldValue(int64_t index, int64_t &outValue);

	bool GetFieldValue(const char *name, uint64_t &outValue);
	bool GetFieldValue(std::string name, uint64_t &outValue);
	bool GetFieldValue(int64_t index, uint64_t &outValue);

	bool GetFieldValue(const char *name, std::string &outValue);
	bool GetFieldValue(std::string name, std::string &outValue);
	bool GetFieldValue(int64_t index, std::string &outValue);

	bool GetFieldValue(const char *name, float &outValue);
	bool GetFieldValue(std::string name, float &outValue);
	bool GetFieldValue(int64_t index, float &outValue);

	bool GetFieldValue(const char *name, double &outValue);
	bool GetFieldValue(std::string name, double &outValue);
	bool GetFieldValue(int64_t index, double &outValue);

	int64_t GetFieldIndex(std::string &name);
	int64_t GetFieldIndex(const char *name);

	int64_t GetRecordCount();
};

typedef std::vector<PT_DBResult> PT_DBResults;

typedef std::vector<PT_DBValue> PT_DBQueryParameter;
class PT_DBRecordset
{
public:
	PT_DBRecordset(PT_Database &database);
	PT_DBRecordset(PT_Database *database);
	~PT_DBRecordset();

	bool Open(std::string sql);
	bool Open(std::string &sql);
	bool Open(std::string &sql, PT_DBQueryParameter &parameters);
	bool Open(const char *sql);
	bool Open(const char *sql, PT_DBQueryParameter &parameters);
	const PT_DBResults &GetResults();

	//当前是否有效的结果集
	bool IsCurrentValidResult();

	//结果集移动
	void FirstResult();
	void NextResult();
	void PreviousResult();
	void LastResult();
	bool IsResultEOF();

	//ROW移动
	void MoveFirst();
	void MoveNext();
	void MovePrevious();
	void MoveLast();
	bool IsEOF();

	const PT_DBRows* GetRows();
	const PT_DBFields* GetFields();

	bool GetFieldValue(const char *name, int32_t &outValue);
	bool GetFieldValue(std::string name, int32_t &outValue);
	bool GetFieldValue(int64_t index, int32_t &outValue);

	bool GetFieldValue(const char *name, uint32_t &outValue);
	bool GetFieldValue(std::string name, uint32_t &outValue);
	bool GetFieldValue(int64_t index, uint32_t &outValue);

	bool GetFieldValue(const char *name, int64_t &outValue);
	bool GetFieldValue(std::string name, int64_t &outValue);
	bool GetFieldValue(int64_t index, int64_t &outValue);

	bool GetFieldValue(const char *name, uint64_t &outValue);
	bool GetFieldValue(std::string name, uint64_t &outValue);
	bool GetFieldValue(int64_t index, uint64_t &outValue);

	bool GetFieldValue(const char *name, std::string &outValue);
	bool GetFieldValue(std::string name, std::string &outValue);
	bool GetFieldValue(int64_t index, std::string &outValue);

	bool GetFieldValue(const char *name, float &outValue);
	bool GetFieldValue(std::string name, float &outValue);
	bool GetFieldValue(int64_t index, float &outValue);

	bool GetFieldValue(const char *name, double &outValue);
	bool GetFieldValue(std::string name, double &outValue);
	bool GetFieldValue(int64_t index, double &outValue);

	int64_t GetFieldIndex(std::string &name);
	int64_t GetFieldIndex(const char *name);

	int64_t GetRecordCount();
	int64_t GetResultCount();
    int64_t GetAffectedRows();
    //int64_t affected_rows

	void ParseError(MYSQL_STMT *stmt, int err);
private:
	PT_Database &m_databaseRef;
	//结果集
	PT_DBResults results;
	int64_t current_result;
};
#endif
