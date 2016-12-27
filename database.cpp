#include "database.hpp"

PT_DBValue::PT_DBValue()
{
	valueType = DB_VALUE_TYPE_NULL;
}

PT_DBValue::PT_DBValue(std::string value)
{
	valueType = DB_VALUE_TYPE_STRING;
	stringValue = value;
}

PT_DBValue::PT_DBValue(int32_t value)
{
	valueType = DB_VALUE_TYPE_INT;
	intValue = value;
}

PT_DBValue::PT_DBValue(uint32_t value)
{
	valueType = DB_VALUE_TYPE_UINT;
	uintValue = value;
}

PT_DBValue::PT_DBValue(int64_t value)
{
	valueType = DB_VALUE_TYPE_INT64;
	int64Value = value;
}


PT_DBValue::PT_DBValue(uint64_t value)
{
	valueType = DB_VALUE_TYPE_UINT64;
	uint64Value = value;
}

PT_DBValue::PT_DBValue(float value)
{
	valueType = DB_VALUE_TYPE_FLOAT;
	floatValue = value;
}

PT_DBValue::PT_DBValue(double value)
{
	valueType = DB_VALUE_TYPE_DOUBLE;
	doubleValue = value;
}

PT_DBValue::PT_DBValue(unsigned char *buffer, size_t length)
{
	stringValue = std::string((const char *)buffer, length);
	valueType = DB_VALUE_TYPE_BLOB;
}

PT_DBValue::~PT_DBValue()
{
	
}

bool PT_DBValue::IsNull()
{
	return valueType = DB_VALUE_TYPE_NULL;
}

bool PT_DBValue::IsUnsigned()
{
	if(valueType == DB_VALUE_TYPE_UINT || valueType == DB_VALUE_TYPE_UINT64)
	{
		return true;
	}

	return false;
}

int32_t PT_DBValue::getInt()
{
	int32_t value = 0;
	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (int32_t)intValue;
			break;
		case DB_VALUE_TYPE_UINT:
			value = (int32_t)uintValue;
			break;
		case DB_VALUE_TYPE_INT64:
			value = (int32_t)int64Value;
			break;
		case DB_VALUE_TYPE_UINT64:
			value = (int32_t)uint64Value;
			break;
		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
			sscanf(stringValue.c_str(), "%d", &value);
			break;
		case DB_VALUE_TYPE_FLOAT:
			value = (int32_t)floatValue;
			break;
		case DB_VALUE_TYPE_DOUBLE:
			value = (int32_t)doubleValue;
			break;
		default:
			break;
	}

	return value;
}

uint32_t PT_DBValue::getUInt()
{
	uint32_t value = 0;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (uint32_t)intValue;
			break;
		case DB_VALUE_TYPE_UINT:
			value = (uint32_t)uintValue;
			break;
		case DB_VALUE_TYPE_INT64:
			value = (uint32_t)int64Value;
			break;
		case DB_VALUE_TYPE_UINT64:
			value = (uint32_t)uint64Value;
			break;
		case DB_VALUE_TYPE_STRING:
			sscanf(stringValue.c_str(), "%u", &value);
			break;
		case DB_VALUE_TYPE_FLOAT:
			value = (uint32_t)floatValue;
			break;
		case DB_VALUE_TYPE_DOUBLE:
			value = (uint32_t)doubleValue;
			break;
		default:
			break;
	}

	return value;
}


int64_t PT_DBValue::getInt64()
{
	int64_t value = 0;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (int64_t)intValue;
			break;
		case DB_VALUE_TYPE_UINT:
			value = (int64_t)uintValue;
			break;
		case DB_VALUE_TYPE_INT64:
			value = (int64_t)int64Value;
			break;
		case DB_VALUE_TYPE_UINT64:
			value = (int64_t)uint64Value;
			break;
		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
#ifdef __APPLE_CC__
			sscanf(stringValue.c_str(), "%lld", &value);
#else
			sscanf(stringValue.c_str(), "%ld", &value);
#endif	
			break;
		case DB_VALUE_TYPE_FLOAT:
			value = (int64_t)floatValue;
			break;
		case DB_VALUE_TYPE_DOUBLE:
			value = (int64_t)doubleValue;
			break;
		default:
			break;
	}

	return value;
}

uint64_t PT_DBValue::getUInt64()
{
	uint64_t value = 0;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (uint64_t)intValue;
			break;
		case DB_VALUE_TYPE_UINT:
			value = (uint64_t)uintValue;
			break;
		case DB_VALUE_TYPE_INT64:
			value = (uint64_t)int64Value;
			break;
		case DB_VALUE_TYPE_UINT64:
			value = (uint64_t)uint64Value;
			break;
		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
#ifdef __APPLE_CC__
			sscanf(stringValue.c_str(), "%llu", &value);
#else
			sscanf(stringValue.c_str(), "%lu", &value);
#endif
			break;
		case DB_VALUE_TYPE_FLOAT:
			value = (uint64_t)floatValue;
			break;
		case DB_VALUE_TYPE_DOUBLE:
			value = (uint64_t)doubleValue;
			break;
		default:
			break;
	}

	return value;
}

float PT_DBValue::getFloat()
{
	float value = 0.0f;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (float)intValue;
			break;
		case DB_VALUE_TYPE_UINT:
			value = (float)uintValue;
			break;
		case DB_VALUE_TYPE_INT64:
			value = (float)int64Value;
			break;
		case DB_VALUE_TYPE_UINT64:
			value = (float)uint64Value;
			break;
		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
			sscanf(stringValue.c_str(), "%f", &value);
			break;
		case DB_VALUE_TYPE_FLOAT:
			value = (float)floatValue;
			break;
		case DB_VALUE_TYPE_DOUBLE:
			value = (float)doubleValue;
			break;
		default:
			break;
	}

	return value;
}

double PT_DBValue::getDouble()
{
	double value = 0.0;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
			value = (double)intValue;
			break;

		case DB_VALUE_TYPE_UINT:
			value = (double)uintValue;
			break;

		case DB_VALUE_TYPE_INT64:
			value = (double)int64Value;
			break;

		case DB_VALUE_TYPE_UINT64:
			value = (double)uint64Value;
			break;

		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
			sscanf(stringValue.c_str(), "%lf", &value);
			break;

		case DB_VALUE_TYPE_FLOAT:
			value = floatValue;
			break;

		case DB_VALUE_TYPE_DOUBLE:
			value = doubleValue;
			break;

		default:
			break;
	}

	return value;
}

std::string PT_DBValue::getString()
{
	char value[64];

	switch(valueType)
	{
		case DB_VALUE_TYPE_NULL:
			return "";

		case DB_VALUE_TYPE_INT:
			sprintf(value, "%d", intValue);
			break;

		case DB_VALUE_TYPE_UINT:
			sprintf(value, "%u", uintValue);
			break;

		case DB_VALUE_TYPE_INT64:
#ifdef __APPLE_CC__
			sprintf(value, "%lld", int64Value);
#else
			sprintf(value, "%ld", int64Value);
#endif
			break;

		case DB_VALUE_TYPE_UINT64:
#ifdef __APPLE_CC__
			sprintf(value, "%llu", int64Value);
#else
			sprintf(value, "%lu", int64Value);
#endif
			break;

		case DB_VALUE_TYPE_BLOB:
		case DB_VALUE_TYPE_STRING:
			return stringValue;

		case DB_VALUE_TYPE_FLOAT:
			sprintf(value, "%f", floatValue);
			break;

		case DB_VALUE_TYPE_DOUBLE:
			sprintf(value, "%lf", doubleValue);
			break;

		default:
			break;
	}

	return value;
}

size_t PT_DBValue::valueSize()
{
	size_t size = 0;

	switch(valueType)
	{
		case DB_VALUE_TYPE_INT:
		case DB_VALUE_TYPE_UINT:
			size = sizeof(uint32_t);
			break;
		case DB_VALUE_TYPE_INT64:
		case DB_VALUE_TYPE_UINT64:
			size = sizeof(uint64_t);
			break;
		case DB_VALUE_TYPE_STRING:
		case DB_VALUE_TYPE_BLOB:
			size = stringValue.length();
			break;
		case DB_VALUE_TYPE_FLOAT:
			size = sizeof(float);
			break;
		case DB_VALUE_TYPE_DOUBLE:
			size = sizeof(double);
			break;
		default:
			size = 0;
			break;
	}

	return size;
}
void PT_DBValue::value2bind(MYSQL_BIND *bind)
{
	bzero(bind, sizeof(MYSQL_BIND));

	if(!m_sharedBuffer)
	{
		if(valueType == DB_VALUE_TYPE_NULL)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[1]);
			m_sharedBuffer = sharedPtr;

			m_mysqlIsNull = 1;
			bind->is_null = &m_mysqlIsNull;
			bind->buffer_type = MYSQL_TYPE_NULL;
		}
		
		//INT32处理
		if(valueType == DB_VALUE_TYPE_INT)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[valueSize()]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &intValue, sizeof(intValue));
			bind->buffer_type = MYSQL_TYPE_LONG;
		}

		if(valueType == DB_VALUE_TYPE_UINT)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[valueSize()]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &uintValue, sizeof(uintValue));
			bind->buffer_type = MYSQL_TYPE_LONG;
			bind->is_unsigned = 1;
		}
		
		//INT64处理
		if(valueType == DB_VALUE_TYPE_INT64)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[valueSize()]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &int64Value, sizeof(int64Value));
			bind->buffer_type = MYSQL_TYPE_LONGLONG;
		}

		if(valueType == DB_VALUE_TYPE_UINT64)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[valueSize()]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &uint64Value, sizeof(uint64Value));
			bind->buffer_type = MYSQL_TYPE_LONGLONG;
			bind->is_unsigned = 1;
		}

		//字符串处理
		if(valueType == DB_VALUE_TYPE_STRING || valueType == DB_VALUE_TYPE_BLOB)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[stringValue.length()]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), stringValue.c_str(), stringValue.length());
			
			if(valueType == DB_VALUE_TYPE_STRING)
			{
				bind->buffer_type = MYSQL_TYPE_VAR_STRING;
			}
			else
			{
				bind->buffer_type = MYSQL_TYPE_BLOB;
			}
		}


		if(valueType == DB_VALUE_TYPE_FLOAT)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[sizeof(float)]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &floatValue, sizeof(floatValue));
			bind->buffer_type = MYSQL_TYPE_FLOAT;
		}

		if(valueType == DB_VALUE_TYPE_DOUBLE)
		{
			std::shared_ptr<unsigned char> sharedPtr(new unsigned char[sizeof(double)]);
			m_sharedBuffer = sharedPtr;

			memcpy(sharedPtr.get(), &doubleValue, sizeof(doubleValue));
			bind->buffer_type = MYSQL_TYPE_DOUBLE;
		}

		bind->buffer = m_sharedBuffer.get();
		bind->buffer_length = valueSize();
	}
}

enum PT_DBValueType PT_DBValue::getValueType()
{
	return valueType;
}

PT_DBException::PT_DBException(int error_code, std::string error_str, std::string sqlstate) : 
	m_errorstr(error_str), 
	m_error_code(error_code),
	m_sqlstate(sqlstate)
{

}

PT_DBException::~PT_DBException()
{

}

const std::string& PT_DBException::get_error()
{
	return m_errorstr;
}

int PT_DBException::get_errno()
{
	return m_error_code;
}

const std::string &PT_DBException::get_sqlstate()
{
	return m_sqlstate;
}
PT_Database::PT_Database(std::string host, uint16_t port, std::string username, std::string password, std::string databaseName)
{
	m_IsInit = false;
	m_IsOpen = false;

	m_Address.IsNetworkConn = true;
	m_Address.address = host;
	m_Address.port = port;
	m_Address.username = username;
	m_Address.password = password;
	m_Address.databaseName = databaseName;

	Init();
}

PT_Database::PT_Database(std::string address, std::string username, std::string password, std::string databaseName)
{
	m_IsInit = false;
	m_IsOpen = false;

	m_Address.IsNetworkConn = false;
	m_Address.address = address;
	m_Address.port = 0;
	m_Address.username = username;
	m_Address.password = password;
	m_Address.databaseName = databaseName;

	Init();
	
}

PT_Database::~PT_Database()
{

}

void PT_Database::Init()
{
	if(!m_IsInit)
	{
		my_bool reconnect = 1;
		bzero(&m_Conn, sizeof(m_Conn));
		mysql_init(&m_Conn);
		mysql_options(&m_Conn, MYSQL_OPT_RECONNECT, &reconnect);

		m_IsInit = true;
	}
}

bool PT_Database::IsOpen()
{
	return m_IsOpen;
}

bool PT_Database::Open()
{
	MYSQL *pConn = NULL;

	Init();

	if(m_Address.IsNetworkConn)
	{
		pConn = mysql_real_connect(&m_Conn,m_Address.address.c_str(),
				m_Address.username.c_str(),m_Address.password.c_str(),
				m_Address.databaseName.c_str(),m_Address.port,NULL,CLIENT_MULTI_STATEMENTS);
	}
	else
	{
		pConn = mysql_real_connect(&m_Conn, NULL,m_Address.username.c_str(),
				m_Address.password.c_str(),m_Address.databaseName.c_str(),
				0,m_Address.address.c_str(),CLIENT_MULTI_STATEMENTS);

		
	}

	if(pConn == NULL) {
		return false;
	}

	
	m_IsOpen = true;

	return true;
}

void PT_Database::Close()
{
	if(m_IsInit)
	{
		mysql_close(&m_Conn);
		m_IsInit = false;
	}
}

MYSQL* PT_Database::getConn()
{
	return &m_Conn;
}

void PT_Database::beginTransaction()
{
	Execute("START TRANSACTION");
}

void PT_Database::commitTransaction()
{
	ParseError(mysql_commit(&m_Conn));
}

void PT_Database::rollbackTransaction()
{
	ParseError(mysql_rollback(&m_Conn));
}

void PT_Database::transaction(fnTransactionCallback callback)
{
	try
	{
		beginTransaction();

		callback(this);

		commitTransaction();
	}
	catch(PT_DBException &exception)
	{
		rollbackTransaction();
		throw exception;
	}
}

uint32_t PT_Database::Execute(std::string sql)
{
	return Execute(sql.c_str());
}

uint32_t PT_Database::Execute(const char *sql)
{
	MYSQL_RES *result;
	int status;

	status = mysql_query(&m_Conn, sql);
	ParseError(status);

	do
	{
		result = mysql_store_result(&m_Conn);

		if(result)
		{
			mysql_free_result(result);
		}

		status = mysql_next_result(&m_Conn);

		if(status > 0) {
			ParseError(status);
		}
	}while(status == 0);

	return mysql_affected_rows(&m_Conn);
}

void PT_Database::ParseError(int result)
{
	if(result == 0)
	{
		return;
	}

	throw PT_DBException(mysql_errno(&m_Conn), mysql_error(&m_Conn), mysql_sqlstate(&m_Conn));
}

PT_DBRecordset::PT_DBRecordset(PT_Database &database) : m_databaseRef(database), result_index(0)
{

}



PT_DBResult::PT_DBResult() : row_index(0) , affected_rows(0)
{

}

PT_DBResult::~PT_DBResult()
{

}

bool PT_DBResult::GetFieldValue(const char *name, int32_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, int32_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, int32_t &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getInt();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, uint32_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getUInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, uint32_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getUInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, uint32_t &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getUInt();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, int64_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getInt64();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, int64_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getInt64();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, int64_t &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getInt64();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, uint64_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getUInt64();
	
	return true;
}
bool PT_DBResult::GetFieldValue(std::string name, uint64_t &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getUInt64();
	
	return true;
}
bool PT_DBResult::GetFieldValue(uint32_t index, uint64_t &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getUInt64();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, std::string &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getString();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, std::string &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getString();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, std::string &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getString();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, float &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getFloat();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, float &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getFloat();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, float &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getFloat();
		return true;
	}

	return false;
}

bool PT_DBResult::GetFieldValue(const char *name, double &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getDouble();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, double &outValue)
{
	uint32_t index = FindFieldIndex(name);

	if(index == -1)
	{
		return false;
	}

	outValue = rows[row_index][index].getDouble();
	
	return true;
}

bool PT_DBResult::GetFieldValue(uint32_t index, double &outValue)
{
	if(index < fields.size()) {
		outValue = rows[row_index][index].getDouble();
		return true;
	}

	return false;
}

uint32_t PT_DBResult::FindFieldIndex(std::string &name)
{
	for(size_t i = 0; i < fields.size(); i++)
	{
		if(!fields[i].name.compare(name))
		{
			return i;
		}
	}

	return -1;
}

uint32_t PT_DBResult::FindFieldIndex(const char *name)
{
	for(size_t i = 0; i < fields.size(); i++)
	{
		if(!fields[i].name.compare(name))
		{
			return i;
		}
	}

	return -1;
}

uint32_t PT_DBResult::GetRecordCount()
{
	return rows.size();
}

bool PT_DBResult::MoveFirst()
{
	if(rows.empty()){
		return false;
	}

	row_index = 0;

	return true;
}

bool PT_DBResult::MoveNext()
{
	if(row_index + 1 < rows.size()){
		row_index++;
		return true;
	}

	return false;
}

bool PT_DBResult::MovePrevious()
{
	if(rows.empty())
	{
		return false;
	}

	if(row_index - 1 < rows.size()){
		row_index = row_index - 1;
		return true;
	}

	return false;
}

bool PT_DBResult::MoveLast()
{
	if(rows.empty())
	{
		return false;
	}

	row_index = rows.size() - 1;

	return true;
}

const PT_DBRows& PT_DBResult::GetRows()
{
	return rows;
}

const PT_DBFields& PT_DBResult::GetFields()
{
	return fields;
}


PT_DBRecordset::PT_DBRecordset(PT_Database *database) : m_databaseRef(*database)
{

}

PT_DBRecordset::~PT_DBRecordset()
{

}

void PT_DBRecordset::ParseError(MYSQL_STMT *stmt, int err)
{
	if( err == 0 ) return;

	throw PT_DBException(mysql_stmt_errno(stmt), mysql_stmt_error(stmt), mysql_stmt_sqlstate(stmt));
}
bool PT_DBRecordset::Open(std::string sql)
{
	PT_DBQueryParameter parameters;

	return Open(sql, parameters);
}
bool PT_DBRecordset::Open(std::string &sql)
{
	PT_DBQueryParameter parameters;

	return Open(sql, parameters);
}
bool PT_DBRecordset::Open(std::string &sql, PT_DBQueryParameter &parameters)
{
	return Open(sql.c_str(), parameters);
}
bool PT_DBRecordset::Open(const char *sql)
{
	PT_DBQueryParameter parameters;
	return Open(sql, parameters);
}
bool PT_DBRecordset::Open(const char *sql, PT_DBQueryParameter &parameters)
{
	MYSQL_STMT *stmt;
	int err;
	unsigned long param_count;
	MYSQL_BIND *input = NULL, *output = NULL;
	uint64_t num_fields;
	MYSQL_RES *res;
	unsigned long *output_length = NULL;

	stmt = mysql_stmt_init(m_databaseRef.getConn());

	try
	{
		err = mysql_stmt_prepare(stmt, sql, strlen(sql));
		ParseError(stmt, err);

		param_count = mysql_stmt_param_count(stmt);
		
		//if have parameter
		if(param_count > 0)
		{
			if(param_count != parameters.size())
			{
				mysql_stmt_close(stmt);
				return false;
			}

			input = new MYSQL_BIND[param_count];

			for(size_t i = 0; i < parameters.size();i++)
			{
				parameters[i].value2bind(&input[i]);
			}

			err = mysql_stmt_bind_param(stmt, input);
			ParseError(stmt, err);
		}

		
		err = mysql_stmt_execute(stmt);
		ParseError(stmt, err);

		//process return value
		do
		{
			PT_DBResult db_result;

			err = mysql_stmt_store_result(stmt);
			ParseError(stmt, err);

			num_fields = mysql_stmt_field_count(stmt);
			
			db_result.affected_rows = mysql_stmt_affected_rows(stmt);

			//process rows and fields
			if(num_fields > 0)
			{
				res = mysql_stmt_result_metadata(stmt);
			
				for(uint64_t i = 0; i < num_fields; i++)
				{
					PT_DBField field;

					field.name = res->fields[i].name;
					field.type = res->fields[i].type;
					field.is_unsigned = res->fields[i].flags & UNSIGNED_FLAG;
					field.is_not_null = res->fields[i].flags & NOT_NULL_FLAG;
					field.is_primary_key = res->fields[i].flags & PRI_KEY_FLAG;
					field.is_unique_key = res->fields[i].flags & UNIQUE_KEY_FLAG;

					//std::cout << "field " << i << " name:" << field.name << " type:" << field.type << "unsigned:" << field.is_unsigned << std::endl;

					db_result.fields.push_back(field);
				}

				mysql_free_result(res);

				output = new MYSQL_BIND[num_fields];
				output_length = new unsigned long[num_fields];

				bzero(output, sizeof(MYSQL_BIND) * num_fields);

				for(uint64_t i = 0; i < num_fields; i++)
				{
					output[i].length = &output_length[i];
				}

				err = mysql_stmt_bind_result(stmt, output);
				ParseError(stmt, err);
				
				//store all rows
				while(true)
				{
					PT_DBRow row;

					//clear state
					bzero(output_length, sizeof(unsigned long) * num_fields);
					for(uint64_t i = 0; i < num_fields; i++)
					{
						output[i].length = &output_length[i];
					}

					//fetch one row
					err = mysql_stmt_fetch(stmt);

					if(err == MYSQL_NO_DATA) break;
					if(err != MYSQL_DATA_TRUNCATED) ParseError(stmt, err);
					
					//fetch column
					for(uint64_t i = 0; i < num_fields; i++)
					{
						my_bool isNull = 0;

						output[i].buffer = new unsigned char [output_length[i]];
						output[i].buffer_length = output_length[i];
						output[i].is_null = &isNull;
						
						err = mysql_stmt_fetch_column(stmt, &output[i], i, 0);

						if(err)
						{
							delete[] (unsigned char *)output[i].buffer;
							output[i].buffer = NULL;
							ParseError(stmt, err);
						}
						
						PT_DBValue db_value((unsigned char *)output[i].buffer, output[i].buffer_length);
						row.push_back(db_value);

						delete[] (unsigned char *)output[i].buffer;
						output[i].buffer = NULL;
					}

					db_result.rows.push_back(row);
				}
			}

			//copy data struct
			results.push_back(db_result);
			
			err = mysql_stmt_next_result(stmt);
			if(err == -1) break;
			ParseError(stmt, err);

		} while(true);		
	}
	catch(PT_DBException &exception)
	{
		if(stmt)
		{
			mysql_stmt_close(stmt);
			stmt = NULL;
		}

		if(input)
		{
			delete[] input;
			input = NULL;
		}

		if(output)
		{
			delete[] output;
			output = NULL;
		}

		if(output_length)
		{
			delete[] output_length;
			output_length = NULL;
		}

		throw exception;
	}


	if(stmt)
	{
		mysql_stmt_close(stmt);
		stmt = NULL;
	}

	if(input)
	{
		delete[] input;
		input = NULL;
	}

	if(output)
	{
		delete[] output;
		output = NULL;
	}

	if(output_length)
	{
		delete[] output_length;
		output_length = NULL;
	}
	
	return true;
}

const PT_DBResults &PT_DBRecordset::GetResults()
{
	return results;
}
	//结果集移动
bool PT_DBRecordset::FirstResult()
{
	result_index = 0;

	if(results.empty()){
		return false;
	}

	return true;
}

bool PT_DBRecordset::NextResult()
{
	uint32_t index = result_index + 1;
	if(index < results.size())
	{
		result_index = index;
		return true;
	}
	return false;
}

bool PT_DBRecordset::PreviousResult()
{
	uint32_t index = result_index - 1;
	if(index < results.size())
	{
		result_index = index;
		return true;
	}
	return false;
}

bool PT_DBRecordset::LastResult()
{
	if(results.empty()){
		return false;
	}

	result_index = results.size() - 1;

	return true;
}

	//ROW移动
bool PT_DBRecordset::MoveFirst()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].MoveFirst();
}

bool PT_DBRecordset::MoveNext()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].MoveNext();
}

bool PT_DBRecordset::MovePrevious()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].MovePrevious();
}

bool PT_DBRecordset::MoveLast()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].MoveLast();
}

const PT_DBRows& PT_DBRecordset::GetRows()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].GetRows();
}

const PT_DBFields& PT_DBRecordset::GetFields()
{
	if(result_index >= results.size()){
		throw std::out_of_range("PT_DBRecordset::result_index");
	}

	return results[result_index].GetFields();
}

bool PT_DBRecordset::GetFieldValue(const char *name, int32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, int32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, int32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, uint32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, uint32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, uint32_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, int64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}

	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, int64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, int64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, uint64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, uint64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, uint64_t &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, std::string &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, std::string &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, std::string &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, float &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, float &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, float &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, double &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, double &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(uint32_t index, double &outValue)
{
	if(result_index >= results.size()){
		return false;
	}
	
	return results[result_index].GetFieldValue(index, outValue);
}

uint32_t PT_DBRecordset::FindFieldIndex(std::string &name)
{
	if(result_index >= results.size())
	{
		return -1;
	}

	return results[result_index].FindFieldIndex(name);
}

uint32_t PT_DBRecordset::FindFieldIndex(const char *name)
{
	if(result_index >= results.size())
	{
		return -1;
	}
	
	return results[result_index].FindFieldIndex(name);
}

uint32_t PT_DBRecordset::GetRecordCount()
{
	if(result_index >= results.size())
	{
		return 0;
	}
	
	return results[result_index].GetRecordCount();
}

uint32_t PT_DBRecordset::GetResultCount()
{
	return results.size();
}