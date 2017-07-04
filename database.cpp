#include "common.hpp"
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
	return (valueType = DB_VALUE_TYPE_NULL);
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
		case DB_VALUE_TYPE_BLOB:
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
	m_IsOpen = false;

	m_Address.IsNetworkConn = true;
	m_Address.address = host;
	m_Address.port = port;
	m_Address.username = username;
	m_Address.password = password;
	m_Address.databaseName = databaseName;
}

PT_Database::PT_Database(std::string address, std::string username, std::string password, std::string databaseName)
{
	m_IsOpen = false;

	m_Address.IsNetworkConn = false;
	m_Address.address = address;
	m_Address.port = 0;
	m_Address.username = username;
	m_Address.password = password;
	m_Address.databaseName = databaseName;
}

PT_Database::~PT_Database()
{
	Close();
}


bool PT_Database::IsOpen()
{
	return m_IsOpen;
}

bool PT_Database::Open()
{
	MYSQL *pConn = NULL;
	my_bool reconnect = 1;

	if(m_IsOpen)
        return true;

	bzero(&m_Conn, sizeof(m_Conn));
	mysql_init(&m_Conn);
	

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
		mysql_close(&m_Conn);
		return false;
	}
    
    mysql_options(&m_Conn, MYSQL_OPT_RECONNECT, &reconnect);
	
	m_IsOpen = true;

	return true;
}

void PT_Database::Close()
{
	if(m_IsOpen)
	{
		mysql_close(&m_Conn);
		m_IsOpen = false;
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

	return (uint32_t)mysql_affected_rows(&m_Conn);
}

void PT_Database::ParseError(int result)
{
	if(result == 0)
	{
		return;
	}

	throw PT_DBException(mysql_errno(&m_Conn), mysql_error(&m_Conn), mysql_sqlstate(&m_Conn));
}




PT_DBResult::PT_DBResult() : current_row(0) , affected_rows(0)
{

}

PT_DBResult::~PT_DBResult()
{

}

bool PT_DBResult::IsCurrentValidRow()
{
	if(current_row >= 0 && current_row < (int64_t)rows.size())
	{
		return true;
	}

	return false;
}

bool PT_DBResult::IsCurrentValidField(int64_t field_index)
{
	if(field_index >= 0 && field_index < (int64_t)rows[current_row].size())
	{
		return true;
	}

	return false;
}

#define VALID_FIND_FIELD_BY_NAME() 	int64_t index;	\
	if(!IsCurrentValidRow()) return false;	\
	index = GetFieldIndex(name);	\
	if(!IsCurrentValidField(index)) return false;	

#define VALID_ROW_AND_INDEX() 	if(!IsCurrentValidRow()) return false;	\
	if(!IsCurrentValidField(index)) return false;	

bool PT_DBResult::GetFieldValue(const char *name, int32_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();
	
	outValue = rows[current_row][index].getInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, int32_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(int64_t index, int32_t &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getInt();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, uint32_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();
	
	outValue = rows[current_row][index].getUInt();
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, uint32_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getUInt();
	
	return true;
}

bool PT_DBResult::GetFieldValue(int64_t index, uint32_t &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getUInt();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, int64_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getInt64();
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, int64_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getInt64();
	return true;
}

bool PT_DBResult::GetFieldValue(int64_t index, int64_t &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getInt64();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, uint64_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getUInt64();
	
	return true;
}
bool PT_DBResult::GetFieldValue(std::string name, uint64_t &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getUInt64();
	return true;
}
bool PT_DBResult::GetFieldValue(int64_t index, uint64_t &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getUInt64();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, std::string &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getString();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, std::string &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getString();
	
	return true;
}

bool PT_DBResult::GetFieldValue(int64_t index, std::string &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getString();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, float &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getFloat();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, float &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getFloat();
	
	return true;
}

bool PT_DBResult::GetFieldValue(int64_t index, float &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getFloat();
	return true;
}

bool PT_DBResult::GetFieldValue(const char *name, double &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getDouble();
	
	return true;
}

bool PT_DBResult::GetFieldValue(std::string name, double &outValue)
{
	VALID_FIND_FIELD_BY_NAME();

	outValue = rows[current_row][index].getDouble();
	
	return true;
}


bool PT_DBResult::GetFieldValue(int64_t index, double &outValue)
{
	VALID_ROW_AND_INDEX();

	outValue = rows[current_row][index].getDouble();
	return true;
}

#undef VALID_FIND_FIELD_BY_NAME
#undef VALID_ROW_AND_INDEX

int64_t PT_DBResult::GetFieldIndex(std::string &name)
{
	for(int64_t i = 0; i < (int64_t)fields.size(); i++)
	{
		if(!fields[i].name.compare(name))
		{
			return i;
		}
	}

	return -1;
}

int64_t PT_DBResult::GetFieldIndex(const char *name)
{
	for(int64_t i = 0; i < (int64_t)fields.size(); i++)
	{
		if(!fields[i].name.compare(name))
		{
			return i;
		}
	}

	return -1;
}

int64_t PT_DBResult::GetRecordCount()
{
	return rows.size();
}

bool PT_DBResult::IsEOF()
{
	if(current_row >= (int64_t)rows.size())
	{
		return true;
	}

	return false;
}

void PT_DBResult::MoveFirst()
{
	current_row = 0;
}

void PT_DBResult::MoveNext()
{
	current_row++;
}

void PT_DBResult::MovePrevious()
{
	int64_t previous_row = current_row - 1;

	if(previous_row < 0)
	{
		return;
	}

	current_row = previous_row;
}

void PT_DBResult::MoveLast()
{
	int64_t last_row = rows.size() - 1;

	if(last_row > 0)
	{
		current_row = last_row;
	}
}

const PT_DBRows& PT_DBResult::GetRows()
{
	return rows;
}

const PT_DBFields& PT_DBResult::GetFields()
{
	return fields;
}


PT_DBRecordset::PT_DBRecordset(PT_Database &database) : m_databaseRef(database), current_result(0)
{

}

PT_DBRecordset::PT_DBRecordset(PT_Database *database) : m_databaseRef(*database), current_result(0)
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

	if(!m_databaseRef.IsOpen())
        m_databaseRef.Open();
    
    results.clear();

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
						
						err = mysql_stmt_fetch_column(stmt, &output[i],(unsigned int)i, 0);

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
void PT_DBRecordset::FirstResult()
{
	current_result = 0;
}

void PT_DBRecordset::NextResult()
{
	current_result++;
}

void PT_DBRecordset::PreviousResult()
{
	int64_t previous_result = current_result - 1;

	if(previous_result < 0)
	{
		return;
	}

	current_result = previous_result;
}

void PT_DBRecordset::LastResult()
{
	int64_t last_result = results.size() - 1;

	if(last_result > 0)
	{
		current_result = last_result;
	}
}

bool PT_DBRecordset::IsResultEOF()
{
	if(current_result >= (int64_t)results.size())
	{
		return true;
	}

	return false;
}

bool PT_DBRecordset::IsCurrentValidResult()
{
	if(current_result >= 0 && current_result < (int64_t)results.size())
	{
		return true;
	}
	return false;
}

//ROW移动
void PT_DBRecordset::MoveFirst()
{
	if(!IsCurrentValidResult()){
		return;
	}

	return results[current_result].MoveFirst();
}

void PT_DBRecordset::MoveNext()
{
	if(!IsCurrentValidResult()){
		return;
	}

	return results[current_result].MoveNext();
}

void PT_DBRecordset::MovePrevious()
{
	if(!IsCurrentValidResult()){
		return;
	}

	return results[current_result].MovePrevious();
}

void PT_DBRecordset::MoveLast()
{
	if(!IsCurrentValidResult()){
		return;
	}

	return results[current_result].MoveLast();
}

bool PT_DBRecordset::IsEOF()
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].IsEOF();
}

const PT_DBRows* PT_DBRecordset::GetRows()
{
	if(!IsCurrentValidResult())
	{
		return nullptr;
	}

	return &results[current_result].GetRows();
}

const PT_DBFields* PT_DBRecordset::GetFields()
{
	if(!IsCurrentValidResult()){
		return nullptr;
	}

	return &results[current_result].GetFields();
}

bool PT_DBRecordset::GetFieldValue(const char *name, int32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, int32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, int32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, uint32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, uint32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, uint32_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, int64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}

	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, int64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, int64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, uint64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, uint64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, uint64_t &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, std::string &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, std::string &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, std::string &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, float &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, float &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, float &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(index, outValue);
}

bool PT_DBRecordset::GetFieldValue(const char *name, double &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(std::string name, double &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(name, outValue);
}

bool PT_DBRecordset::GetFieldValue(int64_t index, double &outValue)
{
	if(!IsCurrentValidResult()){
		return false;
	}
	
	return results[current_result].GetFieldValue(index, outValue);
}

int64_t PT_DBRecordset::GetFieldIndex(std::string &name)
{
	if(!IsCurrentValidResult()){
		return -1;
	}

	return results[current_result].GetFieldIndex(name);
}

int64_t PT_DBRecordset::GetFieldIndex(const char *name)
{
	if(!IsCurrentValidResult()){
		return -1;
	}
	
	return results[current_result].GetFieldIndex(name);
}

int64_t PT_DBRecordset::GetRecordCount()
{
	if(!IsCurrentValidResult()){
		return 0;
	}
	
	return results[current_result].GetRecordCount();
}

int64_t PT_DBRecordset::GetResultCount()
{
	return results.size();
}

int64_t PT_DBRecordset::GetAffectedRows()
{
    if(!IsCurrentValidResult()){
        return 0;
    }
    
    return results[current_result].affected_rows;
}
