/*
    ORM for MySQL
    Powered By Aleksandr Belov 2017
    wernher.pad@gmail.com
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using MySql.Data.MySqlClient;
using System.Collections;
using System.Diagnostics;
using System.Threading;

namespace NanoORMMySQL
{
    public class NanoOrmMySQL : IDisposable
    {
        #region Private Fields

        private MySqlConnection _connection;

        private readonly Stopwatch _diagStopwatch = new Stopwatch();

        #endregion Private Fields

        #region Properties

        /// <summary>
        /// Флаг логирования
        /// </summary>
        public bool Log { get; set; }

        /// <summary>
        /// Строка подключения
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Последний запрос к базе данных
        /// </summary>
        public string LastQuery { get; private set; }

        /// <summary>
        /// Не присваивает вставляемому объекту PrimaryKey.
        /// </summary>
        /// <remarks>Ускоряет вставку.</remarks>
        public bool NoGetLastID { get; set; } = false;

        #endregion Properties

        #region Constructor

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка подключения</param>
        public NanoOrmMySQL(string connectionString)
        {
            Log = true;
            ConnectionString = connectionString;
            _connection = new MySqlConnection(connectionString);

            if (Log)
            {
                Debug.WriteLine("Create new instance NanoOrm");
                Debug.WriteLine(connectionString);
            }
        }

        #endregion Constructor

        #region ORM

        /// <summary>
        /// Закрывает подключение к БД, но не освобождает объект
        /// </summary>
        public void CloseConnection()
        {
            _connection.Close();
        }

        /// <summary>
        /// Стартует транзакцию
        /// </summary>
        /// <param name="synchronousOFF">Флаг отключения синхронизации</param>
        public void BeginTransaction(bool synchronousOFF)
        {
            var query = "BEGIN;";
            if (synchronousOFF)
                query = "PRAGMA synchronous = OFF;" + query;
            var sqlRes = ExecuteNoData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Подтверждает транзакицю
        /// </summary>
        public void CommitTransaction()
        {
            var sqlRes = ExecuteNoData("COMMIT;PRAGMA synchronous = NORMAL;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Откатывает транзакцию
        /// </summary>
        public void RollbackTransaction()
        {
            var sqlRes = ExecuteNoData("ROLLBACK;PRAGMA synchronous = NORMAL;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Возвращает кол-во записей в таблице
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Кол-во записей</returns>
        public int Count<T>() where T : new()
        {
            return Count<T>(string.Empty);
        }

        /// <summary>
        /// Возвращает кол-во записей в таблице при условии
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <returns>Кол-во записей</returns>
        /// <example>Count("WHERE id &gt; 1000")</example>
        public int Count<T>(string where) where T : new()
        {
            var type = typeof(T);
            var tableName = GetTableName(type);
            var query = string.Format("SELECT COUNT(*) FROM {0} {1};", tableName, where);
            var sqlRes = ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            var res = Convert.ToInt32(sqlRes.Table.Rows[0].ItemArray[0]);
            sqlRes.Dispose();
            return Convert.ToInt32(sqlRes.Table.Rows[0].ItemArray[0]);
        }

        /// <summary>
        /// Возвращает записи по sql запросу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="sql">Запрос</param>
        /// <returns>Список объектов типа Т</returns>
        public List<T> Query<T>(string sql) where T : new()
        {
            var sqlRes = ExecuteData(sql);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            var res = new List<T>();
            foreach (DataRow row in sqlRes.Table.Rows)
                res.Add(RowToObject<T>(row));
            sqlRes.Dispose();
            return res;
        }

        /// <summary>
        /// Возвращает записи по параметризированному sql запросу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="sql">Запрос</param>
        /// <param name="param">Массив параметров</param>
        /// <returns>Список объектов типа Т</returns>
        /// <example>Query("SELECT * FROM testclass WHERE id &gt; ?",1000)</example>
        public List<T> Query<T>(string sql, params object[] param) where T : new()
        {
            var arr = sql.Split(new[] { '?' });
            var sb = new StringBuilder();
            for (int i = 0; i < param.Length; i++)
            {
                var val = param[i];
                if (val is string)
                    val = string.Format("'{0}'", val);
                sb.Append(arr[i]);
                sb.Append(val);
            }

            var sqlRes = ExecuteData(sb.ToString());
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            var res = new List<T>();
            foreach (DataRow row in sqlRes.Table.Rows)
                res.Add(RowToObject<T>(row));
            sqlRes.Dispose();
            return res;
        }

        /// <summary>
        /// Возвращает 1 запись по идентификатору
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="id">Идентификатор</param>
        /// <returns>Объект типа Т</returns>
        public T SelectOne<T>(object id) where T : new()
        {
            var type = typeof(T);
            var primaryKeyCollumn = string.Empty;
            foreach (var property in type.GetProperties())
            {
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    break;
                }
            }
            return SelectOne<T>(string.Format("WHERE {0} = {1}", primaryKeyCollumn, id));
        }

        /// <summary>
        /// Возвращает 1 запись по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <returns>Объект типа Т</returns>
        public T SelectOne<T>(string where) where T : new()
        {
            return Select<T>(where + " LIMIT 1").FirstOrDefault();
        }

        /// <summary>
        /// Возвращает 1 запись по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <param name="tableName">Имя таблицы</param>
        /// <returns>Объект типа Т</returns>
        public T SelectOne<T>(string where, string tableName) where T : new()
        {
            return Select<T>(where + " LIMIT 1", tableName).FirstOrDefault();
        }

        /// <summary>
        /// Синоним метода Select()
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Список объектов типа Т</returns>
        public SQLSelect<T> Table<T>() where T : class, new()
        {
            return new SQLSelect<T>(this);
        }

        /// <summary>
        /// Синоним метода Select()
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="tableName">Имя таблицы</param>
        /// <returns>Список объектов типа Т</returns>
        public SQLSelect<T> Table<T>(string tableName) where T : class, new()
        {
            return new SQLSelect<T>(this, tableName);
        }

        /// <summary>
        /// Возвращает все сохранненые в таблице записи
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Список объектов типа Т</returns>
        public List<T> Select<T>() where T : new()
        {
            return Select<T>(string.Empty);
        }

        /// <summary>
        /// Возвращает список объектов по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <returns>Список объектов типа Т</returns>
        public List<T> Select<T>(string where) where T : new()
        {
            var type = typeof(T);
            string tableName = GetTableName(type);
            return Select<T>(where, tableName);
        }

        /// <summary>
        /// Возвращает список объектов по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <param name="tableName">Имя таблицы</param>
        /// <returns>Список объектов типа Т</returns>
        public List<T> Select<T>(string where, string tableName) where T : new()
        {
            var query = string.Format("SELECT * FROM {0} {1};", tableName, where);
            var sqlRes = ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            var res = new List<T>();
            foreach (DataRow row in sqlRes.Table.Rows)
                res.Add(RowToObject<T>(row));
            sqlRes.Dispose();
            return res;
        }

        /// <summary>
        /// Удаляет объект из базы
        /// </summary>
        /// <param name="obj">Объект для удаления</param>
        /// <remarks>У объекта обязательно должен быть указан атрибут PrimaryKey</remarks>
        public void Delete(object obj)
        {
            var type = obj.GetType();
            string tableName = GetTableName(type);
            var primaryKeyCollumn = string.Empty;
            object primaryKeyData = null;
            foreach (var property in type.GetProperties())
            {
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    primaryKeyData = property.GetValue(obj, null);
                    break;
                }
            }
            if (primaryKeyData == null)
                throw new Exception("PrimaryKey not found");
            var query = string.Format("DELETE FROM {0} WHERE {1} = {2};", tableName, primaryKeyCollumn, primaryKeyData);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Удаление через LINQ подобный запрос
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> DeleteWhere<T>() where T : class, new()
        {
            return new SQLDelete<T>(this);
        }

        /// <summary>
        /// Удаляет объект из базы по идентификатору
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="id">Идентификатор</param>
        public void DeleteByID<T>(object id) where T : new()
        {
            var type = typeof(T);
            string tableName = GetTableName(type);
            var primaryKeyCollumn = string.Empty;
            foreach (var property in type.GetProperties())
            {
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    break;
                }
            }
            if (string.IsNullOrEmpty(primaryKeyCollumn))
                throw new Exception("PrimaryKey not found");
            var query = string.Format("DELETE FROM {0} WHERE {1} = {2};", tableName, primaryKeyCollumn, id);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Удаляет объекты из базы по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        public void DeleteByWhere<T>(string where)
        {
            var type = typeof(T);
            string tableName = GetTableName(type);
            var query = string.Format("DELETE FROM {0} {1};", tableName, where);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Удаляет все.
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        public void DeleteAll<T>()
        {
            var type = typeof(T);
            string tableName = GetTableName(type);
            DeleteAll(tableName);
        }

        /// <summary>
        /// Удаляет все.
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public void DeleteAll(string tableName)
        {
            var query = string.Format("DELETE FROM {0};", tableName);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Обновляет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        public void Update(object obj)
        {
            var type = obj.GetType();
            string tableName = GetTableName(type);
            Update(obj, tableName);
        }

        /// <summary>
        /// Обновляет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        /// <param name="tableName">Имя таблицы</param>
        public void Update(object obj, string tableName)
        {
            var type = obj.GetType();
            var sb = new StringBuilder();

            sb.AppendFormat("UPDATE `{0}` SET ", tableName).AppendLine();
            var primaryKeyCollumn = string.Empty;
            object primaryKeyData = null;
            foreach (var property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    primaryKeyData = property.GetValue(obj, null);
                    continue;
                }

                var colName = GetColName(property);
                var tmpl = "{0} = {1},";
                if (IsString(property))
                    tmpl = "{0} = '{1}',";
                var value = property.GetValue(obj, null);
                value = ConvertValue(property, value);
                if (!IsString(property))
                    sb.AppendFormat(tmpl, colName, value.ToString().Replace(",", ".")).AppendLine();
                else
                    sb.AppendFormat(tmpl, colName, value).AppendLine();
            }
            if (primaryKeyData == null)
                throw new Exception("PrimaryKey not found");
            var query = sb.ToString();
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1);
            query += string.Format(" WHERE `{0}`.`{1}` = {2};", tableName, primaryKeyCollumn, primaryKeyData);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Обновление через LINQ подобный запрос
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> UpdateWhere<T>() where T : class, new()
        {
            return new SQLUpdate<T>(this);
        }

        /// <summary>
        /// Обновляет список объектов в базе
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="list">Список объектов</param>
        public void UpdateAll<T>(IList list)
        {
            BeginTransaction(true);
            try
            {
                foreach (var obj in (List<T>)list)
                    Update(obj);
            }
            catch
            {
                RollbackTransaction();
                return;
            }
            CommitTransaction();
        }

        /// <summary>
        /// Сохраняет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        public void Insert(object obj)
        {
            var type = obj.GetType();
            var tableName = GetTableName(type);
            Insert(obj, tableName);
        }

        /// <summary>
        /// Сохраняет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        /// <param name="tableName">Имя таблицы</param>
        public void Insert(object obj, string tableName)
        {
            var type = obj.GetType();
            var query = string.Format("INSERT INTO `{0}`(", tableName);
            var query2 = "VALUES (";

            foreach (var property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                if (IsPrimaryKey(property))
                    continue;

                query += string.Format("`{0}`,", GetColName(property));
                var tmpl = "{0},";
                var value = property.GetValue(obj, null);
                if (IsString(property))
                    tmpl = "'{0}',";
                if (value == null && IsString(property))
                {
                    value = string.Empty;
                }
                else
                {
                    value = ConvertValue(property, value);
                }
                if (!IsString(property))
                    query2 += string.Format(tmpl, value.ToString().Replace(",", "."));
                else
                    query2 += string.Format(tmpl, value);
            }
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + ")";
            query2 = query2.Remove(query2.LastIndexOf(",", StringComparison.Ordinal), 1) + ");";
            query += query2;
            if (!NoGetLastID)
            {
                query += "SELECT LAST_INSERT_ID();";
                var sqlres = ExecuteData(query);
                if (sqlres.HasError)
                    throw new Exception(sqlres.Message);

                foreach (var property in type.GetProperties())
                {
                    if (IsPrimaryKey(property))
                    {
                        property.SetPropertyValue(sqlres.Table.Rows[0].ItemArray[0], obj);
                        break;
                    }
                }
                sqlres.Dispose();
            }
            else
            {
                var sqlres = ExecuteNoData(query);
                if (sqlres.HasError)
                    throw new Exception(sqlres.Message);
            }
        }

        /// <summary>
        /// Вставляет список объектов в базу в одной транзакции В случае ошибки вставки откатывает транзакцию
        /// </summary>
        /// <param name="list">Список объектов</param>
        public void InsertAll(IList list)
        {
            InsertAll(list, true);
        }

        /// <summary>
        /// Вставляет список объектов в базу в одной транзакции В случае ошибки вставки откатывает транзакцию
        /// </summary>
        /// <param name="list">Список объектов</param>
        /// <param name="synchronousOFF">Флаг отключения синхронизации</param>
        public void InsertAll(IList list, bool synchronousOFF)
        {
            BeginTransaction(synchronousOFF);
            try
            {
                foreach (var obj in list)
                    Insert(obj);
            }
            catch
            {
                RollbackTransaction();
                return;
            }
            CommitTransaction();
        }

        /// <summary>
        /// Сохраняет список объектов в базе
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="list">Список объектов</param>
        /// <param name="tableName"></param>
        public void InsertAll<T>(object list, string tableName) where T : new()
        {
            BeginTransaction(true);
            try
            {
                foreach (var obj in (List<T>)list)
                    Insert(obj, tableName);
            }
            catch
            {
                RollbackTransaction();
                return;
            }
            CommitTransaction();
        }

        /// <summary>
        /// Создает таблицу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        public void CreateTable<T>()
        {
            var type = typeof(T);
            string tableName = GetTableName(type);
            CreateTable<T>(tableName);
        }

        /// <summary>
        /// Создает таблицу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="tableName">Имя таблицы</param>
        public void CreateTable<T>(string tableName)
        {
            var type = typeof(T);
            var indexList = new List<string>();
            string primaryKeyText = string.Empty;
            var sb = new StringBuilder();
            sb.AppendFormat("CREATE TABLE IF NOT EXISTS {0} (", tableName).AppendLine();
            foreach (var property in type.GetProperties())
            {
                var colName = property.Name;
                if (IsNoMapAttribute(property))
                    continue;
                bool primaryKey = IsPrimaryKey(property);
                var att = property.GetCustomAttributes(typeof(AutoIncrement), true);
                bool autoIncrement = (att != null && att.Length != 0);

                att = property.GetCustomAttributes(typeof(NotNULL), true);
                bool notNull = (att != null && att.Length != 0);

                att = property.GetCustomAttributes(typeof(Indexed), true);
                if (att != null && att.Length != 0) indexList.Add(colName);

                colName = GetColName(property);
                var colType = GetColType(property);
                sb.AppendFormat("\t`{0}` {1}", colName, colType);
                if (primaryKey)
                {
                    primaryKeyText = string.Format(" PRIMARY KEY(`{0}`)", colName);
                }
                if (autoIncrement)
                    sb.Append(" AUTO_INCREMENT");
                if (notNull)
                    sb.Append(" NOT NULL");
                sb.AppendLine(",");
            }
            sb.Append(primaryKeyText).Append(");");
            var query = sb.ToString();
            //query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + ");";

            query = indexList.Aggregate(query, (current, colindex) => current + string.Format("CREATE INDEX  IF NOT EXISTS name_{0} ON {1}({2});", colindex, tableName, colindex));

            //Debug.WriteLine(query);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Уничтожает таблицу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        public void DropTable<T>()
        {
            var type = typeof(T);
            var tableName = type.Name;
            foreach (var attribute in type.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof(TableName))
                {
                    tableName = ((TableName)attribute).Name;
                    break;
                }
            }
            DropTable(tableName);
        }

        /// <summary>
        /// Уничтожает таблицу
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public void DropTable(string tableName)
        {
            var query = string.Format("DROP TABLE IF EXISTS {0};", tableName);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Создает индекс в таблице
        /// </summary>
        /// <param name="indexName">Имя индекса</param>
        /// <param name="colName">Столбец</param>
        /// <param name="tableName">Имя таблицы</param>
        public void CreateIndex(string indexName, string colName, string tableName)
        {
            var query = string.Format("CREATE INDEX `{0}` ON `{1}` (`{2}`);", indexName, tableName, colName);
            var sqlRes = ExecuteNoData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Сжатие базы
        /// </summary>
        public void Vacuum()
        {
            var sqlRes = ExecuteNoData("VACUUM;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        #endregion ORM

        #region Private Methods

        private object ConvertValue(PropertyInfo property, object value)
        {
            if (property.PropertyType == typeof(DateTime))
                value = ((DateTime)value).ConvertToUnixTimestamp().ToString(CultureInfo.InvariantCulture);
            if (property.PropertyType == typeof(byte[]))
                value = Convert.ToBase64String((byte[])value);
            if (property.PropertyType == typeof(bool))
                value = ((bool)value) ? "1" : "0";
            if (property.PropertyType != typeof(DateTime) && property.PropertyType != typeof(bool))
            {
                if (value != null)
                {
                    value = value.ToString();
                }
                else
                {
                    value = default(int);
                }
            }

            return value;
        }

        private T RowToObject<T>(DataRow row) where T : new()
        {
            var res = new T();
            var type = typeof(T);
            foreach (var property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                var colName = GetColName(property);
                if (row.Table.Columns.Contains(colName))
                    property.SetPropertyValue(row[colName], res);
            }

            return res;
        }

        private string GetColType(PropertyInfo property)
        {
            object[] att = property.GetCustomAttributes(typeof(CollumnType), true);
            if (att != null && att.Length != 0)
                return ((CollumnType)att[0]).Type;
            if (property.PropertyType == typeof(int))
                return "INT";
            if (property.PropertyType == typeof(uint))
                return "INT UNSIGNED";
            if (property.PropertyType == typeof(long))
                return "BIGINT";
            if (property.PropertyType == typeof(ulong))
                return "BIGINT UNSIGNED";
            if (property.PropertyType == typeof(sbyte))
                return "TINYINT";
            if (property.PropertyType == typeof(bool))
                return "BIT";
            if (property.PropertyType == typeof(float))
                return "FLOAT";
            if (property.PropertyType == typeof(double))
                return "DOUBLE";
            if (property.PropertyType == typeof(DateTime))
                return "DOUBLE";
            if (property.PropertyType == typeof(byte[]))
                return "LONGTEXT";
            if (property.PropertyType == typeof(Guid))
                return "VARCHAR(32)";
            if (IsString(property))
                return "TEXT";

            return "TEXT";
        }

        private string GetTableName(Type type)
        {
            var tableName = type.Name;
            foreach (var attribute in type.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof(TableName))
                {
                    tableName = ((TableName)attribute).Name;
                    break;
                }
            }

            return tableName;
        }

        private static bool IsNoMapAttribute(PropertyInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(NoMap), true);
            return att != null && att.Length != 0;
        }

        private static bool IsPrimaryKey(PropertyInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(PrimaryKey), true);
            return att != null && att.Length != 0;
        }

        private static bool IsString(PropertyInfo propertyInfo)
        {
            if (propertyInfo.PropertyType == typeof(string))
                return true;
            return propertyInfo.PropertyType == typeof(Guid);
        }

        private static string GetColName(MemberInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(CollumnName), true);
            if (att != null && att.Length != 0)
                return ((CollumnName)att[0]).Name;
            return propertyInfo.Name;
        }

        private void CreateOpenAndCheckConnetion()
        {
            if (_connection == null)
            {
                _connection = new MySqlConnection(ConnectionString);
            }

            if (_connection.State != ConnectionState.Open)
            {
                if (_connection.State == ConnectionState.Closed)
                    _connection.Open();
            }

            if (_connection.State == ConnectionState.Executing || _connection.State == ConnectionState.Fetching)
                Thread.Sleep(100);
        }

        #endregion Private Methods

        #region DB Execute

        /// <summary>
        /// Выполняет запрос в базе без возвращения данных
        /// </summary>
        /// <param name="query">Запрос</param>
        /// <returns>Результат запроса</returns>
        public SQLReturn ExecuteNoData(string query)
        {
            if (string.IsNullOrEmpty(query))
                throw new Exception("Query is null");
            LastQuery = (string)query.Clone();
            if (Log)
            {
                Debug.WriteLine(query);
                _diagStopwatch.Reset();
                _diagStopwatch.Start();
            }

            try
            {
                CreateOpenAndCheckConnetion();

                lock (_connection)
                {
                    var command = new MySqlCommand(query, _connection);
                    command.ExecuteNonQuery();
                    command.Dispose();
                }

                if (Log)
                {
                    _diagStopwatch.Stop();
                    Debug.WriteLine(string.Concat("Query Time: ", _diagStopwatch.ElapsedMilliseconds, " ms."));
                    _diagStopwatch.Reset();
                }

                return new SQLReturn();
            }
            catch (Exception ex)
            {
                return new SQLReturn(ex);
            }
        }

        /// <summary>
        /// Выполняет запрос в базе с возвращением данных
        /// </summary>
        /// <param name="query">Запрос</param>
        /// <returns>Результат запроса</returns>
        public SQLReturnData ExecuteData(string query)
        {
            if (string.IsNullOrEmpty(query))
                throw new Exception("Query is null");
            LastQuery = (string)query.Clone();
            if (Log)
            {
                Debug.WriteLine(query);
                _diagStopwatch.Stop();
                _diagStopwatch.Reset();
                _diagStopwatch.Start();
            }

            try
            {
                CreateOpenAndCheckConnetion();

                lock (_connection)
                {
                    var adapter = new MySqlDataAdapter(query, _connection);
                    var ds = new DataSet();
                    adapter.Fill(ds);
                    if (Log)
                    {
                        var res = new SQLReturnData(ds.Tables[0]);
                        _diagStopwatch.Stop();
                        Debug.WriteLine(string.Concat("Query Time: ", _diagStopwatch.ElapsedMilliseconds, " ms."));
                        _diagStopwatch.Reset();
                        adapter.Dispose();
                        ds.Dispose();
                        return res;
                    }
                    adapter.Dispose();
                    ds.Dispose();
                    return new SQLReturnData(ds.Tables[0]);
                }
            }
            catch (Exception ex)
            {
                return new SQLReturnData(ex);
            }
        }

        #endregion DB Execute

        #region IDisposable

        /// <summary>
        /// Выполняет определяемые приложением задачи, связанные с удалением, высвобождением или
        /// сбросом неуправляемых ресурсов.
        /// </summary>
        public void Dispose()
        {
            _connection?.Close();
            _connection = null;
            GC.Collect();
        }

        #endregion IDisposable
    }

#region Result Classes
#endregion Result Classes
#region Interfaces
#endregion Interfaces
#region Attributes
#endregion Attributes
#region Extension
#endregion Extension
}