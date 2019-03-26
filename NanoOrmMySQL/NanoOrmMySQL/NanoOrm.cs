/*
	ORM for MySQL
	Powered By Aleksandr Belov 2017
	wernher.pad@gmail.com
*/

using MySql.Data.MySqlClient;
using MySql.Data.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

namespace NanoORMMySQL
{
    public class NanoOrmMySQL : IDisposable
    {
        #region Private Fields

        /// <summary>
        /// Объект подключения кБД
        /// </summary>
        private MySqlConnection _connection;

        /// <summary>
        /// Таймер для отладочных сообщений
        /// </summary>
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
        public bool NoGetLastID { get; set; }

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
        /// Возращает строку для бекапа таблицы
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public string Backup<T>() where T : new()
        {
            string tableName = GetTableName(typeof(T));
            return BackupTable(tableName);
        }

        /// <summary>
        /// Возращает строку для бекапа таблицы
        /// </summary>
        /// <param name="tblName">Имя таблицы</param>
        /// <returns></returns>
        public string BackupTable(string tblName)
        {
            var fileText = new StringBuilder();
            fileText.AppendLine(string.Concat("DROP TABLE IF EXISTS `", tblName, "`;"));
            string query = string.Format("SHOW CREATE TABLE `{0}`", tblName);
            SQLReturnData sqlRaw = ExecuteData(query);
            if (sqlRaw.HasError)
                throw new Exception(sqlRaw.Message);
            fileText.Append(sqlRaw.Table.Rows[0].ItemArray[1]).AppendLine(";");
            fileText.Append("LOCK TABLES `").Append(tblName).AppendLine("` WRITE;");
            SQLReturnData raw2 = ExecuteData(string.Concat("SELECT * FROM `", tblName, "`"));
            string beginLine = string.Concat("INSERT INTO `", tblName, "`");
            var sb = new StringBuilder();
            int j = 0;

            foreach (DataRow row in raw2.Table.Rows)
            {
                string line = string.Empty;
                if (j == 0)
                    line += string.Concat(beginLine, " VALUES");

                line += " (";
                int colCount = row.ItemArray.Length;
                for (int i = 0; i < colCount; i++)
                {
                    string value = row[i].ToString().Replace(",", ".");
                    Type t = row[i].GetType();
                    if (t == typeof(string))
                        value = string.Concat("'", value.Replace("\r", string.Empty).Replace("\n", string.Empty), "'");

                    line += value;
                    if (i < colCount - 1)
                        line += ",";
                }
                line += ")";
                if (j == 1000)
                {
                    sb.Append(line);
                    fileText.Append(sb).AppendLine(";");
                    sb = new StringBuilder();
                    j = 0;
                    continue;
                }
                line += ",";
                sb.Append(line);
                j++;
            }
            if (sb.Length != 0)
                fileText.Append(sb.ToString(0, sb.Length - 1)).AppendLine(";");
            fileText.AppendLine("UNLOCK TABLES;");
            return fileText.ToString();
        }

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
        public void BeginTransaction()
        {
            SQLReturn sqlRes = ExecuteNoData("BEGIN;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Подтверждает транзакицю
        /// </summary>
        public void CommitTransaction()
        {
            SQLReturn sqlRes = ExecuteNoData("COMMIT;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Откатывает транзакцию
        /// </summary>
        public void RollbackTransaction()
        {
            SQLReturn sqlRes = ExecuteNoData("ROLLBACK;");
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
            Type type = typeof(T);
            string query = string.Format("SELECT COUNT(*) FROM {0} {1};", GetTableName(type), where);
            SQLReturnData sqlRes = ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            int res = Convert.ToInt32(sqlRes.Table.Rows[0].ItemArray[0]);
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
            SQLReturnData sqlRes = ExecuteData(sql);
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
            string[] arr = sql.Split(new[] { '?' });
            var sb = new StringBuilder();
            for (int i = 0; i < param.Length; i++)
            {
                object val = param[i];
                if (val is string)
                    val = string.Format("'{0}'", val);
                sb.Append(arr[i]);
                sb.Append(val);
            }

            SQLReturnData sqlRes = ExecuteData(sb.ToString());
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
            Type type = typeof(T);
            string primaryKeyCollumn = string.Empty;
            foreach (PropertyInfo property in type.GetProperties())
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
            Type type = typeof(T);
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
            string query = string.Format("SELECT * FROM {0} {1};", tableName, where);
            SQLReturnData sqlRes = ExecuteData(query);
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
            Type type = obj.GetType();
            string tableName = GetTableName(type);
            Delete(obj, tableName);
        }

        public void Delete(object obj, string tableName)
        {
            Type type = obj.GetType();
            string primaryKeyCollumn = string.Empty;
            object primaryKeyData = null;
            foreach (PropertyInfo property in type.GetProperties())
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
            string query = string.Format("DELETE FROM {0} WHERE {1} = {2};", tableName, primaryKeyCollumn, primaryKeyData);
            SQLReturn sqlres = ExecuteNoData(query);
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
            Type type = typeof(T);
            string tableName = GetTableName(type);
            string primaryKeyCollumn = string.Empty;
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    break;
                }
            }
            if (string.IsNullOrEmpty(primaryKeyCollumn))
                throw new Exception("PrimaryKey not found");
            string query = string.Format("DELETE FROM {0} WHERE {1} = {2};", tableName, primaryKeyCollumn, id);
            SQLReturn sqlres = ExecuteNoData(query);
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
            Type type = typeof(T);
            string tableName = GetTableName(type);
            string query = string.Format("DELETE FROM {0} {1};", tableName, where);
            SQLReturn sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Удаляет все.
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        public void DeleteAll<T>()
        {
            Type type = typeof(T);
            string tableName = GetTableName(type);
            DeleteAll(tableName);
        }

        /// <summary>
        /// Удаляет все.
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public void DeleteAll(string tableName)
        {
            string query = string.Format("DELETE FROM {0};", tableName);
            SQLReturn sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Обновляет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        public void Update(object obj)
        {
            Type type = obj.GetType();
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
            Type type = obj.GetType();
            var sb = new StringBuilder();

            sb.AppendFormat("UPDATE `{0}` SET ", tableName).AppendLine();
            string primaryKeyCollumn = string.Empty;
            object primaryKeyData = null;
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                if (IsPrimaryKey(property))
                {
                    primaryKeyCollumn = GetColName(property);
                    primaryKeyData = property.GetValue(obj, null);
                    continue;
                }

                string colName = GetColName(property);
                string tmpl = "`{0}` = {1},";
                if (IsString(property))
                    tmpl = "`{0}` = '{1}',";
                object value = property.GetValue(obj, null);
                value = ConvertValue(property, value);
                if (!IsString(property))
                    sb.AppendFormat(tmpl, colName, value.ToString().Replace(",", ".")).AppendLine();
                else
                    sb.AppendFormat(tmpl, colName, value).AppendLine();
            }
            if (primaryKeyData == null)
                throw new Exception("PrimaryKey not found");
            string query = sb.ToString();
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1);
            query += string.Format(" WHERE `{0}`.`{1}` = {2};", tableName, primaryKeyCollumn, primaryKeyData);
            SQLReturn sqlres = ExecuteNoData(query);
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
            BeginTransaction();
            try
            {
                foreach (T obj in (List<T>)list)
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
            Type type = obj.GetType();
            string tableName = GetTableName(type);
            Insert(obj, tableName);
        }

        /// <summary>
        /// Сохраняет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        /// <param name="tableName">Имя таблицы</param>
        public void Insert(object obj, string tableName)
        {
            Type type = obj.GetType();
            string query = string.Format("INSERT INTO `{0}`(", tableName);
            string query2 = "VALUES (";
            CreateInsertQuery(obj, type, ref query, ref query2);
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + ")";
            query2 = query2.Remove(query2.LastIndexOf(",", StringComparison.Ordinal), 1) + ");";
            query += query2;
            if (!NoGetLastID)
            {
                query += "SELECT LAST_INSERT_ID();";
                SQLReturnData sqlres = ExecuteData(query);
                if (sqlres.HasError)
                    throw new Exception(sqlres.Message);

                foreach (PropertyInfo property in type.GetProperties())
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
                SQLReturn sqlres = ExecuteNoData(query);
                if (sqlres.HasError)
                    throw new Exception(sqlres.Message);
            }
        }

        private void CreateInsertQuery(object obj, Type type, ref string query, ref string query2)
        {
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                if (IsPrimaryKey(property))
                    continue;

                query += string.Format("`{0}`,", GetColName(property));
                object value = property.GetValue(obj, null);
                bool isString = IsString(property);
                if (value == null && isString)
                    value = string.Empty;
                else
                    value = ConvertValue(property, value);
                string tmpl = "{0},";
                if (!isString)
                {
                    query2 += string.Format(tmpl, value.ToString().Replace(",", "."));
                }
                else
                {
                    tmpl = "'{0}',";
                    query2 += string.Format(tmpl, value);
                }
            }
        }

        /// <summary>
        /// Вставляет список объектов в базу в одной транзакции В случае ошибки вставки откатывает транзакцию
        /// </summary>
        /// <param name="list">Список объектов</param>
        public void InsertAll(IList list)
        {
            BeginTransaction();
            try
            {
                foreach (object obj in list)
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
            BeginTransaction();
            try
            {
                foreach (T obj in (List<T>)list)
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
            Type type = typeof(T);
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
            Type type = typeof(T);
            var indexList = new List<string>();
            string primaryKeyText = string.Empty;
            var sb = new StringBuilder();
            sb.AppendFormat("CREATE TABLE IF NOT EXISTS {0} (", tableName).AppendLine();
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                string colName = property.Name;
                bool primaryKey, autoIncrement, notNull;
                CheckAttributes(indexList, property, colName, out primaryKey, out autoIncrement, out notNull);

                colName = GetColName(property);
                string colType = GetColType(property);
                sb.AppendFormat("\t`{0}` {1}", colName, colType);
                if (primaryKey)
                    primaryKeyText = string.Format(" PRIMARY KEY(`{0}`)", colName);
                if (autoIncrement)
                    sb.Append(" AUTO_INCREMENT");
                if (notNull)
                    sb.Append(" NOT NULL");
                sb.AppendLine(",");
            }
            sb.Append(primaryKeyText).Append(");");

            string query = sb.ToString();
            query = indexList.Aggregate(query, (current, colindex) => current + string.Format("CREATE INDEX  IF NOT EXISTS name_{0} ON {1}({2});", colindex, tableName, colindex));
            SQLReturn sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        private static void CheckAttributes(List<string> indexList, PropertyInfo property, string colName, out bool primaryKey, out bool autoIncrement, out bool notNull)
        {
            primaryKey = IsPrimaryKey(property);
            object[] att = property.GetCustomAttributes(typeof(AutoIncrement), true);
            autoIncrement = (att != null && att.Length != 0);
            att = property.GetCustomAttributes(typeof(NotNULL), true);
            notNull = (att != null && att.Length != 0);
            att = property.GetCustomAttributes(typeof(Indexed), true);
            if (att != null && att.Length != 0)
                indexList.Add(colName);
        }

        /// <summary>
        /// Уничтожает таблицу
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        public void DropTable<T>()
        {
            Type type = typeof(T);
            string tableName = type.Name;
            foreach (object attribute in type.GetCustomAttributes(false))
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
            string query = string.Format("DROP TABLE IF EXISTS {0};", tableName);
            SQLReturn sqlres = ExecuteNoData(query);
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
            string query = string.Format("CREATE INDEX `{0}` ON `{1}` (`{2}`);", indexName, tableName, colName);
            SQLReturn sqlRes = ExecuteNoData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Сжатие базы
        /// </summary>
        public void Vacuum()
        {
            SQLReturn sqlRes = ExecuteNoData("VACUUM;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        #endregion ORM

        #region Private Methods

        private object ConvertValue(PropertyInfo property, object value)
        {
            if (property.PropertyType == typeof(DateTime))
            {
                //2015-11-05 14:29:36
                value = ((DateTime)value).ToString("yyyy-MM-dd hh:mm:ss");
                //var temp = (MySqlDateTime)value;
                //value = (DateTime)temp;
            }
                
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
            Type type = typeof(T);
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (IsNoMapAttribute(property))
                    continue;
                string colName = GetColName(property);
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
                return "DATETIME";
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
            string tableName = type.Name;
            foreach (object attribute in type.GetCustomAttributes(false))
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
            object[] att = propertyInfo.GetCustomAttributes(typeof(NoMap), true);
            return att != null && att.Length != 0;
        }

        private static bool IsPrimaryKey(PropertyInfo propertyInfo)
        {
            object[] att = propertyInfo.GetCustomAttributes(typeof(PrimaryKey), true);
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
            object[] att = propertyInfo.GetCustomAttributes(typeof(CollumnName), true);
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
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _connection?.Close();
                _connection.Dispose();
                _connection = null;
            }
        }

        #endregion IDisposable
    }

    #region Result Classes

    /// <summary>
    /// Результат запроса к базе данных без возвращаемых данных
    /// </summary>
    public class SQLReturn : ISQLReturn
    {
        /// <summary>
        /// Флаг ошибки
        /// </summary>
        public bool HasError { get; set; }

        /// <summary>
        /// Сообщение ошибки
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLReturn()
        {
        }

        /// <summary>
        /// Имплицирует тип Exception на себя
        /// </summary>
        /// <param name="exp">Объект ошибки</param>
        public SQLReturn(Exception exp)
        {
            HasError = true;
            Message = exp.Message;
        }
    }

    /// <summary>
    /// Результат запроса к базе с возвращением данных
    /// </summary>
    public class SQLReturnData : SQLReturn, ISQLReturnData, IDisposable
    {
        /// <summary>
        /// Данные запроса
        /// </summary>
        public DataTable Table { get; set; }

        /// <summary>
        /// Имплицирует тип Exception на себя
        /// </summary>
        /// <param name="exp">Объект ошибки</param>
        public SQLReturnData(Exception exp)
            : base(exp)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLReturnData()
        {
        }

        /// <summary>
        /// Имплицирует тип DataTable на себя
        /// </summary>
        /// <param name="table">Данные запроса</param>
        public SQLReturnData(DataTable table)
        {
            Table = table;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        private void Dispose(bool disposing)
        {
            if (disposing)
                Table?.Dispose();
        }
    }

    #endregion Result Classes

    #region Interfaces

    public interface ISQLReturn
    {
        bool HasError { get; set; }
        string Message { get; set; }
    }

    public interface ISQLReturnData
    {
        DataTable Table { get; set; }
    }

    #endregion Interfaces

    #region Attributes

    [AttributeUsage(AttributeTargets.Property)]
    public class NoMap : Attribute
    {
        public bool Map { get; set; }

        public NoMap()
        {
        }

        public NoMap(bool map)
        {
            Map = map;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class Indexed : Attribute
    {
        public bool Index { get; set; }

        public Indexed()
        {
        }

        public Indexed(bool index)
        {
            Index = index;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class NotNULL : Attribute
    {
        public bool NULL { get; set; }

        public NotNULL()
        {
        }

        public NotNULL(bool notNULL)
        {
            NULL = notNULL;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKey : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class AutoIncrement : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class TableName : Attribute
    {
        public string Name { get; set; }

        public TableName(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class CollumnType : Attribute
    {
        public string Type { get; set; }

        public CollumnType(string type)
        {
            Type = type;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class CollumnName : Attribute
    {
        public string Name { get; set; }

        public CollumnName(string name)
        {
            Name = name;
        }
    }

    #endregion Attributes

    #region Extension

    /// <summary>
    /// Класс методов расширения
    /// </summary>
    public static class ObjectExtension
    {
        /// <summary>
        /// Присваивает указанный объект свойству объекта
        /// </summary>
        /// <param name="val">Объект значения</param>
        /// <param name="property">Свойство</param>
        /// <param name="obj">Целевой объект</param>
        public static void SetPropertyValue(this PropertyInfo property, object val, object obj)
        {
            if (property.PropertyType == typeof(DateTime))
                //property.SetValue(obj, ((double)val).ConvertFromUnixTimestamp(), null);
                property.SetValue(obj, (DateTime)val, null);
            else if (property.PropertyType == typeof(bool))
                property.SetValue(obj, val.ToString() == "1", null);
            else if (property.PropertyType == typeof(int))
                property.SetValue(obj, Convert.ToInt32(val), null);
            else if (property.PropertyType == typeof(uint))
                property.SetValue(obj, Convert.ToUInt32(val), null);
            else if (property.PropertyType == typeof(long))
                property.SetValue(obj, Convert.ToInt64(val), null);
            else if (property.PropertyType == typeof(ulong))
                property.SetValue(obj, Convert.ToUInt64(val), null);
            else if (property.PropertyType == typeof(sbyte))
                property.SetValue(obj, Convert.ToInt32(val), null);
            else if (property.PropertyType == typeof(float))
                property.SetValue(obj, Convert.ToSingle(val), null);
            else if (property.PropertyType == typeof(double))
                property.SetValue(obj, Convert.ToDouble(val), null);
            else if (property.PropertyType == typeof(byte[]))
                property.SetValue(obj, Convert.FromBase64String(val.ToString()), null);
            else if (property.PropertyType == typeof(string))
                property.SetValue(obj, val.ToString(), null);
            else if (property.PropertyType == typeof(Guid))
                property.SetValue(obj, new Guid(val.ToString()), null);
            else
                property.SetValue(obj, val, null);
        }

        /// <summary>
        /// Конвертирует дату из форматат UnixTime в DateTime
        /// </summary>
        /// <param name="timestamp">Дата в формате UnixTime</param>
        /// <returns>Дата в формате DateTime</returns>
        public static DateTime ConvertFromUnixTimestamp(this double timestamp)
        {
            return timestamp.ConvertDateTimeFromDouble();
        }

        /// <summary>
        /// Конвертирует дату из форматат DateTime в UnixTime
        /// </summary>
        /// <param name="dateTime">Дата в формате DateTime</param>
        /// <returns>Дата в формате UnixTime</returns>
        public static double ConvertToUnixTimestamp(this DateTime dateTime)
        {
            return dateTime.ConvertDateTimeToDouble();
        }

        public static double ConvertDateTimeToDouble(this DateTime value)
        {
            double res = value.Year * Math.Pow(10, 10);
            res += value.Month * Math.Pow(10, 8);
            res += value.Day * Math.Pow(10, 6);
            res += value.Hour * Math.Pow(10, 4);
            res += value.Minute * Math.Pow(10, 2);
            res += value.Second;
            return res;
        }

        public static DateTime ConvertDateTimeFromDouble(this double value)
        {
            string str = value.ToString();
            if (str.Length == 10)
            {
                //Для совместимости со старым форматом
                var origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
                return origin.AddSeconds(value);
            }
            if (str.Length < 14)
                return new DateTime(1, 1, 1, 0, 0, 0);//Костыль упрощения

            var res = new DateTime(
                int.Parse(str.Substring(0, 4)),
                int.Parse(str.Substring(4, 2)),
                int.Parse(str.Substring(6, 2)),
                int.Parse(str.Substring(8, 2)),
                int.Parse(str.Substring(10, 2)),
                int.Parse(str.Substring(12, 2)));
            return res;
        }

        /// <summary>
        /// Возвращает подготовленную строку
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string GetSQLValue(this object value)
        {
            if (value is string || value is char || value is Guid)
                return string.Concat("'", value.ToString(), "'");
            if (value.GetType() == typeof(bool))
                return ((bool)value) ? "1" : "0";
            if (value is DateTime dateTime)
                (dateTime).ConvertToUnixTimestamp();
            return value.ToString();
        }
    }

    #endregion Extension

    /// <summary>
    /// Базовый класс запроса
    /// </summary>
    /// <typeparam name="T">Тип</typeparam>
    public abstract class SQLBase<T>
    {
        /// <summary>
        /// ОRM
        /// </summary>
        protected NanoOrmMySQL _orm;

        /// <summary>
        /// Строки Limit
        /// </summary>
        protected string _limit = string.Empty;

        /// <summary>
        /// Условия
        /// </summary>
        protected List<Condition> _conditions = new List<Condition>();

        /// <summary>
        /// Имя таблицы
        /// </summary>
        protected string _globalTableName = string.Empty;

        #region Конструкторы

        /// <summary>
        /// Конструктор
        /// </summary>
        protected SQLBase()
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        protected SQLBase(string tableName)
        {
            _globalTableName = tableName;
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        protected SQLBase(NanoOrmMySQL orm)
        {
            _orm = orm;
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        /// <param name="tableName">Имя таблицы</param>
        protected SQLBase(NanoOrmMySQL orm, string tableName)
        {
            _orm = orm;
            _globalTableName = tableName;
        }

        #endregion Конструкторы

        /// <summary>
        /// Формирует из списка условий строку Where
        /// </summary>
        /// <param name="sbQuery"></param>
        protected void AddWhere(StringBuilder sbQuery)
        {
            Condition where = _conditions.Find(c => c.ConnectType == ExpressionConnectType.WHERE);
            if (where != null)
            {
                sbQuery.AppendLine(where.ToString());
                _conditions.Remove(where);
                foreach (Condition condition in _conditions)
                    sbQuery.AppendLine(condition.ToString());

                _conditions.Add(where);
            }
        }

        /// <summary>
        /// Возвращает имя столбца таблицы
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns></returns>
        protected string GetColName(Expression<Func<T, object>> member)
        {
            if (!(member.Body is UnaryExpression body))
            {
                if (!(member.Body is MemberExpression mem))
                    throw new Exception(string.Format("member not MemberExpression: {0}", member));
                return mem.Member.Name;
            }
            return ((MemberExpression)body.Operand).Member.Name;
        }

        /// <summary>
        ///Формирует список условий
        /// </summary>
        /// <param name="expression">Условие</param>
        /// <param name="connectType">Тип соединения</param>
        /// <param name="conditions">Список условий</param>
        /// <returns></returns>
        protected string ExpressionToString(Expression expression, ExpressionConnectType connectType, List<Condition> conditions)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Constant:
                    if (expression.Type == typeof(DateTime))
                    {
                        return
                            ((DateTime)((ConstantExpression)expression).Value).ConvertToUnixTimestamp()
                                .ToString(CultureInfo.InvariantCulture);
                    }

                    if (expression.Type == typeof(bool))
                        return ((bool)((ConstantExpression)expression).Value) ? "1" : "0";
                    if (expression.Type == typeof(byte[]))
                    {
                        return string.Concat("'",
                            Convert.ToBase64String((byte[])((ConstantExpression)expression).Value), "'");
                    }

                    if (expression.Type == typeof(string) || expression.Type == typeof(char)
                        || expression.Type == typeof(Guid))
                    {
                        return string.Concat("'", ((ConstantExpression)expression).Value.ToString(), "'");
                    }

                    return ((ConstantExpression)expression).Value.ToString();

                case ExpressionType.Convert:
                    var c = expression as UnaryExpression;
                    if (c != null)
                    {
                        if (c.Operand is BinaryExpression)
                        {
                            var op = c.Operand as BinaryExpression;
                            string str1 = ExpressionToString(op.Left, connectType, conditions);
                            string str2 = ExpressionToString(op.Right, connectType, conditions);
                            var cond = new Condition
                            {
                                Left = str1,
                                Right = str2,
                                Type = op.NodeType,
                                ConnectType = connectType
                            };
                            conditions.Add(cond);
                            return ToString();
                        }
                        throw new Exception(string.Format("Expression is not BinaryExpression: {0}", expression));
                    }
                    throw new Exception(string.Format("Expression is not UnaryExpression: {0}", expression));

                case ExpressionType.Lambda:
                    if (!(expression is LambdaExpression l))
                        throw new Exception(string.Format("Expression is not LambdaExpression: {0}", expression));
                    return ExpressionToString(l.Body, connectType, conditions);

                case ExpressionType.Parameter:
                    return ((ParameterExpression)expression).Name;

                case ExpressionType.MemberAccess:
                    if (!(expression is MemberExpression exp))
                        throw new Exception(string.Format("Expression is not MemberExpression: {0}", expression));
                    if (exp.Expression?.NodeType == ExpressionType.Parameter)
                        return exp.Member.Name;
                    Delegate func = Expression.Lambda(expression).Compile();
                    //object value = func.Method.Invoke(func.Target, null);
                    object value = func.DynamicInvoke();
                    return value.GetSQLValue();

                default:
                    return ((MemberExpression)expression).Member.Name;
            }
        }

        /// <summary>
        /// Возвращает имя таблицы
        /// </summary>
        /// <param name="type">Тип</param>
        /// <returns></returns>
        protected string GetTableName(Type type)
        {
            if (!string.IsNullOrEmpty(_globalTableName))
                return _globalTableName;
            string tableName = type.Name;
            foreach (object attribute in type.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof(TableName))
                {
                    tableName = ((TableName)attribute).Name;
                    break;
                }
            }

            return tableName;
        }

        public void AddCondition(Expression<Func<T, object>> member, ExpressionConnectType connectType,
            string searchPattern)
        {
            string col = GetColName(member);
            var cond = new Condition
            {
                Left = col,
                Right = searchPattern,
                Type = ExpressionType.Quote,
                ConnectType = connectType
            };
            _conditions.Add(cond);
        }

        public void AddCondition(Expression<Func<T, object>> predicate, ExpressionConnectType connectType)
        {
            ExpressionToString(predicate, connectType, _conditions);
        }
    }

    /// <summary>
    /// Класс запроса Delete
    /// </summary>
    /// <typeparam name="T">Тип</typeparam>
    public class SQLDelete<T> : SQLBase<T>
    {
        #region Конструкторы

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLDelete() : base()
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public SQLDelete(string tableName) : base(tableName)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        public SQLDelete(NanoOrmMySQL orm) : base(orm)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        /// <param name="tableName">Имя таблицы</param>
        public SQLDelete(NanoOrmMySQL orm, string tableName) : base(orm, tableName)
        {
        }

        #endregion Конструкторы

        /// <summary>
        /// Производит запрос к БД
        /// </summary>
        public void Exec()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string query = ToString();
            SQLReturn res = _orm.ExecuteNoData(query);
            if (res.HasError)
                throw new Exception(res.Message);
        }

        /// <summary>
        /// Возвращает строку запроса
        /// </summary>
        /// <returns>Строка запроса</returns>
        public override string ToString()
        {
            var sbQuery =
                new StringBuilder(string.Format("DELETE FROM\n `{0}` ", GetTableName(typeof(T))));
            AddWhere(sbQuery);

            return sbQuery.ToString();
        }

        /// <summary>
        /// Добавляет начальное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> Where(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE);
            return this;
        }

        /// <summary>
        /// Добавляет начальное условие к запросу Паттерн поиска: % - The percent sign represents
        /// zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> Where(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> And(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.AND);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> And(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.AND, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> Or(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.OR);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLDelete<T> Or(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.OR, searchPattern);
            return this;
        }
    }



    /// <summary>
    /// Класс запроса Select
    /// </summary>
    /// <typeparam name="T">Тип</typeparam>
    public class SQLSelect<T> : SQLBase<T> where T : class, new()
    {
        private readonly List<OrderTerm> _orderTerms = new List<OrderTerm>();
        private readonly List<string> _groups = new List<string>();

        private readonly List<string> _selects = new List<string>();

        #region Конструкторы

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLSelect() : base()
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public SQLSelect(string tableName) : base(tableName)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        public SQLSelect(NanoOrmMySQL orm) : base(orm)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        /// <param name="tableName">Имя таблицы</param>
        public SQLSelect(NanoOrmMySQL orm, string tableName) : base(orm, tableName)
        {
        }

        #endregion Конструкторы

        /// <summary>
        /// Возвращает первый элемент запроса
        /// </summary>
        /// <returns></returns>
        public T First()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            Limit(1);
            List<T> list = _orm.Query<T>(ToString());
            if (list.Count == 0)
                return null;
            return list[0];
        }

        /// <summary>
        /// Возвращает последний элемент запроса
        /// </summary>
        /// <returns></returns>
        public T Last()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            int count = Count();
            if (count == 0)
                return null;
            Limit(count - 1, 1);
            List<T> list = _orm.Query<T>(ToString());
            if (list.Count == 0)
                return null;
            return list.Last();
        }

        /// <summary>
        /// Возвращает список объектов из БД
        /// </summary>
        /// <returns>Список объектов</returns>
        public List<T> ToList()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            return _orm.Query<T>(ToString());
        }

        /// <summary>
        /// Возвращает массив объектов из БД
        /// </summary>
        /// <returns>Массив объектов</returns>
        public T[] ToArray()
        {
            return ToList().ToArray();
        }

        /// <summary>
        /// Возвращает готовый SQL запрос
        /// </summary>
        /// <returns>SQL запрос</returns>
        public override string ToString()
        {
            var sbQuery =
                new StringBuilder(string.Format("SELECT {1} FROM\n `{0}`\n", GetTableName(typeof(T)), GetSelects()));
            AddWhere(sbQuery);

            return AddOrderGroupLimit(sbQuery);
        }

        /// <summary>
        /// Возвращает кол-во записей в таблице
        /// </summary>
        /// <returns>Кол-во записей</returns>
        public Int32 Count()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            var sbQuery = new StringBuilder(string.Format("SELECT COUNT(*) FROM `{0}` ", GetTableName(typeof(T))));
            AddWhere(sbQuery);
            string query = sbQuery.ToString();
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return Convert.ToInt32(sqlRes.Table.Rows[0].ItemArray[0]);
        }

        /// <summary>
        /// Возвращает кол-во записей в таблице
        /// </summary>
        /// <returns>Кол-во записей</returns>
        public Int64 LongCount()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            var sbQuery = new StringBuilder(string.Format("SELECT COUNT(*) FROM `{0}` ", GetTableName(typeof(T))));
            AddWhere(sbQuery);
            string query = sbQuery.ToString();
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return Convert.ToInt64(sqlRes.Table.Rows[0].ItemArray[0]);
        }

        /// <summary>
        /// Возвращает сумму столбца
        /// </summary>
        /// <param name="member">Столбец</param>
        /// <returns></returns>
        public object Sum(Expression<Func<T, object>> member)
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string col = GetColName(member);
            var sbQuery =
                new StringBuilder(string.Format("SELECT Sum(`{1}`) FROM `{0}` ", GetTableName(typeof(T)), col));
            AddWhere(sbQuery);
            string query = sbQuery.ToString();
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return sqlRes.Table.Rows[0].ItemArray[0];
        }

        /// <summary>
        /// Возвращает минимальное значение аргумента member
        /// </summary>
        /// <param name="member">Аргумент</param>
        /// <returns></returns>
        public object Min(Expression<Func<T, object>> member)
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string col = GetColName(member);
            string query = string.Format("SELECT MIN(`{1}`) FROM `{0}` ", GetTableName(typeof(T)), col);
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return sqlRes.Table.Rows[0].ItemArray[0];
        }

        /// <summary>
        /// Возвращает максмальное значение аргумента member
        /// </summary>
        /// <param name="member">Аргумент</param>
        /// <returns></returns>
        public object Max(Expression<Func<T, object>> member)
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string col = GetColName(member);
            string query = string.Format("SELECT MAX(`{1}`) FROM `{0}` ", GetTableName(typeof(T)), col);
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return sqlRes.Table.Rows[0].ItemArray[0];
        }

        /// <summary>
        /// Возвращает среднее значение аргумента member
        /// </summary>
        /// <param name="member">Аргумент</param>
        /// <returns></returns>
        public object Avg(Expression<Func<T, object>> member)
        {
            if (member == null)
                throw new Exception("member = null");
            if (_orm == null)
                throw new Exception("Not set ORM");
            string query = string.Format("SELECT AVG(*) FROM `{0}` ", GetTableName(typeof(T)));
            SQLReturnData sqlRes = _orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return sqlRes.Table.Rows[0].ItemArray[0];
        }

        /// <summary>
        /// Возвращает заданный элемент
        /// </summary>
        /// <param name="index">Индекс элемента</param>
        /// <returns></returns>
        public T ElementAt(int index)
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            Limit(index, 1);
            List<T> list = _orm.Query<T>(ToString());
            if (list.Count == 0)
                return null;
            return list[0];
        }

        /// <summary>
        /// Возвращает заданное число элементов Аналог Limit
        /// </summary>
        /// <param name="count">Кол-во элементов</param>
        /// <returns></returns>
        public List<T> Take(int count)
        {
            Take(0, count);
            return ToList();
        }

        /// <summary>
        /// Возвращает заданное число элементов Аналог Limit
        /// </summary>
        /// <param name="begin">Индекс первого элемента</param>
        /// <param name="count">Кол-во элементов</param>
        /// <returns></returns>
        public List<T> Take(int begin, int count)
        {
            Limit(begin, count);
            return ToList();
        }

        /// <summary>
        /// Пропускает заданное число элементов
        /// </summary>
        /// <param name="count">Кол-во</param>
        /// <returns></returns>
        public List<T> Skip(int count)
        {
            int c = Count();
            if (c == 0)
                return new List<T>();
            Limit(count, c);
            return ToList();
        }

        /// <summary>
        /// Добавляет начальное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Where(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE);
            return this;
        }

        /// <summary>
        /// Добавляет начальное условие к запросу Паттерн поиска: % - The percent sign represents
        /// zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Where(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> And(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.AND);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> And(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.AND, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Or(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.OR);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Or(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.OR, searchPattern);
            return this;
        }

        /// <summary>
        /// Ограничивает кол-во результатов начиная с первого
        /// </summary>
        /// <param name="count">Кол-во</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Limit(int count)
        {
            _limit = string.Concat(" LIMIT ", count);
            return this;
        }

        /// <summary>
        /// Ограничивает кол-во результатов запроса начиная с begin
        /// </summary>
        /// <param name="begin">Начальный индекс</param>
        /// <param name="count">Кол-во</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Limit(int begin, int count)
        {
            _limit = string.Concat(" LIMIT ", begin, ",", count);
            return this;
        }

        /// <summary>
        /// Добавляет колонки сортировки
        /// </summary>
        /// <param name="members">Массив колонок</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> OrderBy(params Expression<Func<T, object>>[] members)
        {
            foreach (Expression<Func<T, object>> member in members)
                AddOrder(member, OrderType.ASC);
            return this;
        }

        /// <summary>
        /// Добавляет колонку сортировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> OrderBy(Expression<Func<T, object>> member)
        {
            return AddOrder(member, OrderType.ASC);
        }

        /// <summary>
        /// Добавляет колонку сортировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <param name="orderType">Тип сортировки</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> OrderBy(Expression<Func<T, object>> member, OrderType orderType)
        {
            return AddOrder(member, orderType);
        }

        /// <summary>
        /// Добавляет колонку группировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> GroupBy(Expression<Func<T, object>> member)
        {
            _groups.Add(GetColName(member));
            return this;
        }

        /// <summary>
        /// Добавляет колонки группировки
        /// </summary>
        /// <param name="members">Массив колонок</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> GroupBy(params Expression<Func<T, object>>[] members)
        {
            foreach (Expression<Func<T, object>> member in members)
            {
                GroupBy(member);
            }
            return this;
        }

        /// <summary>
        /// Ограничивает выборку только указаными столбцами
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLSelect<T> Select(Expression<Func<T, object>> member)
        {
            _selects.Add(GetColName(member));
            return this;
        }

        /// <summary>
        /// Создает индекс в таблице
        /// </summary>
        /// <param name="member">Условие</param>
        /// <param name="indexName">
        /// Название индекса. Если значение null или пустая строка, то будет создан индекс с именем ИмяТаблицы_ИмяСтолбца
        /// </param>
        public void CreateIndex(Expression<Func<T, object>> member, string indexName)
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string colName = GetColName(member);
            string tableName = (string.IsNullOrEmpty(_globalTableName)) ? GetTableName(typeof(T)) : _globalTableName;
            if (string.IsNullOrEmpty(indexName))
                indexName = string.Concat(tableName, "_", colName);
            _orm.CreateIndex(indexName, colName, tableName);
        }

        #region Private methods

        private string GetSelects()
        {
            if (_selects.Count == 0)
                return "*";
            var sbSelects = new StringBuilder();
            foreach (string column in _selects)
                sbSelects.Append(column).Append(",");
            string res = sbSelects.ToString();
            return res.Remove(res.LastIndexOf(",", StringComparison.Ordinal), 1);
        }

        private SQLSelect<T> AddOrder(Expression<Func<T, object>> member, OrderType type)
        {
            string colName = GetColName(member);
            _orderTerms.Add(new OrderTerm
            {
                Member = colName,
                Type = type
            });
            return this;
        }

        private string AddOrderGroupLimit(StringBuilder sbQuery)
        {
            if (_groups.Count != 0)
            {
                sbQuery.AppendLine(" GROUP BY ");
                foreach (string group in _groups)
                    sbQuery.Append(group).Append(", ");
                sbQuery.Remove(sbQuery.Length - 2, 1);
            }

            if (_orderTerms.Count != 0)
            {
                sbQuery.AppendLine(" ORDER BY ");
                foreach (OrderTerm order in _orderTerms)
                    sbQuery.Append(order).Append(", ");
                sbQuery.Remove(sbQuery.Length - 2, 1);
            }

            sbQuery.AppendLine(_limit);

            return sbQuery.ToString();
        }

        #endregion Private methods
    }

    /// <summary>
    /// Класс запроса Update
    /// </summary>
    /// <typeparam name="T">Тип</typeparam>
    public class SQLUpdate<T> : SQLBase<T> where T : class, new()
    {
        private readonly List<SetCondition> _sets = new List<SetCondition>();

        #region Конструкторы

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLUpdate() : base()
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="tableName">Имя таблицы</param>
        public SQLUpdate(string tableName) : base(tableName)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        public SQLUpdate(NanoOrmMySQL orm) : base(orm)
        {
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        /// <param name="tableName">Имя таблицы</param>
        public SQLUpdate(NanoOrmMySQL orm, string tableName) : base(orm, tableName)
        {
        }

        #endregion Конструкторы

        /// <summary>
        /// Производит запрос к БД
        /// </summary>
        public void Exec()
        {
            if (_orm == null)
                throw new Exception("Not set ORM");
            string query = ToString();
            SQLReturn res = _orm.ExecuteNoData(query);
            if (res.HasError)
                throw new Exception(res.Message);
        }

        /// <summary>
        /// Устанавливает новые значения
        /// </summary>
        /// <param name="member">Свойство</param>
        /// <param name="value">Новое значение</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> Set(Expression<Func<T, object>> member, object value)
        {
            _sets.Add(new SetCondition
            {
                ColName = GetColName(member),
                Value = value.GetSQLValue()
            });
            return this;
        }

        /// <summary>
        /// Возвращает строку запроса
        /// </summary>
        /// <returns>Строка запроса</returns>
        public override string ToString()
        {
            var sbQuery =
                new StringBuilder(string.Format("UPDATE\n`{0}`\n{1}\n", GetTableName(typeof(T)), GetSets()));
            AddWhere(sbQuery);

            return sbQuery.ToString();
        }

        /// <summary>
        /// Добавляет начальное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> Where(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE);
            return this;
        }

        /// <summary>
        /// Добавляет начальное условие к запросу Паттерн поиска: % - The percent sign represents
        /// zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> Where(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.WHERE, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> And(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.AND);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> And(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.AND, searchPattern);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> Or(Expression<Func<T, object>> predicate)
        {
            AddCondition(predicate, ExpressionConnectType.OR);
            return this;
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу Паттерн поиска: % - The percent sign
        /// represents zero, one, or multiple characters _ - The underscore represents a single character
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <param name="searchPattern">Паттерн поиска</param>
        /// <returns>Объект запроса</returns>
        public SQLUpdate<T> Or(Expression<Func<T, object>> predicate, string searchPattern)
        {
            AddCondition(predicate, ExpressionConnectType.OR, searchPattern);
            return this;
        }

        private string GetSets()
        {
            StringBuilder sbQuery = new StringBuilder().AppendLine("SET");
            foreach (SetCondition set in _sets)
                sbQuery.Append(set.ToString()).AppendLine(",");
            if (sbQuery.Length > 2)
                sbQuery.Remove(sbQuery.Length - 2, 1);
            return sbQuery.ToString();
        }
    }

    /// <summary>
    /// Класс условия присваивания
    /// </summary>
    public class SetCondition
    {
        /// <summary>
        /// Имя столбца
        /// </summary>
        public string ColName { get; set; }

        /// <summary>
        /// Значение
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Возвращает в виде готовой строки
        /// </summary>
        public override string ToString() => $"`{ColName}` = {Value}";
    }

    /// <summary>
    /// Класс правила сортировки
    /// </summary>
    public class OrderTerm
    {
        /// <summary>
        /// Колонка
        /// </summary>
        public string Member;

        /// <summary>
        /// Тип сортировки
        /// </summary>
        public OrderType Type;

        /// <summary>
        /// Возвращает строку, представляющую текущий объект.
        /// </summary>
        /// <returns>Строка, представляющая текущий объект.</returns>
        public override string ToString()
        {
            return string.Concat(Member, " ", (Type == OrderType.DESC) ? "DESC" : "ASC");
        }
    }

    /// <summary>
    /// Тип сортировки
    /// </summary>
    public enum OrderType
    {
        ASC, //Туда
        DESC //Обратно
    }

    /// <summary>
    /// Тип условия
    /// </summary>
    public enum ExpressionConnectType
    {
        WHERE,
        AND,
        OR
    }

    /// <summary>
    /// Класс условия
    /// </summary>
    public class Condition
    {
        /// <summary>
        /// Левая часть выражения
        /// </summary>
        public string Left;

        /// <summary>
        /// Выражение
        /// </summary>
        public ExpressionType Type;

        /// <summary>
        /// Правая часть выражения
        /// </summary>
        public string Right;

        /// <summary>
        /// Тип условия
        /// </summary>
        public ExpressionConnectType ConnectType;

        /// <summary>
        /// Возвращает строку, представляющую текущий объект.
        /// </summary>
        /// <returns>Строка, представляющая текущий объект.</returns>
        public override string ToString()
        {
            string ex = GetExpressionTypeString();
            string con = GetConnectString();
            if (Type == ExpressionType.Quote)
                Right = string.Concat("('", Right, "')");
            return string.Format(" {0} {1} {2} {3}", con, Left, ex, Right);
        }

        private string GetExpressionTypeString()
        {
            if (Type == ExpressionType.LessThan)
                return "<";
            else if (Type == ExpressionType.LessThanOrEqual)
                return "<=";
            else if (Type == ExpressionType.NotEqual)
                return "!=";
            else if (Type == ExpressionType.GreaterThan)
                return ">";
            else if (Type == ExpressionType.GreaterThanOrEqual)
                return ">=";
            else if (Type == ExpressionType.Quote)
                return "LIKE";
            return "=";//Default ExpressionType.Equal
        }

        private string GetConnectString()
        {
            if (ConnectType == ExpressionConnectType.AND)
                return "AND\n";
            else if (ConnectType == ExpressionConnectType.OR)
                return "OR\n";
            return "WHERE\n";
        }
    }
}