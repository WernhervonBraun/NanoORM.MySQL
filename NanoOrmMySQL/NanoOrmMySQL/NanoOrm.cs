/*
    ORM for MySQL
    Powered By Aleksandr Belov 2017
    wernher.pad@gmail.com
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Reflection;
using MySql.Data.MySqlClient;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using System.Diagnostics;

namespace NanoOrmMySQL
{
    public class NanoOrm
    {
        /// <summary>
        /// Последний запрос к базе данных
        /// </summary>
        public string LastQuery { get; private set; }

        /// <summary>
        /// СТрока подключения
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connectionString">Строка подключения</param>
        public NanoOrm(string connectionString)
        {
            ConnectionString = connectionString;
        }

        #region ORM

        /// <summary>
        /// Стартует транзакцию
        /// </summary>
        public void BeginTransaction()
        {
            var sqlRes = ExecuteNoData("START TRANSACTION;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Подтверждает транзакицю
        /// </summary>
        public void CommitTransaction()
        {
            var sqlRes = ExecuteNoData("COMMIT;");
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
        }

        /// <summary>
        /// Откатывает транзакцию
        /// </summary>
        public void RollbackTransaction()
        {
            var sqlRes = ExecuteNoData("ROLLBACK;");
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
            return (from DataRow row in sqlRes.Table.Rows select RowToObject<T>(row)).ToList();
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
            return (from DataRow row in sqlRes.Table.Rows select RowToObject<T>(row)).ToList();
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

        public T SelectOne<T>(object id, string tableName) where T : new()
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
            return SelectOne<T>(string.Format("WHERE {0} = {1}", primaryKeyCollumn, id), tableName);
        }

        /// <summary>
        /// Возвращает 1 запись по условию
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="where">Условие</param>
        /// <returns>Объект типа Т</returns>
        public T SelectOne<T>(string where) where T : new()
        {
            var res = Select<T>(where + " LIMIT 1");
            if (res.Count == 0)
                return new T();
            return res[0];
        }

        public T SelectOne<T>(string where, string tableName) where T : new()
        {
            var res = Select<T>(where + " LIMIT 1", tableName);
            if (res.Count == 0)
                return new T();
            return res[0];
        }

        /// <summary>
        /// Возвращает объект запроса для построения запроса подобоно LINQ
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>Список объектов типа Т</returns>
        public SQLQuery<T> Table<T>() where T : class, new()
        {
            return new SQLQuery<T>(this);
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
        /// <param name="tableName"></param>
        /// <returns>Список объектов типа Т</returns>
        public List<T> Select<T>(string where, string tableName) where T : new()
        {
            var type = typeof(T);
            var query = string.Format("SELECT * FROM {0} {1};", tableName, where);
            var sqlRes = ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return (from DataRow row in sqlRes.Table.Rows select RowToObject<T>(row)).ToList();
        }

        public void Delete(object obj)
        {
            if (obj == null)
                return;
            var type = obj.GetType();
            string tableName = GetTableName(type);
            Delete(obj, tableName);
        }

        /// <summary>
        /// Удаляет объект из базы
        /// </summary>
        /// <param name="obj">Объект для удаления</param>
        /// <param name="tableName"></param>
        /// <remarks>У объекта обязательно должен быть указан атрибут PrimaryKey</remarks>
        public void Delete(object obj, string tableName)
        {
            var type = obj.GetType();
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
            //Debug.WriteLine(query);

            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
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
            var query = string.Format("DELETE FROM {0};", tableName);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

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
        /// <param name="tableName"></param>
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

                var tmpl = "`{0}` = {1},";
                if (IsString(property))
                    tmpl = "`{0}` = '{1}',";
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

            //Debug.WriteLine(query);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Обновляет список объектов в базе
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="list">Список объектов</param>
        public void UpdateAll<T>(object list) where T : new()
        {
            BeginTransaction();
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
            string tableName = GetTableName(type);
            Insert(obj, tableName);
        }

        /// <summary>
        /// Сохраняет объект в базе
        /// </summary>
        /// <param name="obj">Объект</param>
        /// <param name="tableName"></param>
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

                query += string.Format("{0},", GetColName(property));
                var tmpl = "{0},";
                var value = property.GetValue(obj, null);
                if (IsString(property))
                    tmpl = "'{0}',";
                if (value == null && IsString(property))
                    value = string.Empty;
                else
                    value = ConvertValue(property, value);
                if (!IsString(property))
                    query2 += string.Format(tmpl, value.ToString().Replace(",", "."));
                else
                    query2 += string.Format(tmpl, value);
            }
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + ")";
            query2 = query2.Remove(query2.LastIndexOf(",", StringComparison.Ordinal), 1) + ");";
            query += query2 + "SELECT LAST_INSERT_ID();";
            var sqlres = ExecuteData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
            foreach (var property in type.GetProperties())
                if (IsPrimaryKey(property))
                {
                    property.SetPropertyValue(sqlres.Table.Rows[0].ItemArray[0], obj);
                    break;
                }
        }

        /// <summary>
        /// Сохраняет список объектов в базе
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <param name="list">Список объектов</param>
        public void InsertAll<T>(object list) where T : new()
        {
            BeginTransaction();
            try
            {
                foreach (var obj in (List<T>)list)
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
        /// СОздает таблицу
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
                var colName = GetColName(property);
                if (IsNoMapAttribute(property))
                    continue;
                bool primaryKey = IsPrimaryKey(property);
                var att = property.GetCustomAttributes(typeof(AutoIncrement), true);
                bool autoIncrement = (att != null && att.Length != 0);

                att = property.GetCustomAttributes(typeof(NotNULL), true);
                bool notNull = (att != null && att.Length != 0);

                att = property.GetCustomAttributes(typeof(Indexed), true);
                if (att != null && att.Length != 0) indexList.Add(colName);

                var colType = GetColType(property);
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
            var query = sb.ToString();
            query = indexList.Aggregate(query, (current, colindex) => current + string.Format("CREATE INDEX  IF NOT EXISTS name_{0} ON {1}({2});", colindex, tableName, colindex));
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
            var query = string.Format("DROP TABLE {0};", tableName);
            //Debug.WriteLine(query);
            var sqlres = ExecuteNoData(query);
            if (sqlres.HasError)
                throw new Exception(sqlres.Message);
        }

        /// <summary>
        /// Возвращает таблицу в виде sql строки для сохранения как бекап
        /// </summary>
        /// <typeparam name="T">Тип</typeparam>
        /// <returns>строка</returns>
        public string Backup<T>() where T : new()
        {
            var tableName = GetTableName(typeof(T));
            return BackupTable(tableName);
        }

        /// <summary>
        /// Возвращает таблицу в виде sql строки для сохранения как бекап
        /// </summary>
        /// <param name="tblName">Название таблицы</param>
        /// <returns>Строка</returns>
        public string BackupTable(string tblName)
        {
            var fileText = new StringBuilder();
            fileText.AppendLine(string.Concat("DROP TABLE IF EXISTS `", tblName, "`;"));
            var query = $"SHOW CREATE TABLE `{tblName}`;";
            var sqlRaw = ExecuteData(query);
            if (sqlRaw.HasError)
                throw new Exception(sqlRaw.Message);
            fileText.AppendLine(sqlRaw.Table.Rows[0].ItemArray[1] + ";");
            fileText.AppendLine($"LOCK TABLES `{tblName}` WRITE;");
            var raw2 = ExecuteData(string.Concat("SELECT * FROM `", tblName, "`"));
            string beginLine = string.Concat("INSERT INTO `", tblName, "`");
            var sb = new StringBuilder();
            int j = 0;

            foreach (DataRow row in raw2.Table.Rows)
            {
                string line = string.Empty;
                if (j == 0)
                {
                    line += string.Concat(beginLine, " VALUES");
                }
                line += " (";
                int colCount = row.ItemArray.Length;
                for (int i = 0; i < colCount; i++)
                {
                    string value = row[i].ToString();
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
                    fileText.AppendLine(sb + ";");
                    sb = new StringBuilder();
                    j = 0;
                    continue;
                }
                line += ",";
                sb.Append(line);
                j++;
            }
            if (sb.Length != 0)
                fileText.AppendLine(sb.ToString(0, sb.Length - 1) + ";");
            fileText.AppendLine("UNLOCK TABLES;");
            return fileText.ToString();
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
            if (property.PropertyType != typeof(DateTime) && property.PropertyType != typeof(bool) && value != null)
                value = value.ToString();
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
                //row[colName].SetPropertyValue(property, res);
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

        private bool IsNoMapAttribute(PropertyInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(NoMap), true);
            return att != null && att.Length != 0;
        }

        private bool IsPrimaryKey(PropertyInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(PrimaryKey), true);
            return att != null && att.Length != 0;
        }

        private bool IsString(PropertyInfo propertyInfo)
        {
            if (propertyInfo.PropertyType == typeof(string))
                return true;
            if (propertyInfo.PropertyType == typeof(Guid))
                return true;
            return false;
        }

        private string GetColName(PropertyInfo propertyInfo)
        {
            var att = propertyInfo.GetCustomAttributes(typeof(CollumnName), true);
            if (att != null && att.Length != 0)
                return ((CollumnName)att[0]).Name;
            return propertyInfo.Name;
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
            LastQuery = query;

#if DEBUG
            Debug.WriteLine(query);
#endif

            var result = new SQLReturn();
            try
            {
                using (var connRc = new MySqlConnection(ConnectionString))
                {
                    using (var commRc = new MySqlCommand(query, connRc))
                    {
                        connRc.Open();
                        try
                        {
                            commRc.ExecuteNonQuery();
                            result.HasError = false;
                        }
                        catch (Exception ex)
                        {
                            result.Message = string.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, query);

                            result.HasError = true;
                        }
                    }
                }
            }
            catch (Exception ex) //Этот эксепшн на случай отсутствия соединения с сервером.
            {
                result.Message = string.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, query);
                result.HasError = true;
            }
            return result;
        }

        /// <summary>
        /// Выполняет запрос в базе с возвращением данных
        /// </summary>
        /// <param name="query">Запрос</param>
        /// <returns>Результат запроса</returns>
        public SQLReturnData ExecuteData(string query)
        {
            LastQuery = query;

#if DEBUG
            Debug.WriteLine(query);
#endif

            var result = new SQLReturnData();
            try
            {
                using (var connRc = new MySqlConnection(ConnectionString))
                {
                    using (var commRc = new MySqlCommand(query, connRc))
                    {
                        connRc.Open();

                        try
                        {
                            var adapterP = new MySqlDataAdapter
                            {
                                SelectCommand = commRc
                            };
                            var ds1 = new DataSet();
                            //result.ResultData = new DataTable();
                            adapterP.Fill(ds1);
                            result.Table = ds1.Tables[0];
                        }
                        catch (Exception ex)
                        {
                            result.HasError = true;
                            result.Message = string.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, query);
                        }
                    }
                }
            }
            catch (Exception ex) //Этот эксепшн на случай отсутствия соединения с сервером.
            {
                result.Message = string.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, query);
                result.HasError = true;
            }
            return result;
        }

        #endregion DB Execute
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
    public class SQLReturnData : SQLReturn, ISQLReturnData
    {
        /// <summary>
        /// Данные запроса
        /// </summary>
        public DataTable Table { get; set; }

        /// <summary>
        /// Имплицирует тип Exception на себя
        /// </summary>
        /// <param name="exp">Объект ошибки</param>
        public SQLReturnData(Exception exp) : base(exp)
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

    #region Singleton

    public sealed class DBHelper : NanoOrm
    {
        private static volatile DBHelper _Instance;
        private static readonly object _SyncRoot = new Object();

        /// <summary>
        /// Имя файла базы данных
        /// </summary>
        public static string CONNECTION_STRING { get; set; } = "Database=testorm;DataSource=10.110.50.113;Uid=root;Password=*****;CharSet=utf8;DefaultCommandTimeout=300;ConnectionTimeout=300;";

        private DBHelper() : base(CONNECTION_STRING)
        {
        }

        /// <summary>
        /// База данных
        /// </summary>
        public static DBHelper DB
        {
            get
            {
                if (_Instance == null)
                {
                    lock (_SyncRoot)
                    {
                        if (_Instance == null)
                            _Instance = new DBHelper();
                    }
                    return _Instance;
                }

                return _Instance;
            }
        }
    }

    #endregion Singleton

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
                property.SetValue(obj, ((double)val).ConvertFromUnixTimestamp(), null);
            else if (property.PropertyType == typeof(bool))
                property.SetValue(obj, val.ToString() == "1", null);
            else if (property.PropertyType == typeof(byte[]))
                property.SetValue(obj, Convert.FromBase64String(val.ToString()), null);
            else if (property.PropertyType == typeof(Guid))
                property.SetValue(obj, new Guid(val.ToString()), null);
            else if (property.PropertyType == typeof(string))
                property.SetValue(obj, val.ToString(), null);
            else
                property.SetValue(obj, Convert.ChangeType(val, property.PropertyType, null), null);
            //property.SetValue(obj, val, null);
        }

        /// <summary>
        /// Конвертирует дату из форматат UnixTime в DateTime
        /// </summary>
        /// <param name="timestamp">Дата в формате UnixTime</param>
        /// <returns>Дата в формате DateTime</returns>
        public static DateTime ConvertFromUnixTimestamp(this double timestamp)
        {
            var origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return origin.AddSeconds(timestamp);
        }

        /// <summary>
        /// Конвертирует дату из форматат DateTime в UnixTime
        /// </summary>
        /// <param name="dateTime">Дата в формате DateTime</param>
        /// <returns>Дата в формате UnixTime</returns>
        public static double ConvertToUnixTimestamp(this DateTime dateTime)
        {
            var origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            var diff = dateTime - origin;
            return Math.Floor(diff.TotalSeconds);
        }
    }

    #endregion Extension

    /// <summary>
    /// Класс запроса
    /// </summary>
    /// <typeparam name="T">Тип</typeparam>
    public class SQLQuery<T> where T : class, new()
    {
        private readonly string _TableName;
        private string _Limit = string.Empty;
        private readonly List<Condition> _Conditions = new List<Condition>();
        private readonly List<OrderTerm> _OrderTerms = new List<OrderTerm>();
        private readonly List<string> _Groups = new List<string>();
        private readonly NanoOrm _Orm;

        /// <summary>
        /// Конструктор
        /// </summary>
        public SQLQuery()
        {
            _TableName = GetTableName(typeof(T));
        }

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="orm">ОРМ</param>
        public SQLQuery(NanoOrm orm)
        {
            _Orm = orm;
            _TableName = GetTableName(typeof(T));
        }

        public T First()
        {
            if (_Orm == null)
                throw new Exception("Not set ORM");
            var list = _Orm.Query<T>(ToString());
            if (list.Count == 0)
                return null;
            return list[0];
        }

        public T Last()
        {
            if (_Orm == null)
                throw new Exception("Not set ORM");
            var list = _Orm.Query<T>(ToString());
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
            if (_Orm == null)
                throw new Exception("Not set ORM");
            return _Orm.Query<T>(ToString());
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
            var sbQuery = new StringBuilder(string.Format("SELECT * FROM `{0}` ", GetTableName(typeof(T))));
            AddWhere(sbQuery);

            return AddOrderGroupLimit(sbQuery);
        }

        /// <summary>
        /// Возвращает кол-во записей в таблице
        /// </summary>
        /// <returns>Кол-во записей</returns>
        public int Count()
        {
            if (_Orm == null)
                throw new Exception("Not set ORM");
            var sbQuery = new StringBuilder(string.Format("SELECT COUNT(*) FROM `{0}` ", GetTableName(typeof(T))));
            AddWhere(sbQuery);
            var query = sbQuery.ToString();
            var sqlRes = _Orm.ExecuteData(query);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.Message);
            return Convert.ToInt32(sqlRes.Table.Rows[0].ItemArray[0]);
        }

        /// <summary>
        /// Добавляет начальное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> Where(Expression<Func<T, bool>> predicate)
        {
            return Add(predicate, ExpressionConnectType.WHERE);
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> And(Expression<Func<T, bool>> predicate)
        {
            return Add(predicate, ExpressionConnectType.AND);
        }

        /// <summary>
        /// Добавляет дополнительное условие к запросу
        /// </summary>
        /// <param name="predicate">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> Or(Expression<Func<T, bool>> predicate)
        {
            return Add(predicate, ExpressionConnectType.OR);
        }

        /// <summary>
        /// Ограничивает кол-во результатов начиная с первого
        /// </summary>
        /// <param name="count">Кол-во</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> Limit(int count)
        {
            _Limit = string.Concat(" LIMIT ", count);
            return this;
        }

        /// <summary>
        /// Ограничивает кол-во результатов запроса начиная с begin
        /// </summary>
        /// <param name="begin">Начальный индекс</param>
        /// <param name="count">Кол-во</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> Limit(int begin, int count)
        {
            _Limit = string.Concat(" LIMIT ", begin, ",", count);
            return this;
        }

        /// <summary>
        /// Добавляет колонки сортировки
        /// </summary>
        /// <param name="members">Массив колонок</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> OrderBy(params Expression<Func<T, object>>[] members)
        {
            foreach (var member in members)
                AddOrder(member, OrderType.ASC);
            return this;
        }

        /// <summary>
        /// Добавляет колонку сортировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> OrderBy(Expression<Func<T, object>> member)
        {
            return AddOrder(member, OrderType.ASC);
        }

        /// <summary>
        /// Добавляет колонку сортировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <param name="orderType">Тип сортировки</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> OrderBy(Expression<Func<T, object>> member, OrderType orderType)
        {
            return AddOrder(member, orderType);
        }

        /// <summary>
        /// Добавляет колонку группировки
        /// </summary>
        /// <param name="member">Условие</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> GroupBy(Expression<Func<T, object>> member)
        {
            var body = member.Body as UnaryExpression;
            if (body == null)
            {
                var mem = member.Body as MemberExpression;
                if (mem == null)
                    throw new Exception(string.Format("member not MemberExpression: {0}", member));
                _Groups.Add(mem.Member.Name);
            }
            else
            {
                _Groups.Add(((MemberExpression)body.Operand).Member.Name);
            }
            return this;
        }

        /// <summary>
        /// Добавляет колонки группировки
        /// </summary>
        /// <param name="members">Массив колонок</param>
        /// <returns>Объект запроса</returns>
        public SQLQuery<T> GroupBy(params Expression<Func<T, object>>[] members)
        {
            foreach (var member in members)
                GroupBy(member);
            return this;
        }

        #region Private methods

        private SQLQuery<T> AddOrder(Expression<Func<T, object>> member, OrderType type)
        {
            var body = member.Body as UnaryExpression;
            if (body == null)
            {
                var mem = member.Body as MemberExpression;
                if (mem == null)
                    throw new Exception(string.Format("member not MemberExpression: {0}", member));
                _OrderTerms.Add(new OrderTerm
                {
                    Member = mem.Member.Name,
                    Type = type
                });
            }
            else
            {
                _OrderTerms.Add(new OrderTerm
                {
                    Member = ((MemberExpression)body.Operand).Member.Name,
                    Type = type
                });
            }
            return this;
        }

        private SQLQuery<T> Add(Expression<Func<T, bool>> predicate, ExpressionConnectType connectType)
        {
            if (predicate.Body is BinaryExpression body)
            {
                var cond = new Condition
                {
                    Left = GetMemberValue(body.Left),
                    Right = GetMemberValue(body.Right),
                    Type = body.NodeType,
                    ConnectType = connectType
                };
                _Conditions.Add(cond);
                return this;
            }
            throw new Exception(string.Format("predicate not BinaryExpression: {0}", predicate));
        }

        private string GetMemberValue(Expression member)
        {
            if (member.NodeType == ExpressionType.MemberAccess)
            {
                try
                {
                    object value = Expression.Lambda(member).Compile().DynamicInvoke();
                    return value.ToString();
                }
                catch
                {
                }
            }

            if (member.NodeType == ExpressionType.Constant)
            {
                if (((ConstantExpression)member).Type == typeof(DateTime))
                    return ((DateTime)((ConstantExpression)member).Value).ConvertToUnixTimestamp().ToString();
                if (((ConstantExpression)member).Type == typeof(bool))
                    return ((bool)((ConstantExpression)member).Value) ? "1" : "0";
                if (((ConstantExpression)member).Type == typeof(byte[]))
                    return Convert.ToBase64String((byte[])((ConstantExpression)member).Value);
                if (((ConstantExpression)member).Type == typeof(string) || ((ConstantExpression)member).Type == typeof(char) || ((ConstantExpression)member).Type == typeof(Guid))
                    return string.Concat("'", ((ConstantExpression)member).Value.ToString(), "'");
                return ((ConstantExpression)member).Value.ToString();
            }

            return ((MemberExpression)member).Member.Name;
        }

        private string GetMemberLeftValue(Expression member)
        {
            if (member.NodeType == ExpressionType.Constant)
            {
                if (((ConstantExpression)member).Type == typeof(DateTime))
                    return ((DateTime)((ConstantExpression)member).Value).ConvertToUnixTimestamp().ToString();
                if (((ConstantExpression)member).Type == typeof(bool))
                    return ((bool)((ConstantExpression)member).Value) ? "1" : "0";
                if (((ConstantExpression)member).Type == typeof(byte[]))
                    return Convert.ToBase64String((byte[])((ConstantExpression)member).Value);
                if (((ConstantExpression)member).Type == typeof(string) || ((ConstantExpression)member).Type == typeof(char) || ((ConstantExpression)member).Type == typeof(Guid))
                    return string.Concat("'", ((ConstantExpression)member).Value.ToString(), "'");
                return ((ConstantExpression)member).Value.ToString();
            }

            return ((MemberExpression)member).Member.Name;
        }

        private string AddOrderGroupLimit(StringBuilder sbQuery)
        {
            if (_Groups.Count != 0)
            {
                sbQuery.AppendLine(" GROUP BY ");
                foreach (var group in _Groups)
                    sbQuery.Append(group).Append(", ");
                sbQuery.Remove(sbQuery.Length - 2, 1);
            }

            if (_OrderTerms.Count != 0)
            {
                sbQuery.AppendLine(" ORDER BY ");
                foreach (var order in _OrderTerms)
                    sbQuery.Append(order.ToString()).Append(", ");
                sbQuery.Remove(sbQuery.Length - 2, 1);
            }

            sbQuery.AppendLine(_Limit);

            return sbQuery.ToString();
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

        private void AddWhere(StringBuilder sbQuery)
        {
            var where = _Conditions.Find(c => c.ConnectType == ExpressionConnectType.WHERE);
            if (where != null)
            {
                sbQuery.AppendLine(where.ToString());
                _Conditions.Remove(where);
                foreach (var condition in _Conditions)
                    sbQuery.AppendLine(condition.ToString());

                _Conditions.Add(where);
            }
        }

        #endregion Private methods
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
        ASC,//Туда
        DESC//Обратно
    }

    /// <summary>
    /// Тип условия
    /// </summary>
    public enum ExpressionConnectType
    {
        WHERE, AND, OR
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
            var ex = string.Empty;
            switch (Type)
            {
                case ExpressionType.Equal:
                    ex = "=";
                    break;

                case ExpressionType.LessThan:
                    ex = "<";
                    break;

                case ExpressionType.LessThanOrEqual:
                    ex = "<=";
                    break;

                case ExpressionType.NotEqual:
                    ex = "!=";
                    break;

                case ExpressionType.GreaterThan:
                    ex = ">";
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    ex = ">=";
                    break;

                default:
                    ex = "=";
                    break;
            }
            var con = string.Empty;
            switch (ConnectType)
            {
                case ExpressionConnectType.AND:
                    con = "AND\n";
                    break;

                case ExpressionConnectType.OR:
                    con = "OR\n";
                    break;

                case ExpressionConnectType.WHERE:
                    con = "WHERE\n";
                    break;
            }
            return string.Format(" {0} {1} {2} {3}", con, Left, ex, Right);
        }
    }
}