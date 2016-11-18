/*
	ORM for MySQL
	Powered By Aleksandr Belov 2016
	wernher.pad@gmail.com
	pbsoft.ru
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using MySql.Data.MySqlClient;

// ReSharper disable once CheckNamespace
namespace NanoORM.MySQL
{
    #region ORM

    public class NanoOrm
    {
        /// <summary>
        /// Строка подключения
        /// </summary>
        public string ConnectionString = string.Empty;

        readonly MySqlData MySqlDb = new MySqlData();

        public int Insert(object obj)
        {
            Type t = obj.GetType();
            var fields = t.GetFields();
            string primaryKey = string.Empty;
            string tableName = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;

            var query = BuildInsertQuery(obj, tableName, fields, primaryKey);

            SQLResultData resultData = MySqlDb.SqlReturnDataset(query,
                                                                ConnectionString);
            if (resultData.HasError)
                throw new Exception(resultData.ErrorText);
            Debug.WriteLine(query);
            return int.Parse(resultData.ResultData.Rows[0].ItemArray[0].ToString());
        }

        private static string BuildInsertQuery(object obj, string tableName, IEnumerable<FieldInfo> fields, string primaryKey)
        {
            string query = string.Format(@"INSERT INTO `{0}`(", tableName);
            string query2 = string.Format(@"VALUE(");
            foreach (FieldInfo field in fields)
            {
                if (field.Name == primaryKey)
                    continue;
                query += string.Format(@"`{0}`,", field.Name);
                string tmpl = @"{0},";
                if (field.FieldType == typeof (string))
                    tmpl = @"'{0}',";
                string value = field.GetValue(obj).ToString();
                query2 += string.Format(tmpl, value.Replace(",", "."));
            }
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + ")";
            query2 = query2.Remove(query2.LastIndexOf(",", StringComparison.Ordinal), 1) + ");";
            query += query2 + @"SELECT LAST_INSERT_ID();";
            return query;
        }

        public T Select<T>(int id) where T : new()
        {
            return Select<T>(id, null);
        }

        /// <summary>
        /// Selects the specified identifier.
        /// Гарантированно возвращает не более 1 результата
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The identifier.</param>
        /// <param name="obj"></param>
        /// <returns></returns>
        public T Select<T>(int id, object obj) where T : new()
        {
            Type t = typeof(T);

            string primaryKey = string.Empty;
            string tableName = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            var fields = t.GetFields();

            var query = BuildSelectOneQuery(id, fields, tableName, primaryKey);
            Debug.WriteLine(query);

            return GetDbRow<T>(query, fields, obj);
        }

        private static string BuildSelectOneQuery(int id, IEnumerable<FieldInfo> fields, string tableName, string primaryKey)
        {
            string query = string.Format(@"SELECT ");
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) +
                    string.Format(@" FROM `{0}` WHERE `{0}`.`{1}` = {2} LIMIT 1", tableName, primaryKey, id);
            return query;
        }

        private void getAttributes(Type t, ref string primaryKey, ref string tableName)
        {
            foreach (object attribute in t.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof (PrimaryKey))
                {
                    var atrType = (PrimaryKey) attribute;
                    primaryKey = atrType.Key;
                }
                if (attribute.GetType() == typeof (TableName))
                {
                    var atrType = (TableName) attribute;
                    tableName = atrType.Name;
                }
            }
        }

        public T Select<T>(string where) where T : new()
        {
            return Select<T>(where, null);
        }

        /// <summary>
        /// Selects the specified where.
        /// Гарантированно возвращает не более 1 результата
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where">"WHERE id = 10"</param>
        /// <param name="obj"></param>
        /// <returns></returns>
        public T Select<T>(string where, object obj) where T : new()
        {
            Type t = typeof(T);
            string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            var fields = t.GetFields();

            var query = BuildSelectOneQuery(@where, fields, tableName);
            Debug.WriteLine(query);

            return GetDbRow<T>(query, fields, obj);
        }

        private static string BuildSelectOneQuery(string @where, IEnumerable<FieldInfo> fields, string tableName)
        {
            string query = string.Format(@"SELECT ");
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) +
                    string.Format(@" FROM `{0}` {1} LIMIT 1", tableName, @where);
            return query;
        }

        private T GetDbRow<T>(string query, IEnumerable<FieldInfo> fields, object obj) where T : new()
        {
            SQLResultData result = MySqlDb.SqlReturnDataset(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
            if (obj == null)
                obj = new T();

            if (result.ResultData.Rows.Count == 0)
                return (T) obj;

            DataRow row = result.ResultData.Rows[0];
            foreach (FieldInfo field in fields)
                field.SetValue(obj, row[field.Name]);
            return (T) obj;
        }

        /// <summary>
        /// Возвращает список всех элементов
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="System.Exception"></exception>
        public List<T> SelectAll<T>() where T : new()
        {
            return SelectList<T>(string.Empty);
        }

        /// <summary>
        /// Возвращает список выбранных элементов
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where">The where.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception"></exception>
        public List<T> SelectList<T>(string where) where T : new()
        {
            Type t = typeof(T);
            string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            var fields = t.GetFields();

            var query = BuildSelectListQuery(@where, fields, tableName);
            Debug.WriteLine(query);
            SQLResultData resultData = MySqlDb.SqlReturnDataset(query, ConnectionString);
            if (resultData.HasError)
                throw new Exception(resultData.ErrorText);
            var list = new List<T>();
            foreach (DataRow row in resultData.ResultData.Rows)
            {
                var item = new T();
                foreach (FieldInfo field in fields)
                    field.SetValue(item, row[field.Name]);
                list.Add(item);
            }
            return list;
        }

        private static string BuildSelectListQuery(string @where, IEnumerable<FieldInfo> fields, string tableName)
        {
            string query = string.Format(@"SELECT ");
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) +
                    string.Format(@" FROM `{0}` {1}", tableName, @where);
            return query;
        }

        /// <summary>
        /// Обновляет объек в БД
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <exception cref="System.Exception"></exception>
        public void Update(object obj)
        {
            Type t = obj.GetType();

            string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;

            string query = string.Format(@"UPDATE `{0}` SET ", tableName);
            var fields = t.GetFields();
            var id = 0;
            foreach (FieldInfo field in fields)
            {
                if (field.Name == primaryKey)
                {
                    id = int.Parse(field.GetValue(obj).ToString());
                    continue;
                }
                    
                string tmpl = @"`{0}`.`{1}` = {2},";
                if (field.FieldType == typeof (string))
                    tmpl = @"`{0}`.`{1}` = '{2}',";
                string value = field.GetValue(obj).ToString();
                query += string.Format(tmpl, tableName, field.Name, value.Replace(",","."));
            }

            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + string.Format(@"  WHERE `{0}` = {1}", primaryKey, id);
            Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }

        public void Delete(object obj)
        {
            Type t = obj.GetType();

            string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;

            var fields = t.GetFields();
            var id = 0;
            foreach (FieldInfo field in fields)
            {
                if (field.Name == primaryKey)
                    id = int.Parse(field.GetValue(obj).ToString());
                break;
            }
            string query = string.Format(@"DELETE FROM `{0}` WHERE  `{1}` = {2}",tableName, primaryKey, id);
            Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }

        /// <summary>
        /// Create table if not EXISTS
        /// </summary>
        /// <param name="type">The type.</param>
        /// <exception cref="System.Exception"></exception>
        public void Map(Type type)
        {
            string primaryKey = string.Empty;
            string tableName = string.Empty;
            foreach (object attribute in type.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof(PrimaryKey))
                {
                    var atrType = (PrimaryKey)attribute;
                    primaryKey = atrType.Key;
                }
                if (attribute.GetType() == typeof(TableName))
                {
                    var atrType = (TableName)attribute;
                    tableName = atrType.Name;
                }
            }
            if (tableName == string.Empty)
                tableName = type.Name;
            string query = string.Format(@"CREATE TABLE IF NOT EXISTS `{0}` (", tableName);
            var fields = type.GetFields();
            foreach (FieldInfo field in fields)
            {
                if (field.Name == primaryKey)
                {
                    query += string.Format(@"`{0}` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,", primaryKey);
                    continue;
                }
                string typename = string.Empty;
                if (field.FieldType == typeof(string))
                    typename = "TEXT";
                if (field.FieldType == typeof(double))
                    typename = "DOUBLE";
                if (field.FieldType == typeof(float))
                    typename = "FLOAT";
                if (field.FieldType == typeof(int))
                    typename = "INT(11)";
                if (field.FieldType == typeof(uint))
                    typename = "INT(11) UNSIGNED";
                if (field.FieldType == typeof(sbyte))
                    typename = "TINYINT(4)";
                query += string.Format(@"`{0}` {1} NOT NULL,", field.Name, typename);
            }
            query += string.Format(@"PRIMARY KEY (`{0}`))", primaryKey);
            Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }
    }

    #region Attributes

    public class PrimaryKey : Attribute
    {
        public string Key { get; set; }

        public PrimaryKey() { }

        public PrimaryKey(string fieldName)
        {
            Key = fieldName;
        }
    }

    public class TableName : Attribute
    {
        public string Name { get; set; }

        public TableName()
        {
        }

        public TableName(string name)
        {
            Name = name;
        }
    }

    #endregion Attributes

    #endregion ORM

    #region MysqlQuery

    public class MySqlData
    {
        /// <summary>
        /// Для выполнения запросов к MySQL без возвращения параметров.
        /// </summary>
        /// <param name="sql">Текст запроса к базе данных</param>
        /// <param name="connection">Строка подключения к базе данных</param>
        /// <returns>Возвращает True - ошибка или False - выполнено успешно.</returns>
        public SQLResult SqlNoneQuery(string sql, string connection)
        {
            var result = new SQLResult();
            try
            {
                using (var connRc = new MySqlConnection(connection))
                {
                    using (var commRc = new MySqlCommand(sql, connRc))
                    {
                        connRc.Open();
                        try
                        {
                            commRc.ExecuteNonQuery();
                            result.HasError = false;
                        }
                        catch (Exception ex)
                        {
                            result.ErrorText = String.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, sql);
                            result.HasError = true;
                        }
                    }
                    connRc.Close();
                }
            }
            catch (Exception ex) //Этот эксепшн на случай отсутствия соединения с сервером.
            {
                result.ErrorText = String.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, sql);
                result.HasError = true;
            }
            return result;
        }

        /// <summary>
        /// Выполняет запрос выборки набора строк.
        /// </summary>
        /// <param name="sql">Текст запроса к базе данных</param>
        /// <param name="connection">Строка подключения к базе данных</param>
        /// <returns>Возвращает набор строк в DataSet.</returns>
        public SQLResultData SqlReturnDataset(string sql, string connection)
        {
            var result = new SQLResultData();
            try
            {
                using (var connRc = new MySqlConnection(connection))
                {
                    using (var commRc = new MySqlCommand(sql, connRc))
                    {
                        connRc.Open();

                        try
                        {
                            var adapterP = new MySqlDataAdapter
                            {
                                SelectCommand = commRc
                            };
                            var ds1 = new DataSet();
                            adapterP.Fill(ds1);
                            result.ResultData = ds1.Tables[0];
                        }
                        catch (Exception ex)
                        {
                            result.HasError = true;
                            result.ErrorText = String.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, sql);
                        }
                    }
                    connRc.Close();
                }
            }
            catch (Exception ex) //Этот эксепшн на случай отсутствия соединения с сервером.
            {
                result.ErrorText = String.Format("{0}=>{1}\n[FULL QUERY: {2}]", ex.Message, ex.StackTrace, sql);
                result.HasError = true;
            }
            return result;
        }
    }

    #endregion MysqlQuery

    #region DataResultClasses

    /// <summary>
    /// Класс результат с возвращаемыми данными
    /// </summary>
    /// <seealso cref="SQLResult" />
    [Serializable]
    public class SQLResultData : SQLResult, IResultData
    {
        /// <summary>
        /// Возвращаемые данные
        /// </summary>
        public DataTable ResultData { get; set; }
    }

    /// <summary>
    /// Класс результата запроса без возвращаемых данных
    /// </summary>
    [Serializable]
    public class SQLResult : ISQLResult
    {
        /// <summary>
        /// Флаг ошибки
        /// </summary>
        public bool HasError
        {
            get;
            set;
        }

        /// <summary>
        /// Текст ошибки
        /// </summary>
        public string ErrorText
        {
            get;
            set;
        }
    }

    #endregion DataResultClasses

    #region Interfaces

    /// <summary>
    /// Интерфейс результата
    /// </summary>
    public interface ISQLResult
    {
        /// <summary>
        /// Флаг ошибки
        /// </summary>
        bool HasError { get; set; }

        /// <summary>
        /// Текст ошибки
        /// </summary>
        string ErrorText { get; set; }
    }

    /// <summary>
    /// Интерфейс возращаемых данных
    /// </summary>
    public interface IResultData
    {
        /// <summary>
        /// Возвращаемые данные
        /// </summary>
        DataTable ResultData { get; set; }
    }

    #endregion Interfaces

}