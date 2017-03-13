/*
	ORM for MySQL
	Powered By Aleksandr Belov 2016
	wernher.pad@gmail.com
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Xml.Schema;
using MySql.Data.MySqlClient;

// ReSharper disable once CheckNamespace
namespace NanoORM
{
    #region ORM

    /// <summary>
    /// 
    /// </summary>
    public class NanoOrm
    {
        /// <summary>
        /// Строка подключения
        /// </summary>
        public string ConnectionString = string.Empty;

        /// <summary>
        /// My SQL database
        /// </summary>
        private readonly MySqlData MySqlDb = new MySqlData();

        public ulong Insert(object obj)
        {
            return Insert(obj, string.Empty);
        }

        /// <summary>
        /// Inserts the specified object.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public ulong Insert(object obj, string tableName)
        {
            string query = string.Empty;
            string query2 = string.Empty;
            Type t = obj.GetType();
            var fields = t.GetProperties();
            string primaryKey = string.Empty;
            //tableName = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            query += string.Format(@"INSERT INTO `{0}`(", tableName);
            query2 += string.Format(@"VALUE(");
            foreach (PropertyInfo field in fields)
            {
                if (field.Name == primaryKey)
                    continue;
                query += string.Format(@"`{0}`,", field.Name);
                string tmpl = @"{0},";
                if (field.PropertyType == typeof (string))
                    tmpl = @"'{0}',";
                string value = field.GetValue(obj, null).ToString();
                query2 += string.Format(tmpl, value.Replace(",","."));
            }
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + ")";
            query2 = query2.Remove(query2.LastIndexOf(",", StringComparison.Ordinal),1)+");";
            query += query2 + "SELECT LAST_INSERT_ID();";

            SQLResultData resultData = MySqlDb.SqlReturnDataset(query,
                                                                ConnectionString);
            if (resultData.HasError)
                throw new Exception(resultData.ErrorText);
            //Debug.WriteLine(query);
            return ulong.Parse(resultData.ResultData.Rows[0].ItemArray[0].ToString());
        }

        public T Select<T>(ulong id) where T : new()
        {
            return Select<T>(id, null, string.Empty);
        }

        public T Select<T>(ulong id, string tableName) where T : new()
        {
            return Select<T>(id, null, tableName);
        }

        /// <summary>
        /// Selects the specified identifier.
        /// Гарантированно возвращает не более 1 результата
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The identifier.</param>
        /// <param name="obj"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public T Select<T>(ulong id, object obj, string tableName) where T : new()
        {
            Type t = typeof(T);

            string primaryKey = string.Empty;
            //string tableName = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            string query = string.Format(@"SELECT ");
            var fields = t.GetProperties();
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));

            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal), 1) + string.Format(" FROM `{0}` WHERE {1} = {2} LIMIT 1", tableName, primaryKey, id);
            //Debug.WriteLine(query);

            return GetDbRow<T>(query, fields, obj);
        }

        private void getAttributes(Type t, ref string primaryKey, ref string tableName)
        {
            foreach (object attribute in t.GetCustomAttributes(false))
            {
                if (attribute.GetType() == typeof (PrimaryKey))
                {
                    var atrType = (PrimaryKey) attribute;
                    if (string.IsNullOrEmpty(primaryKey))
                        primaryKey = atrType.Key;
                }
                if (attribute.GetType() == typeof (TableName))
                {
                    var atrType = (TableName) attribute;
                    if (string.IsNullOrEmpty(tableName))
                        tableName = atrType.Name;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="where"></param>
        /// <param name="tableName"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Select<T>(string where, string tableName) where T : new()
        {
            return Select<T>(where, null, tableName);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="where"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Select<T>(string where) where T : new()
        {
            return Select<T>(where, null, string.Empty);
        }

        /// <summary>
        /// Selects the specified where.
        /// Гарантированно возвращает не более 1 результата
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where">"WHERE id = 10"</param>
        /// <param name="obj"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public T Select<T>(string where, object obj, string tableName) where T : new()
        {
            Type t = typeof(T);
            //string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            string query = string.Format(@"SELECT ");
            var fields = t.GetProperties();
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));

            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + string.Format(" FROM `{0}` {1} LIMIT 1", tableName, where);
            //Debug.WriteLine(query);

            return GetDbRow<T>(query, fields, obj);
        }

        private T GetDbRow<T>(string query, IEnumerable<PropertyInfo> fields, object obj) where T : new()
        {
            SQLResultData result = MySqlDb.SqlReturnDataset(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
            if (obj == null)
                obj = new T();

            if (result.ResultData.Rows.Count == 0)
                return (T) obj;

            DataRow row = result.ResultData.Rows[0];
            foreach (PropertyInfo field in fields)
                field.SetValue(obj, row[field.Name], null);
            return (T) obj;
        }

        public List<T> SelectAll<T>()
        {
            return SelectAll<T>(string.Empty);
        }

        /// <summary>
        /// Возвращает список всех элементов
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="System.Exception"></exception>
        public List<T> SelectAll<T>(string tableName) where T : new()
        {
            Type t = typeof(T);
            //string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            string query = string.Format(@"SELECT ");
            var fields = t.GetProperties();
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + string.Format(" FROM `{0}`", tableName);
            //Debug.WriteLine(query);
            SQLResultData resultData = MySqlDb.SqlReturnDataset(query, ConnectionString);
            if (resultData.HasError)
                throw new Exception(resultData.ErrorText);
            var list = new List<T>();
            foreach (DataRow row in resultData.ResultData.Rows)
            {
                var item = new T();
                foreach (PropertyInfo field in fields)
                    field.SetValue(item, row[field.Name], null);
                list.Add(item);
            }
            return list;
        }

        public List<T> SelectList<T>(string where) where T : new()
        {
            return SelectList<T>(where, string.Empty);
        }

        /// <summary>
        /// Возвращает список выбранных элементов
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where">The where.</param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        /// <exception cref="System.Exception"></exception>
        public List<T> SelectList<T>(string where, string tableName) where T : new()
        {
            Type t = typeof(T);
            //string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;
            string query = string.Format(@"SELECT ");
            var fields = t.GetProperties();
            query = fields.Aggregate(query, (current, field) => current + string.Format(@"`{1}`.`{0}`,", field.Name, tableName));
            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + string.Format(" FROM `{0}` {1}", tableName, where);
            //Debug.WriteLine(query);
            SQLResultData resultData = MySqlDb.SqlReturnDataset(query, ConnectionString);
            if (resultData.HasError)
                throw new Exception(resultData.ErrorText);
            var list = new List<T>();
            foreach (DataRow row in resultData.ResultData.Rows)
            {
                var item = new T();
                foreach (PropertyInfo field in fields)
                    field.SetValue(item, row[field.Name], null);
                list.Add(item);
            }
            return list;
        }

        public List<T> Fetch<T>(string sql) where T : new()
        {
            var res = new List<T>();
            var sqlRes = MySqlDb.SqlReturnDataset(sql, ConnectionString);
            if (sqlRes.HasError)
                throw new Exception(sqlRes.ErrorText);
            var type = typeof (T);
            var propertyes = type.GetProperties();
            foreach (DataRow row in sqlRes.ResultData.Rows)
            {
                var item = new T();
                foreach (PropertyInfo property in propertyes)
                {
                    try
                    {
                        property.SetValue(item, row[property.Name], null);
                    }
                    catch 
                    {
                        continue;
                    }
                }
                res.Add(item);
            }
            return res;
        } 

        public void Update(object obj)
        {
            Update(obj, string.Empty);
        }

        /// <summary>
        /// Обновляет объек в БД
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="tableName"></param>
        /// <exception cref="System.Exception"></exception>
        public void Update(object obj, string tableName)
        {
            Type t = obj.GetType();

            //string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;

            string query = string.Format(@"UPDATE `{0}` SET ", tableName);
            var fields = t.GetProperties();
            ulong id = 0;
            foreach (PropertyInfo field in fields)
            {
                if (field.Name == primaryKey)
                {
                    id = ulong.Parse(field.GetValue(obj, null).ToString());
                    continue;
                }
                    
                string tmpl = @"`{0}`.`{1}` = {2},";
                if (field.PropertyType == typeof (string))
                    tmpl = @"`{0}`.`{1}` = '{2}',";
                string value = field.GetValue(obj, null).ToString();
                query += string.Format(tmpl, tableName, field.Name, value.Replace(",","."));
            }

            query = query.Remove(query.LastIndexOf(",", StringComparison.Ordinal),1) + string.Format("  WHERE `{0}` = {1}", primaryKey, id);
            //Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }

        public void Delete(object obj)
        {
            Delete(obj, string.Empty);
        }

        public void Delete(object obj, string tableName)
        {
            Type t = obj.GetType();

            //string tableName = string.Empty;
            string primaryKey = string.Empty;
            getAttributes(t, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = t.Name;

            var fields = t.GetProperties();
            ulong id = 0;
            foreach (PropertyInfo field in fields)
            {
                if (field.Name == primaryKey)
                    id = ulong.Parse(field.GetValue(obj, null).ToString());
                break;
            }
            string query = string.Format(@"DELETE FROM `{0}` WHERE  `{1}` = {2}",tableName, primaryKey, id);
            //Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }

        public void Map(Type type)
        {
            Map(type, string.Empty);
        }

        /// <summary>
        /// Create table if not EXISTS
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="tableName">Table name</param>
        /// <exception cref="System.Exception"></exception>
        public void Map(Type type, string tableName)
        {
            string primaryKey = string.Empty;
            //string tableName = string.Empty;
            getAttributes(type, ref primaryKey, ref tableName);
            if (tableName == string.Empty)
                tableName = type.Name;
            string query = string.Format(@"CREATE TABLE IF NOT EXISTS `{0}` (", tableName);
            var fields = type.GetProperties();
            foreach (PropertyInfo field in fields)
            {
                if (field.Name == primaryKey)
                {
                    query += string.Format(@"`{0}` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,", primaryKey);
                    continue;
                }
                string typename = string.Empty;
                if (field.PropertyType == typeof(string))
                    typename = "TEXT";
                if (field.PropertyType == typeof(long))
                    typename = "BIGINT";
                if (field.PropertyType == typeof(ulong))
                    typename = "BIGINT UNSIGNED";
                if (field.PropertyType == typeof(double))
                    typename = "DOUBLE";
                if (field.PropertyType == typeof(float))
                    typename = "FLOAT";
                if (field.PropertyType == typeof(int))
                    typename = "INT(11)";
                if (field.PropertyType == typeof(uint))
                    typename = "INT(11) UNSIGNED";
                if (field.PropertyType == typeof(sbyte))
                    typename = "TINYINT(4)";
                query += string.Format(@"`{0}` {1} NOT NULL,", field.Name, typename);
            }
            query += string.Format(@"PRIMARY KEY (`{0}`))", primaryKey);
            //Debug.WriteLine(query);
            SQLResult result = MySqlDb.SqlNoneQuery(query, ConnectionString);
            if (result.HasError)
                throw new Exception(result.ErrorText);
        }

        public SQLResult SqlNoneQuery(string sql)
        {
            return MySqlDb.SqlNoneQuery(sql, ConnectionString);
        }

        public SQLResultData SqlReturnDataset(string sql)
        {
            return MySqlDb.SqlReturnDataset(sql, ConnectionString);
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
#if DEBUG
            var diag = new Stopwatch();
            diag.Start();
#endif
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
            Debug.WriteLine(sql);
#if DEBUG
            diag.Stop();
            Debug.WriteLine("Query time: "+diag.ElapsedMilliseconds+" ms.");
            diag.Reset();
#endif
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
#if DEBUG
            var diag = new Stopwatch();
            diag.Start();
#endif
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
            Debug.WriteLine(sql);
#if DEBUG
            diag.Stop();
            Debug.WriteLine("Query time: " + diag.ElapsedMilliseconds + " ms.");
            diag.Reset();
#endif
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

/*
    /// <summary>
    /// Класс запроса к БД через сервер
    /// </summary>
    [Serializable]
    public class SQLQuery
    {
        /// <summary>
        /// Флаг типа запроса
        /// </summary>
        /// <value>
        ///   <c>true</c> Без возращаемых данных <c>false</c> с данными.
        /// </value>
        public bool NoData
        {
            get;
            set;
        }

        /// <summary>
        /// Строка запроса
        /// </summary>
        public string Query
        {
            get;
            set;
        }
    }
*/

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