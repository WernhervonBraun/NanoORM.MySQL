/*
    ORM for MySQL
    Powered By Aleksandr Belov 2017
    wernher.pad@gmail.com
*/

using System;

namespace NanoORMMySQL
{
    #region Singleton

    public sealed class DBHelper : NanoOrmMySQL
    {
        private static volatile DBHelper _instance;
        private static readonly object _syncRoot = new Object();

        ///<summary>
        /// Имя файла базы данных 
        ///</summary>
        public const string CONNECTION_STRING = "Database=testorm;DataSource=127.0.0.1;Uid=root;Password=****;CharSet=utf8;DefaultCommandTimeout=300;ConnectionTimeout=300;";

        private DBHelper() : base(CONNECTION_STRING)
        {
        }

        ///<summary>
        /// База данных
        ///</summary>
        public static DBHelper DB
        {
            get
            {
                if (_instance == null)
                {
                    lock (_syncRoot)
                    {
                        if (_instance == null)
                            _instance = new DBHelper();
                    }
                }
                return _instance;
            }
        }
    }

    #endregion Singleton
}