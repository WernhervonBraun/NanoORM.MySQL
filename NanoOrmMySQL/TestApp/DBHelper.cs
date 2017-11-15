using System;

namespace NanoORMMySQL
{
    public sealed class DBHelper : NanoOrm
    {
        private static volatile DBHelper _instance;
        private static readonly object SyncRoot = new Object();

        /// <summary>
        /// Имя файла базы данных
        /// </summary>
        public const string CONNECTION_STRING =
            "Database=testorm;DataSource=10.110.50.113;Uid=root;Password=****;CharSet=utf8;DefaultCommandTimeout=300;ConnectionTimeout=300;";

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
                if (_instance == null)
                {
                    lock (SyncRoot)
                    {
                        if (_instance == null)
                            _instance = new DBHelper();
                    }
                    return _instance;
                }

                return _instance;
            }
        }
    }
}