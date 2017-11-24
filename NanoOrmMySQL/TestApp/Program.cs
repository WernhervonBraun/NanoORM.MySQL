using NanoORMMySQL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace TestApp
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            //DBHelper.DB.CreateTable<TestClass1>();
            //DBHelper.DB.CreateTable<TestClass2>();

            DBHelper.DB.CreateTable<TestClass3>();

            #region oldCode

            //#region 1 Insert

            //var c1 = new TestClass1
            //{
            //    BoolProperty = true,
            //    StrProperty = "StrProperty Test Class",
            //    FloatProperty = 0.1F,
            //    DateTimeProperty = DateTime.Now
            //};
            //DBHelper.DB.Insert(c1);
            //foreach (var test in DBHelper.DB.Select<TestClass1>(string.Format("WHERE ID = {0}", c1.ID)))
            //    Console.WriteLine(test);

            //#endregion 1 Insert

            //#region 1 Update

            //c1.StrProperty = "StrProperty Test Class UPDATE";
            //c1.DateTimeProperty = DateTime.Now.AddDays(10);
            //DBHelper.DB.Update(c1);

            //foreach (var test in DBHelper.DB.Select<TestClass1>())
            //    Console.WriteLine(test);

            //#endregion 1 Update

            //var c2 = DBHelper.DB.SelectOne<TestClass1>(c1.ID);
            //Console.WriteLine(c2);

            //#region Mass Insert

            //const int rowCount = 100;
            //var list = new List<TestClass1>();
            //for (int i = 0; i < rowCount; i++)
            //{
            //    var temp = new TestClass1
            //    {
            //        BoolProperty = true,
            //        StrProperty = "StrProperty " + i,
            //        FloatProperty = 0.1F,
            //        DateTimeProperty = DateTime.Now
            //    };
            //    list.Add(temp);
            //}
            //var diag = new Stopwatch();
            //diag.Start();
            //DBHelper.DB.InsertAll<TestClass1>(list);
            //diag.Stop();
            //Console.WriteLine(string.Format("Insert {0}: {1} ms.", rowCount, diag.ElapsedMilliseconds));

            //#endregion Mass Insert

            //#region Mass Update

            //list = DBHelper.DB.Select<TestClass1>();
            //for (int i = 0; i < list.Count; i++)
            //    list[i].IntProperty = i;

            //diag.Reset();
            //diag.Start();
            //DBHelper.DB.UpdateAll<TestClass1>(list);
            //diag.Stop();
            //Console.WriteLine(string.Format("UPDATE {0}: {1} ms.", list.Count, diag.ElapsedMilliseconds));
            //foreach (var test in DBHelper.DB.Select<TestClass1>())
            //    Console.WriteLine(test);

            //#endregion Mass Update

            //Console.WriteLine(string.Format("Count: {0}", DBHelper.DB.Count<TestClass1>()));
            //Console.WriteLine(string.Format("Count: {0}", DBHelper.DB.Count<TestClass1>("WHERE id > 40")));

            //var c3 = new TestClass2
            //{
            //    ID = 30
            //};

            ////var query2 = DBHelper.DB.Table<TestClass1>().Where(c => c.ID > c3.ID).First();
            ////Console.WriteLine("Query 2: {0}", query2);

            //var query3 = DBHelper.DB.Table<TestClass1>().Where(c => c.ID > c3.ID).And(c => c.BoolProperty == true);
            ////Console.WriteLine("Query 3: {0}", query3.ToString());
            //Console.WriteLine(query3.First());

            //var query4 = DBHelper.DB.Table<TestClass1>().Where(c => c.ID > c3.ID).And(c => c.BoolProperty == false);
            ////Console.WriteLine("Query 4: {0}", query4.ToString());
            //Console.WriteLine(query4.Last());

            //var query = DBHelper.DB.Table<TestClass1>().Where(c => c.ID > 30).Sum(c => c.IntProperty);
            //Console.WriteLine("Sum: {0}", query);

            //var elementAt = DBHelper.DB.Table<TestClass1>().ElementAt(30);
            //Console.WriteLine(elementAt);

            //var skip = DBHelper.DB.Table<TestClass1>().Skip(30);
            //Console.WriteLine("Skip: {0}", skip.Count);

            //var queryLike = DBHelper.DB.Table<TestClass1>().Where(c => c.ID, "%1").ToList();
            //Console.WriteLine("queryLike: {0}", queryLike.Count);

            #endregion oldCode

            //DBHelper.DB.DropTable<TestClass1>();
            //DBHelper.DB.DropTable<TestClass2>();
            DBHelper.DB.DropTable<TestClass3>();
            //DBHelper.DB.Dispose();
            Console.ReadLine();
        }
    }

    public class TestClass3
    {
        [PrimaryKey, AutoIncrement]
        public ulong ID { get; set; }

        [NoMap]
        public List<TestClass3Children> List { get; set; }
    }

    public class TestClass3Children
    {
        [PrimaryKey, AutoIncrement]
        public ulong ID { get; set; }

        [Indexed]
        public ulong ParentID { get; set; }

        public string Text { get; set; } = string.Empty;
    }

    public class TestClass1
    {
        [PrimaryKey, AutoIncrement]
        public ulong ID { get; set; }

        public string StrProperty { get; set; }

        [CollumnType("FLOAT")]
        public float FloatProperty { get; set; }

        [NoMap]
        public bool NoMapProperty { get; set; }

        public DateTime DateTimeProperty { get; set; }

        public bool BoolProperty { get; set; }

        [Indexed]
        public int IntProperty { get; set; }

        //public Guid GuidProperty { get; set; }

        public override string ToString()
        {
            return string.Format("ID: {0} STR: {1} Float: {2} DefaultProperty: {3} DateTime: {4} Bool: {5} ", ID, StrProperty, FloatProperty, IntProperty, DateTimeProperty, BoolProperty);
        }
    }

    [TableName("test_table")]
    public class TestClass2
    {
        [PrimaryKey, AutoIncrement]
        public ulong ID { get; set; }

        public string StrProperty { get; set; }

        public float FloatProperty { get; set; }

        [NoMap]
        public bool NoMapProperty { get; set; }

        public DateTime DateTimeProperty { get; set; }

        public bool BoolProperty { get; set; }

        public override string ToString()
        {
            return string.Format("ID: {0} STR: {1} Float: {2} DefaultProperty: {3} DateTime: {4} Bool: {5} ", ID, StrProperty, FloatProperty, DateTimeProperty, BoolProperty);
        }
    }
}