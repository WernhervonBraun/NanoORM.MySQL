# NanoORM.MySQL

Однофайловая Микро ORM для MySQL.

Single-file Micro-ORM for MySQL. 

Net Framework 3.5+

Net Compact Framework 3.5

Время выполнения запросов для ТСД Casio DT-X8 (624 MHz, 128 Mb, Windows CE 6.0):

Query execution time for Handheld Terminal Casio DT-X8 (624 MHz, 128 Mb, Windows CE 6.0): 


    //Insert 100 rows
    Insert TestClass: All: 7762 ms  One: ~77 ms

    //Select 100 rows
    Select TestClass: All: 3935 ms One: ~39 ms
    
    //Select 99 rows to List<>
    SelectList TestClass: All 164 ms One: ~1 ms Count rows: 99
    
    //Select all rows to List<>
    SelectAll TestClass: All 1118 ms One: ~1 ms Count rows: 1100

# Примеры

Examples  
 
Тестовый класс:

Test class : 

    [TableName("NameTable")] //Необязательный атрибут. Если не указывать, то будет использоваться таблица имеющая одинаковое имя с классом
    class TestClass
    {
        [PrimaryKey, AutoIncrement]- Обязательно указание поля первичного ключа
        public ulong ID { get; set; }

        public string StrProperty { get; set; }

        [CollumnType("REAL")] - Принудительное указание типа колонки
        public float FloatProperty { get; set; }

        [NoMap] - Поле с данным флагом не будет использовано в ОРМ
        public bool NoMapProperty { get; set; }

        public DateTime DateTimeProperty { get; set; }

        public bool BoolProperty { get; set; }

        [Indexed] - Дополнительные ключи индексации
        public int IntProperty { get; set; }

        [Indexed]
        public Guid GuidProperty { get; set; }
    }

Настройка подключения:

Configuring the connection:  

    var orm = new NanoOrm
            {
                ConnectionString =
                    "Database=[Database];Data Source=[IP];User Id=[User];Password=[Password];Character Set=utf8;"
            };

Добавление объекта в базу:

Adding an object to the database: 

    TestClass testClass = new TestClass
    {
        TestData = "test string";
    }
    orm.Insert(testClass);
    
Выборка:

Selection: 

    TestClass testClass = orm.SelectOne<TestClass>(Id);

или

or

    TestClass testClass = orm.SelectOne<TestClass>("WHERE `id` = " + Id);//Возвращает первую строку отвещающую заданным параметрам.

Выборка списка:

Selection list: 

    var list = new List<TestClass>;
    list = orm.Select<TestClass>("WHERE `id` < " + Id);

Выборка всего:

Selection all: 

    var list = new List<TestClass>;
    list = orm.Select<TestClass>();

Обновление:

Update:

    TestClass testClass = orm.Select<TestClass>(Id);
    testClass.TestData = "New Data";
    orm.Update(testClass);

Удаление:

Delete:

    TestClass testClass = orm.Select<TestClass>(Id);
    orm.Delete(testClass);


# Linq-подобная выборка
Все Linq-подобные выборки должны идти через метод Table<T>()
foreach (var n in 
    DBHelper.DB.Table<TestClass1>().Where(x => x.ID > 30).And(x=>x.ID < 130).OrderBy(x => x.StrProperty, OrderType.DESC).Limit(1,100).ToList()
    //Будет создан запрос "SELECT * FROM TestClass1 WHERE ID > 30 AND ID < 130 ORDER BY StrProperty LIMIT 1, 100", вызван и результат возвращен в виде списка.
    )
{
    Console.WriteLine(n);
}
