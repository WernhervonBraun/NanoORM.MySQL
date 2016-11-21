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

Примеры
Examples  
 
Тестовый класс:
Test class : 

    [TableName("NameTable")] //Необязательный атрибут. Если не указывать, то будет использоваться таблица имеющая одинаковое имя с классом
    [PrimaryKey("Id")] //[PrimaryKey("Id")] - Обязательно указание поля первичного ключа
    class TestClass
    {
        public uint Id; //Поле PrimaryKey всегда uint
        public string TestData;
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
    int insertId = orm.Insert(testClass);
	
Выборка:
Selection: 

	TestClass testClass = orm.Select<TestClass>(Id);

или
or

	TestClass testClass = orm.Select<TestClass>("WHERE `id` = " + Id);//Возвращает первую строку отвещающую заданным параметрам.

Выборка списка:
Selection list: 

	var list = new List<TestClass>;
	list = orm.SelectList<TestClass>("WHERE `id` < " + Id);

Выборка всего:
Selection all: 

	var list = new List<TestClass>;
	list = orm.SelectAll<TestClass>();

Обновление:
Update:

	TestClass testClass = orm.Select<TestClass>(Id);
	testClass.TestData = "New Data";
	orm.Update(testClass);

Удаление:
Delete:

	TestClass testClass = orm.Select<TestClass>(Id);
	orm.Delete(testClass);
