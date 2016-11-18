# NanoORM.MySQL

Микро ORM для MySQL.

Net Framework 3.5+

Net Compact Framework 3.5

Время выполнения запросов для Casio DT-X8 (624 MHz, 128 Mb, Windows CE):

    //Insert 100 rows
    Insert TestClass: All: 7762 ms  One: ~77 ms

    //Select 100 rows
    Select TestClass: All: 3935 ms One: ~39 ms
    SelectList TestClass: All 164 ms One: ~1 ms Count rows: 99
    SelectAll TestClass: All 1118 ms One: ~1 ms Count rows: 1100
