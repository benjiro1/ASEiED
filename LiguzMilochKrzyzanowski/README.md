Nasz projekt polega na obliczaniu średniej temperatury oraz szybkości wiatru na podstawie danych pochodzących ze stacji meteorologicznych w U.S.
Użyliśmy do tego Scali, wykonując na plikach z danymi operacje bazo-danowe SQL, dzięki którym posegregowaliśmy odpowiednio dane i obliczyliśmy 
średnią godzinową (średnia z cogodzinnych danych danego dnia) oraz średnią miesięczna (średnia z codziennych danych danego miesiąca). 
Oprócz zapytań SQL, użyliśmy biblioteki Vegas-Viz do wyświetlenia obliczonej średniej na wykresie. 

W celu zbudowania kodu należy pobrać projekt i zaimportować go do odpowiedniego IDE najlepiej IntelliJ, czy też Netbeans lub Eclipse.
Po zaimportowaniu projektu należy zbudować projekt (zazwyczaj komenda Build Project), podczas tego procesu pobiorą się wszystkie zależności 
dodane w pliku mavena pom.xml. Następnie uruchamiamy plik DataInput znajdujący się w katalogu src/test/scala.