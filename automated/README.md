# PDZD docker edyszyn

## Wymagania
docker-compose (instalowany razem z np. Docker For Dekstop)
dane pobrane na dysk, najlepiej w ścieżce bez spacji

## Jak Używać
### Uruchomienie
w .env zmienić wartość `HOST_DATASET_PATH` na lokalną ścieżkę do katalogu zawierającego dane do zaimportowania (UWAGA NA SPACJE!).
Cała zawartość podanego katalogu zostanie zaimportowana do HDFS.

Ścieżki windowsowe trzeba zmienić na pseudounixowe:
`C:\jakis\folder` -> `/c/jakis/folder`

Uruchomić klaster:
```
$ cd ./automated
$ docker-compose up --build -d
$ docker-compose logs -f driver
```

Pierwsze uruchomienie trwa dosyć długo, ponieważ docker musi pobrać wszystkie użyte obrazy.

### Reset środowiska
1. Pełny reset: usunięcie kontenerów i załadowanych danych
    ```
    $ docker-compose down
    $ docker volume rm automated_namenode automated_datanode
    ```
2. Restart po zmianach w driverze:
    ```
    $ docker-compose up -d --build
    $ docker-compose logs -f driver
    ```

## Komponenty

### Driver
Kontener ze skryptem w pythonie który uruchamia kolejne kroki.

Konfiguracja: `driver/src/properties.py`

1. Import danych do HDFS
Import plików z `HOST_DATASET_PATH` do HDFS (do katalogu `/user/DataSources` lub innego podanego w konfiguracji)
2. Konwersja JSON -> CSV
JSONy z HDFSa są konwertowane na csv i umieszczane w `/user/DataSources/CSV`
3. Import do hive
Driver odpala skrypt `driver/hive/01_import_to_hive_all.sql`

Driver próbuje wykrywać, które etapy zostały wykonane i nie powtarza ich.

### Hadoop
* Namenode
  * kontener: namenode
  * web UI: http://localhost:15070
  * adres w sieci dockera: http://namenode:50070
  * volume: namenode
* Hue:
  * kontener: hue
  * web UI: http://localhost:8088/home

Login i hasło do Hue podaje się przy pierwszym logowaniu.
Uwaga: po zalogowaniu hue przekierowuje na http://localhost:8088, i wyświetla błąd - wynika to z tego że autorzy obrazu usunęli z jakiegoś powodu homepage.
http://localhost:8088/home działa już normalnie.


### Hive
* hive-server:
    * adres z dockera: hive-server:10000
    * adres z zewnątrz: localhost:10000
    * web UI (status): http://localhost:10002

Dostęp do CLI:
```
$ docker-compose exec hive-server hive
```

Zapisanie wyników do lokalnego pliku
```
$ docker-compose exec hive-server hive -e '<QUERY>' > ./some.file
```

    