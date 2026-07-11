# libqsqlmysql.so — rebuilt QMYSQL/QMARIADB driver plugin

The copy of `libqsqlmysql.so` that ships with the Qt 6.11.1 SDK at
`/home/linuxhomes/software/qt-dynamic-6.8/` was linked against the real MySQL
client library (`libmysqlclient.so.21`), which isn't installed on this
machine — only MariaDB Connector/C (`libmariadb.so.3`) is, via
`libmariadb-dev`. The SDK's plugin fails to load as a result.

This file is a rebuild of that same plugin from Qt's own `qtbase` source
(`qt.qt6.6111.src` component, installed via the Qt Maintenance Tool), using
the standalone `sqldrivers` CMake project documented at
https://doc.qt.io/qt-6/sql-driver.html, against the properly-installed
`libmariadb-dev`. It links against `libmariadb.so.3` directly — no symlink
tricks, no shared-SDK modifications.

CMakeLists.txt copies this file to `sqldrivers/libqsqlmysql.so` next to both
the `MakeTapeFolders` and `dbmigrate` binaries after every build.
`QCoreApplication::addLibraryPath(applicationDirPath())` (in `main.cpp` and
`tools/dbmigrate_main.cpp`) makes Qt check that directory before the SDK's
own (broken) copy.

To rebuild this file from scratch (e.g. after a Qt version bump):

```
mkdir -p /tmp/sqldrivers-build && cd /tmp/sqldrivers-build
/home/linuxhomes/software/qt-dynamic-6.8/6.11.1/gcc_64/bin/qt-cmake \
    /home/linuxhomes/software/qt-dynamic-6.8/6.11.1/Src/qtbase/src/plugins/sqldrivers
cmake --build . --target QMYSQLDriverPlugin -j$(nproc)
cp plugins/sqldrivers/libqsqlmysql.so <this directory>/
```

Requires the qtbase Sources component (`qt.qt6.6111.src`) to be installed
via the Qt Maintenance Tool first.
