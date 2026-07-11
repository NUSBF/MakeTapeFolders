// Standalone CLI tool: runs DbBackend::ensureSchema() against the requested
// backend without needing a GUI/display. Used to apply the one-time
// src_hash schema migration to the production SQLite DB, and to create the
// MariaDB schema, outside of launching the full MakeTapeFolders app.
//
// Usage: dbmigrate <sqlite|mariadb>

#include "../db/DbBackend.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QDebug>
#include <QString>
#include <cstdio>
#include <unistd.h>
#include <limits.h>

// Must run before QCoreApplication is constructed: Qt reads QT_PLUGIN_PATH
// during early static initialization, and QCoreApplication::addLibraryPath()
// called afterward does not take priority over the SDK's own sqldrivers
// plugin for duplicate driver keys (verified empirically). See
// thirdparty/sqldrivers/README.md for why this matters — the SDK's
// QMYSQL/QMARIADB plugin needs libmysqlclient.so.21, which isn't installed
// here (only MariaDB Connector/C is); our rebuilt copy lives next to this
// binary in sqldrivers/.
static void prependExeDirToPluginPath()
{
    char buf[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len <= 0) return;
    buf[len] = '\0';
    QString exe = QString::fromUtf8(buf);
    int slash = exe.lastIndexOf('/');
    if (slash < 0) return;
    QByteArray dir = exe.left(slash).toUtf8();
    QByteArray existing = qgetenv("QT_PLUGIN_PATH");
    qputenv("QT_PLUGIN_PATH", existing.isEmpty() ? dir : dir + ":" + existing);
}

int main(int argc, char* argv[])
{
    prependExeDirToPluginPath();
    QCoreApplication app(argc, argv);

    if (argc != 2 || (QString(argv[1]) != "sqlite" && QString(argv[1]) != "mariadb")) {
        fprintf(stderr, "Usage: %s <sqlite|mariadb>\n", argv[0]);
        return 2;
    }

    DbBackend::Kind kind = (QString(argv[1]) == "sqlite")
        ? DbBackend::Kind::Sqlite
        : DbBackend::Kind::MariaDb;

    DbBackend db(kind);
    QString err;
    if (!db.ensureSchema(&err)) {
        fprintf(stderr, "ensureSchema FAILED: %s\n", qPrintable(err));
        return 1;
    }

    QStringList roots = db.sourceRoots();
    fprintf(stdout, "ensureSchema OK. source_root(s) found: %d\n", roots.size());
    for (const QString& r : roots) fprintf(stdout, "  %s\n", qPrintable(r));

    db.closeThreadConnection();
    return 0;
}
