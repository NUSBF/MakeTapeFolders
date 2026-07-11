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
#include <QElapsedTimer>
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

    bool isStats = argc == 3 && QString(argv[1]) == "stats";
    bool isBench = argc == 2 && QString(argv[1]) == "bench";
    bool isKind  = argc == 2 && (QString(argv[1]) == "sqlite" || QString(argv[1]) == "mariadb");
    if (!isStats && !isBench && !isKind) {
        fprintf(stderr, "Usage: %s <sqlite|mariadb>\n", argv[0]);
        fprintf(stderr, "       %s stats <sourceRoot>   (diagnostic: time statsForSource() against sqlite)\n", argv[0]);
        fprintf(stderr, "       %s bench                (diagnostic: time sourceFileSize()/insertOrphanDone() in a loop)\n", argv[0]);
        return 2;
    }

    if (isStats) {
        DbBackend db(DbBackend::Kind::Sqlite);
        QString err;
        if (!db.ensureSchema(&err)) {
            fprintf(stderr, "ensureSchema FAILED: %s\n", qPrintable(err));
            return 1;
        }
        QElapsedTimer t; t.start();
        SourceStats s = db.statsForSource(QString::fromUtf8(argv[2]));
        fprintf(stdout, "statsForSource(%s) took %lld ms — total=%lld done=%lld\n",
                argv[2], (long long)t.elapsed(), (long long)s.totalFiles, (long long)s.doneFiles);
        db.closeThreadConnection();
        return 0;
    }

    if (isBench) {
        DbBackend db(DbBackend::Kind::Sqlite);
        QString err;
        if (!db.ensureSchema(&err)) {
            fprintf(stderr, "ensureSchema FAILED: %s\n", qPrintable(err));
            return 1;
        }
        QVector<FileRow> rows = db.fetchFilesPage(QString(), QByteArray(), 2000);
        if (rows.isEmpty()) {
            fprintf(stderr, "no rows to bench against\n");
            return 1;
        }
        QElapsedTimer t; t.start();
        qint64 sz = 0;
        for (const FileRow& r : rows) db.sourceFileSize(r.source_root, r.src, &sz);
        qint64 lookupMs = t.elapsed();
        fprintf(stdout, "sourceFileSize() x %d: %lld ms total, %.3f ms/call\n",
                rows.size(), (long long)lookupMs, lookupMs / (double)rows.size());

        t.restart();
        for (const FileRow& r : rows)
            db.insertOrphanDone(r.source_root, r.src, "bench/dst", r.size, r.size, 1.0);
        qint64 insertMs = t.elapsed();
        fprintf(stdout, "insertOrphanDone() x %d: %lld ms total, %.3f ms/call\n",
                rows.size(), (long long)insertMs, insertMs / (double)rows.size());

        db.closeThreadConnection();
        return 0;
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
