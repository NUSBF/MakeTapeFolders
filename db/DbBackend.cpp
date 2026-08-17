#include "DbBackend.h"

#include <QDateTime>
#include <QDebug>
#include <QDir>
#include <QElapsedTimer>
#include <QFileInfo>
#include <QMutexLocker>
#include <QSqlError>

DbBackend::DbBackend(Kind kind, ConnectionConfig config)
    : m_kind(kind), m_config(std::move(config)) {}

DbBackend::~DbBackend()
{
    // Best-effort cleanup of whatever connections are still tracked — normal
    // shutdown should already have called closeThreadConnection() from each
    // owning thread, since a connection can only be closed on the thread
    // that opened it.
    QMutexLocker lk(&m_mutex);
    qDeleteAll(m_doneInsertStmt);
    qDeleteAll(m_scanInsertStmt);
    qDeleteAll(m_sourceFileSizeStmt);
    qDeleteAll(m_orphanInsertStmt);
    m_doneInsertStmt.clear();
    m_scanInsertStmt.clear();
    m_sourceFileSizeStmt.clear();
    m_orphanInsertStmt.clear();
}

QString DbBackend::ignoreInsertPrefix() const
{
    return m_kind == Kind::Sqlite ? "INSERT OR IGNORE" : "INSERT IGNORE";
}

QString DbBackend::replaceInsertPrefix() const
{
    return m_kind == Kind::Sqlite ? "INSERT OR REPLACE" : "REPLACE";
}

QString DbBackend::connectionNameForCurrentThread()
{
    QMutexLocker lk(&m_mutex);
    QThread* t = QThread::currentThread();
    auto it = m_connNames.find(t);
    if (it != m_connNames.end()) return it.value();
    QString kindStr = (m_kind == Kind::Sqlite) ? "sqlite" : "mariadb";
    QString name = QString("db_%1_%2_%3")
        .arg(kindStr)
        .arg((quintptr)this, 0, 16)
        .arg((quintptr)t, 0, 16);
    m_connNames.insert(t, name);
    return name;
}

bool DbBackend::openConnection(QSqlDatabase& db, QString* errorOut)
{
    if (m_kind == Kind::Sqlite) {
        QDir().mkpath(QFileInfo(m_config.sqlitePath).path());
        db.setDatabaseName(m_config.sqlitePath);
    } else {
        if (m_config.mariaPass.isEmpty()) {
            if (errorOut) *errorOut = "MariaDB password is not set — refusing to connect "
                "rather than guessing one. Set MTF_MARIADB_PASSWORD in the environment, or "
                "in ~/.config/MakeTapeFolders/mariadb.env (chmod 600).";
            return false;
        }
        db.setHostName(m_config.mariaHost);
        db.setPort(m_config.mariaPort);
        db.setDatabaseName(m_config.mariaDb);
        db.setUserName(m_config.mariaUser);
        db.setPassword(m_config.mariaPass);
    }
    if (!db.open()) {
        if (errorOut) *errorOut = db.lastError().text();
        return false;
    }
    if (m_kind == Kind::Sqlite) {
        QSqlQuery q(db);
        // journal_mode=DELETE (not WAL): WAL creates a -shm file that hangs
        // when the DB path is NFS-mounted.
        q.exec("PRAGMA journal_mode=DELETE");
        q.exec("PRAGMA synchronous=NORMAL");
        q.exec("PRAGMA cache_size=-32768");
        q.exec("PRAGMA temp_store=MEMORY");
    }
    return true;
}

QSqlDatabase DbBackend::connection()
{
    QString name = connectionNameForCurrentThread();
    QSqlDatabase db = QSqlDatabase::contains(name)
        ? QSqlDatabase::database(name, false)
        : QSqlDatabase::addDatabase(m_kind == Kind::Sqlite ? "QSQLITE" : "QMYSQL", name);
    if (!db.isOpen()) {
        QString err;
        if (!openConnection(db, &err))
            qWarning() << "[DB] failed to open connection" << name << ":" << err;
    }
    return db;
}

void DbBackend::closeThreadConnection()
{
    QThread* t = QThread::currentThread();
    QString name;
    {
        QMutexLocker lk(&m_mutex);
        auto it = m_connNames.find(t);
        if (it == m_connNames.end()) return;
        name = it.value();
        m_connNames.erase(it);
        if (auto sit = m_doneInsertStmt.find(t); sit != m_doneInsertStmt.end()) {
            delete sit.value();
            m_doneInsertStmt.erase(sit);
        }
        if (auto sit = m_scanInsertStmt.find(t); sit != m_scanInsertStmt.end()) {
            delete sit.value();
            m_scanInsertStmt.erase(sit);
        }
        if (auto sit = m_sourceFileSizeStmt.find(t); sit != m_sourceFileSizeStmt.end()) {
            delete sit.value();
            m_sourceFileSizeStmt.erase(sit);
        }
        if (auto sit = m_orphanInsertStmt.find(t); sit != m_orphanInsertStmt.end()) {
            delete sit.value();
            m_orphanInsertStmt.erase(sit);
        }
    }
    {
        QSqlDatabase db = QSqlDatabase::database(name, false);
        if (db.isOpen()) db.close();
    }
    QSqlDatabase::removeDatabase(name);
}

QSqlQuery* DbBackend::cachedStmt(QMap<QThread*, QSqlQuery*>& cache, const QString& sql)
{
    QThread* t = QThread::currentThread();
    QSqlQuery* q = nullptr;
    {
        QMutexLocker lk(&m_mutex);
        q = cache.value(t, nullptr);
    }
    if (!q) {
        QSqlDatabase db = connection();
        q = new QSqlQuery(db);
        q->prepare(sql);
        QMutexLocker lk(&m_mutex);
        cache.insert(t, q);
    }
    return q;
}

void DbBackend::interrupt()
{
    // No portable, driver-agnostic way to cancel an in-flight query through
    // QtSql without linking each backend's native client library directly
    // (which we deliberately don't, so the QSQLITE/QMYSQL plugins can own
    // their own bundled copies). This is a no-op; the pipeline's
    // stopRequested checks in the reader/compress/writer loops are the real
    // cancellation mechanism and remain sufficient in practice, since no
    // single query here blocks for long.
    Q_UNUSED(m_kind);
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

void DbBackend::createSchemaSqlite(QSqlDatabase& db)
{
    QSqlQuery q(db);
    q.exec(
        "CREATE TABLE IF NOT EXISTS files ("
        " source_root TEXT NOT NULL, src TEXT NOT NULL, src_hash BLOB NOT NULL,"
        " size INTEGER, ext TEXT, folder TEXT, mtime INTEGER, scanned_at INTEGER,"
        " PRIMARY KEY (source_root, src_hash))");
    q.exec("CREATE INDEX IF NOT EXISTS idx_files_folder ON files(source_root, folder)");
    // Snapshot of files rows taken right before a re-scan deletes and
    // replaces them — no primary key, since the same (source_root,
    // src_hash) legitimately reappears across multiple archived
    // generations over time.
    q.exec(
        "CREATE TABLE IF NOT EXISTS files_archive ("
        " source_root TEXT NOT NULL, src TEXT NOT NULL, src_hash BLOB NOT NULL,"
        " size INTEGER, ext TEXT, folder TEXT, mtime INTEGER, scanned_at INTEGER,"
        " archived_at INTEGER NOT NULL)");
    q.exec("CREATE INDEX IF NOT EXISTS idx_files_archive_source ON files_archive(source_root, archived_at)");
    q.exec(
        "CREATE TABLE IF NOT EXISTS done ("
        " source_root TEXT NOT NULL, src TEXT NOT NULL, src_hash BLOB NOT NULL,"
        " dst TEXT, bytes INTEGER, gz_bytes INTEGER, ratio REAL,"
        " read_ms INTEGER, compress_ms INTEGER, write_ms INTEGER,"
        " PRIMARY KEY (source_root, src_hash))");
    q.exec("CREATE INDEX IF NOT EXISTS idx_done_dst ON done(dst)");
}

void DbBackend::createSchemaMariaDb(QSqlDatabase& db)
{
    QSqlQuery q(db);
    q.exec(
        "CREATE TABLE IF NOT EXISTS files ("
        " source_root VARCHAR(128) NOT NULL, src TEXT NOT NULL, src_hash BINARY(32) NOT NULL,"
        " size BIGINT, ext VARCHAR(32), folder VARCHAR(512), mtime BIGINT, scanned_at BIGINT,"
        " PRIMARY KEY (source_root, src_hash))"
        " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    q.exec(
        "CREATE TABLE IF NOT EXISTS done ("
        " source_root VARCHAR(128) NOT NULL, src TEXT NOT NULL, src_hash BINARY(32) NOT NULL,"
        " dst TEXT, bytes BIGINT, gz_bytes BIGINT, ratio DOUBLE,"
        " read_ms BIGINT, compress_ms BIGINT, write_ms BIGINT,"
        " PRIMARY KEY (source_root, src_hash))"
        " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    // Snapshot of files rows taken right before a re-scan deletes and
    // replaces them — no primary key, since the same (source_root,
    // src_hash) legitimately reappears across multiple archived
    // generations over time.
    q.exec(
        "CREATE TABLE IF NOT EXISTS files_archive ("
        " source_root VARCHAR(128) NOT NULL, src TEXT NOT NULL, src_hash BINARY(32) NOT NULL,"
        " size BIGINT, ext VARCHAR(32), folder VARCHAR(512), mtime BIGINT, scanned_at BIGINT,"
        " archived_at BIGINT NOT NULL)"
        " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    auto indexExists = [&](const QString& table, const QString& index) {
        QSqlQuery chk(db);
        chk.prepare("SELECT 1 FROM information_schema.statistics "
                    "WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?");
        chk.addBindValue(table);
        chk.addBindValue(index);
        return chk.exec() && chk.next();
    };
    if (!indexExists("files", "idx_files_folder"))
        q.exec("CREATE INDEX idx_files_folder ON files(source_root, folder(191))");
    if (!indexExists("done", "idx_done_dst"))
        q.exec("CREATE INDEX idx_done_dst ON done(dst(191))");
    if (!indexExists("files_archive", "idx_files_archive_source"))
        q.exec("CREATE INDEX idx_files_archive_source ON files_archive(source_root, archived_at)");
}

bool DbBackend::migrateSqliteHashKeyIfNeeded(QSqlDatabase& db, QString* errorOut)
{
    // Self-contained existence check (not QSqlDatabase::tables(), whose
    // driver-internal query has been observed to leave a schema-referencing
    // statement open on the SQLITE connection, which then makes the very
    // next ALTER TABLE fail with "database table is locked" — silently,
    // because the original code didn't check exec()'s return value).
    bool filesTableExists = false;
    {
        QSqlQuery chk(db);
        chk.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='files'");
        filesTableExists = chk.next();
        chk.finish();
    }
    if (!filesTableExists) return true; // fresh DB — createSchemaSqlite() makes it directly

    bool hasHash = false;
    {
        QSqlQuery info(db);
        info.exec("PRAGMA table_info(files)");
        while (info.next()) {
            if (info.value(1).toString() == "src_hash") { hasHash = true; break; }
        }
        info.finish(); // release the statement before any DDL below
    }
    if (hasHash) return true; // already migrated

    qDebug() << "[DB MIGRATE] adding src_hash key to files/done (one-time)...";
    QElapsedTimer t; t.start();

    bool ok = true;
    QString err;

    auto run = [&](QSqlQuery& q, const QString& sql) -> bool {
        if (!ok) return false;
        if (!q.exec(sql)) {
            ok = false;
            err = QString("DDL failed: %1 -- SQL: %2").arg(q.lastError().text(), sql);
            qWarning() << "[DB MIGRATE]" << err;
            return false;
        }
        return true;
    };

    auto migrateTable = [&](const QString& table, const QString& createSql,
                             const QStringList& plainCols) {
        if (!ok) return;
        QSqlQuery q(db);
        qDebug() << "[DB MIGRATE]" << table << "— dropping stale" << (table + "_old") << "if present...";
        if (!run(q, QString("DROP TABLE IF EXISTS %1_old").arg(table))) return;
        qDebug() << "[DB MIGRATE]" << table << "— renaming to" << (table + "_old") << "...";
        if (!run(q, QString("ALTER TABLE %1 RENAME TO %1_old").arg(table))) return;
        qDebug() << "[DB MIGRATE]" << table << "— creating new schema...";
        if (!run(q, createSql)) return;
        q.finish();

        QStringList allCols = plainCols;
        allCols.insert(2, "src_hash"); // source_root, src, src_hash, <rest>

        qint64 total = 0;
        {
            QSqlQuery cnt(db);
            if (cnt.exec(QString("SELECT COUNT(*) FROM %1_old").arg(table)) && cnt.next())
                total = cnt.value(0).toLongLong();
            cnt.finish();
        }
        qDebug() << "[DB MIGRATE]" << table << "— copying" << total << "rows...";

        QSqlQuery sel(db);
        sel.setForwardOnly(true);
        if (!sel.exec(QString("SELECT %1 FROM %2_old").arg(plainCols.join(","), table))) {
            ok = false;
            err = QString("SELECT from %1_old failed: %2").arg(table, sel.lastError().text());
            qWarning() << "[DB MIGRATE]" << err;
            return;
        }

        QSqlQuery ins(db);
        ins.prepare(QString("INSERT OR REPLACE INTO %1(%2) VALUES(%3)")
                        .arg(table, allCols.join(","),
                             QStringList(allCols.size(), "?").join(",")));

        int n = 0;
        QElapsedTimer tableTimer; tableTimer.start();
        db.transaction();
        while (sel.next()) {
            QString sourceRoot = sel.value(0).toString();
            QString src        = sel.value(1).toString();
            int bindIdx = 0;
            ins.bindValue(bindIdx++, sourceRoot);
            ins.bindValue(bindIdx++, src);
            ins.bindValue(bindIdx++, computeSrcHash(sourceRoot, src));
            for (int c = 2; c < plainCols.size(); ++c)
                ins.bindValue(bindIdx++, sel.value(c));
            if (!ins.exec()) {
                ok = false;
                err = QString("INSERT into %1 failed: %2").arg(table, ins.lastError().text());
                qWarning() << "[DB MIGRATE]" << err;
                break;
            }
            if (++n % 5000 == 0) {
                db.commit();
                db.transaction();
                double elapsedS = tableTimer.elapsed() / 1000.0;
                double rate = elapsedS > 0 ? n / elapsedS : 0;
                double etaS = rate > 0 ? (total - n) / rate : 0;
                qDebug() << "[DB MIGRATE]" << table << "—" << n << "/" << total
                         << "| " << QString::number(rate, 'f', 0) << "rows/s"
                         << "| elapsed" << QString::number(elapsedS, 'f', 0) << "s"
                         << "| ETA" << QString::number(etaS, 'f', 0) << "s";
            }
        }
        db.commit();
        qDebug() << "[DB MIGRATE]" << table << "— copy loop done, dropping" << (table + "_old") << "...";
        sel.finish();
        ins.finish();

        if (!ok) {
            qWarning() << "[DB MIGRATE]" << table << "ABORTED after" << n
                       << "rows — leaving" << (table + "_old") << "in place for inspection.";
            return;
        }

        run(q, QString("DROP TABLE %1_old").arg(table));
        qDebug() << "[DB MIGRATE]" << table << "migrated," << n << "rows";
    };

    migrateTable("files",
        "CREATE TABLE files (source_root TEXT NOT NULL, src TEXT NOT NULL, src_hash BLOB NOT NULL,"
        " size INTEGER, ext TEXT, folder TEXT, mtime INTEGER, scanned_at INTEGER,"
        " PRIMARY KEY (source_root, src_hash))",
        {"source_root", "src", "size", "ext", "folder", "mtime", "scanned_at"});
    if (ok) {
        QSqlQuery q(db);
        run(q, "CREATE INDEX IF NOT EXISTS idx_files_folder ON files(source_root, folder)");
    }

    if (ok) {
        migrateTable("done",
            "CREATE TABLE done (source_root TEXT NOT NULL, src TEXT NOT NULL, src_hash BLOB NOT NULL,"
            " dst TEXT, bytes INTEGER, gz_bytes INTEGER, ratio REAL,"
            " read_ms INTEGER, compress_ms INTEGER, write_ms INTEGER,"
            " PRIMARY KEY (source_root, src_hash))",
            {"source_root", "src", "dst", "bytes", "gz_bytes", "ratio", "read_ms", "compress_ms", "write_ms"});
    }
    if (ok) {
        QSqlQuery q(db);
        run(q, "CREATE INDEX IF NOT EXISTS idx_done_dst ON done(dst)");
    }

    qDebug() << "[DB MIGRATE]" << (ok ? "done" : "FAILED") << "in" << t.elapsed() << "ms";
    if (!ok && errorOut) *errorOut = err;
    return ok;
}

bool DbBackend::ensureSchema(QString* errorOut)
{
    QSqlDatabase db = connection();
    if (!db.isOpen()) {
        if (errorOut) *errorOut = db.lastError().text();
        return false;
    }
    if (m_kind == Kind::Sqlite) {
        if (!migrateSqliteHashKeyIfNeeded(db, errorOut)) return false;
        createSchemaSqlite(db);
    } else {
        createSchemaMariaDb(db);
    }
    return true;
}

DbState DbBackend::currentState()
{
    DbState state;
    QSqlDatabase db = connection();
    if (!db.isOpen()) {
        state.connected = false;
        state.connectError = db.lastError().text();
        return state;
    }
    state.connected = true;

    // Check schema existence without creating it — ensureSchema() would
    // create tables, defeating the point of an accurate "what's actually
    // there right now" snapshot.
    QSqlQuery chk(db);
    if (m_kind == Kind::Sqlite) {
        chk.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='files'");
    } else {
        chk.prepare("SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = 'files'");
        chk.exec();
    }
    state.schemaExists = chk.next();
    if (!state.schemaExists) return state;

    QSqlQuery q(db);
    if (q.exec("SELECT COUNT(*) FROM files") && q.next())
        state.filesCount = q.value(0).toLongLong();
    if (q.exec("SELECT COUNT(*) FROM done") && q.next())
        state.doneCount = q.value(0).toLongLong();
    if (q.exec("SELECT MAX(scanned_at) FROM files") && q.next())
        state.lastScannedAt = q.value(0).toLongLong();
    return state;
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

void DbBackend::beginTxn()  { connection().transaction(); }
void DbBackend::flushTxn()  { QSqlDatabase db = connection(); db.commit(); db.transaction(); }
void DbBackend::commitTxn() { connection().commit(); }

// ---------------------------------------------------------------------------
// Tab 1: source index
// ---------------------------------------------------------------------------

QStringList DbBackend::sourceRoots()
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.setForwardOnly(true);
    q.exec("SELECT DISTINCT source_root FROM files ORDER BY source_root");
    QStringList roots;
    while (q.next()) roots << q.value(0).toString();
    return roots;
}

SourceStats DbBackend::statsForSource(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    SourceStats s;
    {
        QSqlQuery q(db);
        q.prepare("SELECT COUNT(*), SUM(size) FROM files WHERE source_root = ?");
        q.addBindValue(sourceRoot);
        if (q.exec() && q.next()) {
            s.totalFiles = q.value(0).toLongLong();
            s.totalSize  = q.value(1).toLongLong();
        }
    }
    {
        // Joins on src_hash (indexed on both sides) instead of the old
        // unindexed f.src = d.src comparison.
        QSqlQuery q(db);
        q.prepare("SELECT COUNT(*), SUM(f.size) FROM done d "
                  "JOIN files f ON f.source_root = d.source_root AND f.src_hash = d.src_hash "
                  "WHERE d.source_root = ?");
        q.addBindValue(sourceRoot);
        if (q.exec() && q.next()) {
            s.doneFiles = q.value(0).toLongLong();
            s.doneSize  = q.value(1).toLongLong();
        }
    }
    return s;
}

void DbBackend::archiveScanForSubfolder(const QString& sourceRoot, const QString& relLikePattern)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("INSERT INTO files_archive"
              "(source_root,src,src_hash,size,ext,folder,mtime,scanned_at,archived_at)"
              " SELECT source_root,src,src_hash,size,ext,folder,mtime,scanned_at,?"
              " FROM files WHERE source_root = ? AND src LIKE ?");
    q.addBindValue(QDateTime::currentSecsSinceEpoch());
    q.addBindValue(sourceRoot);
    q.addBindValue(relLikePattern);
    q.exec();
}

void DbBackend::archiveScanForSource(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("INSERT INTO files_archive"
              "(source_root,src,src_hash,size,ext,folder,mtime,scanned_at,archived_at)"
              " SELECT source_root,src,src_hash,size,ext,folder,mtime,scanned_at,?"
              " FROM files WHERE source_root = ?");
    q.addBindValue(QDateTime::currentSecsSinceEpoch());
    q.addBindValue(sourceRoot);
    q.exec();
}

void DbBackend::deleteScanForSubfolder(const QString& sourceRoot, const QString& relLikePattern)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("DELETE FROM files WHERE source_root = ? AND src LIKE ?");
    q.addBindValue(sourceRoot);
    q.addBindValue(relLikePattern);
    q.exec();
}

void DbBackend::deleteScanForSource(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("DELETE FROM files WHERE source_root = ?");
    q.addBindValue(sourceRoot);
    q.exec();
}

void DbBackend::insertScannedFile(const FileRow& row)
{
    QSqlQuery* q = cachedStmt(m_scanInsertStmt,
        QString("%1 INTO files(source_root,src,src_hash,size,ext,folder,mtime,scanned_at)"
                " VALUES(?,?,?,?,?,?,?,?)").arg(replaceInsertPrefix()));
    q->bindValue(0, row.source_root);
    q->bindValue(1, row.src);
    q->bindValue(2, computeSrcHash(row.source_root, row.src));
    q->bindValue(3, row.size);
    q->bindValue(4, row.ext);
    q->bindValue(5, row.folder);
    q->bindValue(6, row.mtime);
    q->bindValue(7, row.scanned_at);
    q->exec();
}

// ---------------------------------------------------------------------------
// Tab 2: backup prep
// ---------------------------------------------------------------------------

qint64 DbBackend::countIndexedFiles(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("SELECT COUNT(*) FROM files WHERE source_root = ?");
    q.addBindValue(sourceRoot);
    if (q.exec() && q.next()) return q.value(0).toLongLong();
    return 0;
}

qint64 DbBackend::countDoneInFolder(const QString& sourceRoot, const QString& folderLikePattern)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("SELECT COUNT(*) FROM done WHERE source_root = ? AND dst LIKE ?");
    q.addBindValue(sourceRoot);
    q.addBindValue(folderLikePattern);
    if (q.exec() && q.next()) return q.value(0).toLongLong();
    return 0;
}

QVector<DoneVerifyItem> DbBackend::loadDoneForVerification(const QString& sourceRoot,
                                                             const QString& folderLikePattern)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.setForwardOnly(true);
    q.prepare("SELECT src, dst, bytes, gz_bytes FROM done WHERE source_root = ? AND dst LIKE ?");
    q.addBindValue(sourceRoot);
    q.addBindValue(folderLikePattern);
    QVector<DoneVerifyItem> out;
    if (q.exec()) {
        while (q.next()) {
            DoneVerifyItem it;
            it.src     = q.value(0).toString();
            it.dst     = q.value(1).toString();
            it.bytes   = q.value(2).toLongLong();
            it.gzBytes = q.value(3).toLongLong();
            out.push_back(it);
        }
    }
    return out;
}

void DbBackend::deleteBadDoneEntries(const QString& sourceRoot, const QVector<QString>& srcList)
{
    if (srcList.isEmpty()) return;
    QSqlDatabase db = connection();
    db.transaction();
    QSqlQuery q(db);
    q.prepare("DELETE FROM done WHERE source_root = ? AND src_hash = ?");
    for (const QString& src : srcList) {
        q.bindValue(0, sourceRoot);
        q.bindValue(1, computeSrcHash(sourceRoot, src));
        q.exec();
    }
    db.commit();
}

QSet<QString> DbBackend::allDstForSource(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.setForwardOnly(true);
    q.prepare("SELECT dst FROM done WHERE source_root = ?");
    q.addBindValue(sourceRoot);
    QSet<QString> out;
    if (q.exec()) while (q.next()) out.insert(q.value(0).toString());
    return out;
}

FolderTotals DbBackend::sumDoneBytesForFolder(const QString& sourceRoot, const QString& folderLikePattern)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare("SELECT SUM(gz_bytes), COUNT(*) FROM done WHERE source_root = ? AND dst LIKE ?");
    q.addBindValue(sourceRoot);
    q.addBindValue(folderLikePattern);
    FolderTotals t;
    if (q.exec() && q.next()) {
        t.gzBytesSum = q.value(0).toLongLong();
        t.count      = q.value(1).toInt();
    }
    return t;
}

bool DbBackend::sourceFileSize(const QString& sourceRoot, const QString& src, qint64* outSize)
{
    // Indexed lookup via src_hash (source_root, src) has no standalone index
    // since only (source_root, src_hash) is the key — this is why the hash
    // is the exact-match path everywhere, and `src`/`dst` stay for LIKE/display.
    // Cached: called once per file during Phase 2 orphan reconciliation, so a
    // fresh prepare() per call is not acceptable (seconds each over NFS).
    QSqlQuery* q = cachedStmt(m_sourceFileSizeStmt,
        "SELECT size FROM files WHERE source_root = ? AND src_hash = ?");
    q->bindValue(0, sourceRoot);
    q->bindValue(1, computeSrcHash(sourceRoot, src));
    if (q->exec() && q->next()) {
        if (outSize) *outSize = q->value(0).toLongLong();
        return true;
    }
    return false;
}

void DbBackend::insertOrphanDone(const QString& sourceRoot, const QString& src, const QString& dst,
                                  qint64 bytes, qint64 gzBytes, double ratio)
{
    // Cached for the same reason as sourceFileSize() above — called once per
    // reconciled orphan file.
    QSqlQuery* q = cachedStmt(m_orphanInsertStmt,
        QString("%1 INTO done(source_root,src,src_hash,dst,bytes,gz_bytes,ratio,read_ms,compress_ms,write_ms)"
                " VALUES(?,?,?,?,?,?,?,0,0,0)").arg(ignoreInsertPrefix()));
    q->bindValue(0, sourceRoot);
    q->bindValue(1, src);
    q->bindValue(2, computeSrcHash(sourceRoot, src));
    q->bindValue(3, dst);
    q->bindValue(4, bytes);
    q->bindValue(5, gzBytes);
    q->bindValue(6, ratio);
    q->exec();
}

// A source directory can legitimately contain both "X" and "X.gz" as two
// separate, real files (confirmed: 105,776 such pairs out of 1,290,623 files
// on one real source root). Both compress/copy to the identical destination
// path ("X.gz"), so backing up both is a collision — the second write
// silently overwrites the first, both get their own `done` row (they're
// different src_hash), and the done-table's row count for a folder ends up
// exceeding its real distinct file count. The gz sibling is already
// compressed and needs no work; the plain sibling is the redundant one, so
// it's excluded here whenever its own ".gz"-suffixed sibling also exists as
// a source file. UNHEX(SHA2(...)) reproduces computeSrcHash() exactly
// (verified against real stored hashes) so this is a src_hash-indexed
// lookup, not a full scan.
static const char* kExcludeGzShadowedSql =
    "AND NOT ("
    "  f.src NOT LIKE '%.gz'"
    "  AND EXISTS ("
    "    SELECT 1 FROM files g"
    "    WHERE g.source_root = f.source_root"
    "      AND g.src_hash = UNHEX(SHA2(CONCAT(f.source_root, CHAR(31), CONCAT(f.src, '.gz')), 256))"
    "  )"
    ") ";

qint64 DbBackend::countRemaining(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare(QString(
              "SELECT COUNT(*) FROM files f "
              "LEFT JOIN done d ON d.source_root = f.source_root AND d.src_hash = f.src_hash "
              "WHERE f.source_root = ? AND d.src_hash IS NULL %1")
              .arg(kExcludeGzShadowedSql));
    q.addBindValue(sourceRoot);
    if (q.exec() && q.next()) return q.value(0).toLongLong();
    return 0;
}

qint64 DbBackend::sumRemainingBytes(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare(QString(
              "SELECT SUM(f.size) FROM files f "
              "LEFT JOIN done d ON d.source_root = f.source_root AND d.src_hash = f.src_hash "
              "WHERE f.source_root = ? AND d.src_hash IS NULL %1")
              .arg(kExcludeGzShadowedSql));
    q.addBindValue(sourceRoot);
    if (q.exec() && q.next()) return q.value(0).toLongLong();
    return 0;
}

DbBackend::RemainingCursor DbBackend::openRemainingCursor(const QString& sourceRoot)
{
    QSqlDatabase db = connection();
    RemainingCursor cur;
    cur.m_query = QSqlQuery(db);
    cur.m_query.setForwardOnly(true);
    cur.m_query.prepare(QString(
        "SELECT f.src, f.size "
        "FROM files f "
        "LEFT JOIN done d ON d.source_root = f.source_root AND d.src_hash = f.src_hash "
        "WHERE f.source_root = ? AND d.src_hash IS NULL %1"
        "ORDER BY f.folder, f.src")
        .arg(kExcludeGzShadowedSql));
    cur.m_query.addBindValue(sourceRoot);
    cur.m_query.exec();
    return cur;
}

bool DbBackend::RemainingCursor::next(QString& src, qint64& size)
{
    if (!m_query.next()) return false;
    src  = m_query.value(0).toString();
    size = m_query.value(1).toLongLong();
    return true;
}

void DbBackend::insertDone(const DoneRow& row)
{
    QSqlQuery* q = cachedStmt(m_doneInsertStmt,
        QString("%1 INTO done"
                "(source_root,src,src_hash,dst,bytes,gz_bytes,ratio,read_ms,compress_ms,write_ms)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)").arg(ignoreInsertPrefix()));
    q->bindValue(0, row.source_root);
    q->bindValue(1, row.src);
    q->bindValue(2, computeSrcHash(row.source_root, row.src));
    q->bindValue(3, row.dst);
    q->bindValue(4, row.bytes);
    q->bindValue(5, row.gz_bytes);
    q->bindValue(6, row.ratio);
    q->bindValue(7, row.read_ms);
    q->bindValue(8, row.compress_ms);
    q->bindValue(9, row.write_ms);
    q->exec();
}

// ---------------------------------------------------------------------------
// Sync support
// ---------------------------------------------------------------------------

QVector<FileRow> DbBackend::fetchFilesPage(const QString& afterSourceRoot, const QByteArray& afterHash, int limit)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.setForwardOnly(true);
    // Also expanded from the row-value form "(source_root, src_hash) > (?, ?)"
    // to this OR-equivalent for portability across both drivers/dialects.
    q.prepare("SELECT source_root, src, size, ext, folder, mtime, scanned_at FROM files "
              "WHERE source_root > ? OR (source_root = ? AND src_hash > ?) "
              "ORDER BY source_root, src_hash LIMIT ?");
    // A default-constructed QString()/QByteArray() (the "start from the
    // beginning" sentinel callers pass) binds as SQL NULL, not empty
    // string/blob — and NULL comparisons are never true, silently matching
    // zero rows. Normalize to non-null empty values here so callers don't
    // need to know about this.
    QString afterRoot = afterSourceRoot.isNull() ? QString("") : afterSourceRoot;
    QByteArray afterH  = afterHash.isNull() ? QByteArray("") : afterHash;
    q.addBindValue(afterRoot);
    q.addBindValue(afterRoot);
    q.addBindValue(afterH);
    q.addBindValue(limit);
    QVector<FileRow> out;
    if (q.exec()) {
        while (q.next()) {
            FileRow r;
            r.source_root = q.value(0).toString();
            r.src         = q.value(1).toString();
            r.size        = q.value(2).toLongLong();
            r.ext         = q.value(3).toString();
            r.folder      = q.value(4).toString();
            r.mtime       = q.value(5).toLongLong();
            r.scanned_at  = q.value(6).toLongLong();
            out.push_back(r);
        }
    } else {
        qWarning() << "[DB SYNC] fetchFilesPage failed:" << q.lastError().text();
    }
    return out;
}

QVector<DoneRow> DbBackend::fetchDonePage(const QString& afterSourceRoot, const QByteArray& afterHash, int limit)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.setForwardOnly(true);
    // See fetchFilesPage() above for both the OR-equivalent rewrite and the
    // null-vs-empty-string binding fix.
    q.prepare("SELECT source_root, src, dst, bytes, gz_bytes, ratio, read_ms, compress_ms, write_ms FROM done "
              "WHERE source_root > ? OR (source_root = ? AND src_hash > ?) "
              "ORDER BY source_root, src_hash LIMIT ?");
    QString afterRoot = afterSourceRoot.isNull() ? QString("") : afterSourceRoot;
    QByteArray afterH  = afterHash.isNull() ? QByteArray("") : afterHash;
    q.addBindValue(afterRoot);
    q.addBindValue(afterRoot);
    q.addBindValue(afterH);
    q.addBindValue(limit);
    QVector<DoneRow> out;
    if (q.exec()) {
        while (q.next()) {
            DoneRow r;
            r.source_root  = q.value(0).toString();
            r.src          = q.value(1).toString();
            r.dst          = q.value(2).toString();
            r.bytes        = q.value(3).toLongLong();
            r.gz_bytes     = q.value(4).toLongLong();
            r.ratio        = q.value(5).toDouble();
            r.read_ms      = q.value(6).toLongLong();
            r.compress_ms  = q.value(7).toLongLong();
            r.write_ms     = q.value(8).toLongLong();
            out.push_back(r);
        }
    } else {
        qWarning() << "[DB SYNC] fetchDonePage failed:" << q.lastError().text();
    }
    return out;
}

void DbBackend::insertFileIgnore(const FileRow& row)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare(QString("%1 INTO files(source_root,src,src_hash,size,ext,folder,mtime,scanned_at)"
                       " VALUES(?,?,?,?,?,?,?,?)").arg(ignoreInsertPrefix()));
    q.addBindValue(row.source_root);
    q.addBindValue(row.src);
    q.addBindValue(computeSrcHash(row.source_root, row.src));
    q.addBindValue(row.size);
    q.addBindValue(row.ext);
    q.addBindValue(row.folder);
    q.addBindValue(row.mtime);
    q.addBindValue(row.scanned_at);
    q.exec();
}

void DbBackend::insertDoneIgnore(const DoneRow& row)
{
    QSqlDatabase db = connection();
    QSqlQuery q(db);
    q.prepare(QString("%1 INTO done"
                       "(source_root,src,src_hash,dst,bytes,gz_bytes,ratio,read_ms,compress_ms,write_ms)"
                       " VALUES(?,?,?,?,?,?,?,?,?,?)").arg(ignoreInsertPrefix()));
    q.addBindValue(row.source_root);
    q.addBindValue(row.src);
    q.addBindValue(computeSrcHash(row.source_root, row.src));
    q.addBindValue(row.dst);
    q.addBindValue(row.bytes);
    q.addBindValue(row.gz_bytes);
    q.addBindValue(row.ratio);
    q.addBindValue(row.read_ms);
    q.addBindValue(row.compress_ms);
    q.addBindValue(row.write_ms);
    q.exec();
}
