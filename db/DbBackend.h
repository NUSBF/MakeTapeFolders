#ifndef DBBACKEND_H
#define DBBACKEND_H

#include "DbTypes.h"
#include <QMap>
#include <QMutex>
#include <QSet>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QString>
#include <QThread>
#include <QVector>

// One shared data-access layer for both SQLite and MariaDB, built on QtSql
// (QSqlDatabase/QSqlQuery). Both drivers are addressed through identical
// QSqlQuery calls; only ensureSchema()'s DDL text, a couple of INSERT-dialect
// keywords, and the connection parameters differ per Kind. QSqlDatabase
// connections are not safe to share across threads, so a connection is
// opened lazily per calling thread and cached (keyed by QThread*) for the
// lifetime of that thread.
class DbBackend
{
public:
    enum class Kind { Sqlite, MariaDb };

    // Uses ConnectionConfig::load() (QSettings-persisted) if no config is
    // given explicitly — pass one explicitly to test/create a connection
    // with different parameters than the currently-saved ones (e.g. from
    // the config dialog) without touching the persisted settings.
    explicit DbBackend(Kind kind, ConnectionConfig config = ConnectionConfig::load());
    ~DbBackend();

    // Snapshot of what's actually in this database right now — connects if
    // needed, checks schema, counts rows. Safe to call even if the schema
    // has never been created (schemaExists will just be false).
    DbState currentState();

    // Opens (or reuses) the connection for the calling thread, creates the
    // schema if missing, and migrates an existing SQLite DB that predates the
    // src_hash key. Safe to call more than once (idempotent). Returns false
    // and fills errorOut on failure.
    bool ensureSchema(QString* errorOut = nullptr);

    // Closes and removes the connection (and any cached prepared statements)
    // owned by the calling thread. Call this explicitly before a short-lived
    // worker QThread (e.g. the backup writer thread) exits, since
    // QSqlDatabase connections must be closed on the thread that opened them.
    void closeThreadConnection();

    // Best-effort cancel of whatever query is currently running on any
    // SQLite connection (mirrors sqlite3_interrupt). No portable equivalent
    // for MariaDB from here, so it's a no-op for that backend — the
    // pipeline's stopRequested checks remain the primary cancellation path.
    void interrupt();

    Kind kind() const { return m_kind; }

    // ---- Tab 1: source index ------------------------------------------------
    QStringList sourceRoots();
    SourceStats statsForSource(const QString& sourceRoot);
    void deleteScanForSubfolder(const QString& sourceRoot, const QString& relLikePattern);
    void deleteScanForSource(const QString& sourceRoot);
    void insertScannedFile(const FileRow& row);   // lazily prepares+caches per thread

    // ---- Tab 2: backup prep --------------------------------------------------
    qint64 countIndexedFiles(const QString& sourceRoot);
    qint64 countDoneInFolder(const QString& sourceRoot, const QString& folderLikePattern);
    QVector<DoneVerifyItem> loadDoneForVerification(const QString& sourceRoot,
                                                     const QString& folderLikePattern);
    void deleteBadDoneEntries(const QString& sourceRoot, const QVector<QString>& srcList);

    QSet<QString> allDstForSource(const QString& sourceRoot);
    FolderTotals sumDoneBytesForFolder(const QString& sourceRoot, const QString& folderLikePattern);
    bool sourceFileSize(const QString& sourceRoot, const QString& src, qint64* outSize);
    void insertOrphanDone(const QString& sourceRoot, const QString& src, const QString& dst,
                           qint64 bytes, qint64 gzBytes, double ratio);

    qint64 countRemaining(const QString& sourceRoot);
    qint64 sumRemainingBytes(const QString& sourceRoot);

    // Generic transaction helpers, reused by the scan loop, orphan
    // reconciliation, and the backup write loop — all follow the same
    // begin-once / periodic-flush / final-commit pattern.
    void beginTxn();
    void flushTxn();   // commit current transaction, begin a new one
    void commitTxn();  // final commit, no new transaction

    // Streaming cursor over remaining files (files with no `done` row yet),
    // ordered by folder then src — mirrors the sequential-read-friendly order
    // the old sqlite3 cursor used.
    class RemainingCursor {
    public:
        bool next(QString& src, qint64& size);
    private:
        QSqlQuery m_query;
        friend class DbBackend;
    };
    RemainingCursor openRemainingCursor(const QString& sourceRoot);

    // Hot path: called once per successfully written file from the writer
    // thread. Uses a prepared statement cached per thread-connection.
    void insertDone(const DoneRow& row);

    // ---- Sync support (used by DbSync, reads/writes raw rows) ---------------
    // Keyset-paginated iteration ordered by (source_root, src_hash) — avoids
    // needing a surrogate id column while still allowing safe resumable
    // batching over multi-million-row tables.
    QVector<FileRow> fetchFilesPage(const QString& afterSourceRoot, const QByteArray& afterHash, int limit);
    QVector<DoneRow> fetchDonePage(const QString& afterSourceRoot, const QByteArray& afterHash, int limit);
    void insertFileIgnore(const FileRow& row);
    void insertDoneIgnore(const DoneRow& row);

private:
    QSqlDatabase connection();
    QString connectionNameForCurrentThread();
    bool openConnection(QSqlDatabase& db, QString* errorOut);
    bool migrateSqliteHashKeyIfNeeded(QSqlDatabase& db, QString* errorOut);
    void createSchemaSqlite(QSqlDatabase& db);
    void createSchemaMariaDb(QSqlDatabase& db);
    QString ignoreInsertPrefix() const;  // "INSERT OR IGNORE" (sqlite) / "INSERT IGNORE" (mariadb)
    QString replaceInsertPrefix() const; // "INSERT OR REPLACE" (sqlite) / "REPLACE" (mariadb)

    // Returns the calling thread's cached prepared statement for `sql`,
    // creating and preparing it on first use. Every hot-path query (called
    // more than once per backup run) must go through this — a fresh
    // QSqlQuery::prepare() per call costs seconds over NFS, whereas a
    // cached statement reused via bindValue()+exec() does not.
    QSqlQuery* cachedStmt(QMap<QThread*, QSqlQuery*>& cache, const QString& sql);

    Kind m_kind;
    ConnectionConfig m_config;
    QMutex m_mutex;
    QMap<QThread*, QString> m_connNames;             // calling thread -> unique QSqlDatabase connection name
    QMap<QThread*, QSqlQuery*> m_doneInsertStmt;     // per-thread prepared INSERT for insertDone()
    QMap<QThread*, QSqlQuery*> m_scanInsertStmt;     // per-thread prepared INSERT for insertScannedFile()
    QMap<QThread*, QSqlQuery*> m_sourceFileSizeStmt; // per-thread prepared SELECT for sourceFileSize()
    QMap<QThread*, QSqlQuery*> m_orphanInsertStmt;   // per-thread prepared INSERT for insertOrphanDone()
};

#endif // DBBACKEND_H
