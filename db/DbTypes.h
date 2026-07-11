#ifndef DBTYPES_H
#define DBTYPES_H

#include <QDir>
#include <QFile>
#include <QSettings>
#include <QString>
#include <QByteArray>
#include <QCryptographicHash>
#include <QStandardPaths>

// SHA-256(source_root + 0x1F + src) — the real uniqueness key for the files/done
// tables. Needed because MariaDB/InnoDB cannot index an unbounded TEXT column,
// while `src` is an uncapped file path. Computed identically for both backends
// so the schemas stay logically identical and rows sync 1:1.
inline QByteArray computeSrcHash(const QString& sourceRoot, const QString& src)
{
    QByteArray buf = sourceRoot.toUtf8();
    buf.append('\x1F');
    buf.append(src.toUtf8());
    return QCryptographicHash::hash(buf, QCryptographicHash::Sha256);
}

struct SourceStats {
    qint64 totalFiles = 0;
    qint64 totalSize  = 0;
    qint64 doneFiles  = 0;
    qint64 doneSize   = 0;
};

struct DoneVerifyItem {
    QString src;
    QString dst;
    qint64  bytes   = 0;
    qint64  gzBytes = 0;
};

struct FolderTotals {
    qint64 gzBytesSum = 0;
    int    count      = 0;
};

// Snapshot of a backend's current state — used by the reconcile UI to show
// "what's actually in this database right now" before syncing.
struct DbState {
    bool    connected     = false; // could open a connection at all
    QString connectError;          // set if connected == false
    bool    schemaExists  = false; // files/done tables present
    qint64  filesCount    = 0;
    qint64  doneCount     = 0;
    qint64  lastScannedAt = 0;     // MAX(files.scanned_at), 0 if empty
};

// MariaDB password is deliberately never written to QSettings (plaintext
// .conf file) — kept out-of-band in the environment, or in the same
// restricted-permission credentials file already used elsewhere
// (~/.config/MakeTapeFolders/mariadb.env, chmod 600). Checked in that order:
// an already-set MTF_MARIADB_PASSWORD wins (respects however the process was
// launched); otherwise the file is read directly so the app doesn't require
// the launching shell to have sourced it first.
inline QString resolveMariaPassword()
{
    QByteArray fromEnv = qgetenv("MTF_MARIADB_PASSWORD");
    if (!fromEnv.isEmpty()) return QString::fromUtf8(fromEnv);

    QString credFile = QDir::homePath() + "/.config/MakeTapeFolders/mariadb.env";
    QFile f(credFile);
    if (!f.open(QIODevice::ReadOnly | QIODevice::Text)) return QString();
    while (!f.atEnd()) {
        QString line = QString::fromUtf8(f.readLine()).trimmed();
        if (line.startsWith("MTF_MARIADB_PASSWORD="))
            return line.mid(QString("MTF_MARIADB_PASSWORD=").length());
    }
    return QString();
}

// Connection parameters for either backend, persisted via QSettings so the
// config dialog can edit them at runtime instead of requiring a rebuild.
// mariaPass is the one exception — never persisted here, see
// resolveMariaPassword() above.
struct ConnectionConfig {
    QString sqlitePath  = "/home/data/raw4/Tape/session.db";
    QString mariaHost   = "192.168.1.1";
    int     mariaPort   = 3306;
    QString mariaDb     = "maketapefolders";
    QString mariaUser   = "maketapefolders";
    QString mariaPass;

    static ConnectionConfig load() {
        QSettings s("MakeTapeFolders", "MakeTapeFolders");
        ConnectionConfig c;
        c.sqlitePath = s.value("db/sqlitePath", c.sqlitePath).toString();
        c.mariaHost  = s.value("db/mariaHost", c.mariaHost).toString();
        c.mariaPort  = s.value("db/mariaPort", c.mariaPort).toInt();
        c.mariaDb    = s.value("db/mariaDb", c.mariaDb).toString();
        c.mariaUser  = s.value("db/mariaUser", c.mariaUser).toString();
        c.mariaPass  = resolveMariaPassword();
        return c;
    }
    void save() const {
        QSettings s("MakeTapeFolders", "MakeTapeFolders");
        s.setValue("db/sqlitePath", sqlitePath);
        s.setValue("db/mariaHost", mariaHost);
        s.setValue("db/mariaPort", mariaPort);
        s.setValue("db/mariaDb", mariaDb);
        s.setValue("db/mariaUser", mariaUser);
        // mariaPass intentionally not saved — see resolveMariaPassword().
    }
};

struct FileRow {
    QString source_root;
    QString src;
    qint64  size = 0;
    QString ext;
    QString folder;
    qint64  mtime = 0;
    qint64  scanned_at = 0;
};

struct DoneRow {
    QString source_root;
    QString src;
    QString dst;
    qint64  bytes = 0;
    qint64  gz_bytes = 0;
    double  ratio = 0.0;
    qint64  read_ms = 0;
    qint64  compress_ms = 0;
    qint64  write_ms = 0;
};

#endif // DBTYPES_H
