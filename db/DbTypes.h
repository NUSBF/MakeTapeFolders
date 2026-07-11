#ifndef DBTYPES_H
#define DBTYPES_H

#include <QString>
#include <QByteArray>
#include <QCryptographicHash>

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
