#include "DbSync.h"

namespace DbSync {

namespace {
constexpr int kPageSize = 2000;
}

void syncAll(DbBackend& from, DbBackend& to, const ProgressCallback& onProgress,
             std::atomic<bool>& stopRequested)
{
    Progress progress;

    // ---- files ----
    {
        QString afterRoot;
        QByteArray afterHash;
        to.beginTxn();
        int sinceFlush = 0;
        while (!stopRequested.load()) {
            QVector<FileRow> page = from.fetchFilesPage(afterRoot, afterHash, kPageSize);
            if (page.isEmpty()) break;
            for (const FileRow& row : page) {
                to.insertFileIgnore(row);
                afterRoot = row.source_root;
                afterHash = computeSrcHash(row.source_root, row.src);
                ++progress.filesCopied;
                if (++sinceFlush >= 5000) { to.flushTxn(); sinceFlush = 0; }
            }
            if (onProgress) onProgress(progress);
            if (page.size() < kPageSize) break;
        }
        to.commitTxn();
    }

    if (stopRequested.load()) return;

    // ---- done ----
    {
        QString afterRoot;
        QByteArray afterHash;
        to.beginTxn();
        int sinceFlush = 0;
        while (!stopRequested.load()) {
            QVector<DoneRow> page = from.fetchDonePage(afterRoot, afterHash, kPageSize);
            if (page.isEmpty()) break;
            for (const DoneRow& row : page) {
                to.insertDoneIgnore(row);
                afterRoot = row.source_root;
                afterHash = computeSrcHash(row.source_root, row.src);
                ++progress.doneCopied;
                if (++sinceFlush >= 5000) { to.flushTxn(); sinceFlush = 0; }
            }
            if (onProgress) onProgress(progress);
            if (page.size() < kPageSize) break;
        }
        to.commitTxn();
    }
}

} // namespace DbSync
