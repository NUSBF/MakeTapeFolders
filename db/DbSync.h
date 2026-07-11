#ifndef DBSYNC_H
#define DBSYNC_H

#include "DbBackend.h"
#include <atomic>
#include <functional>

// Copies all `files` then `done` rows from one backend to another, in
// keyset-paginated batches, using INSERT-IGNORE semantics on the
// destination so re-running sync is idempotent and never overwrites rows
// that already exist there.
namespace DbSync {

struct Progress {
    qint64 filesCopied = 0;
    qint64 doneCopied  = 0;
};

using ProgressCallback = std::function<void(const Progress&)>;

// Runs synchronously on the calling thread — callers should invoke this via
// QtConcurrent::run (same pattern as scanFuture/backupFuture) to keep the
// GUI thread free.
void syncAll(DbBackend& from, DbBackend& to, const ProgressCallback& onProgress,
             std::atomic<bool>& stopRequested);

} // namespace DbSync

#endif // DBSYNC_H
