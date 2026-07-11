# MakeTapeFolders — New Session Handoff

## Project purpose

Archive ~682 TB of cryo-EM NAS data to LTO-8 tapes (12 TB native capacity each).
The Qt6/C++ app reads files from NAS, gzip-compresses them in RAM, writes them into
numbered staging folders (tape_001, tape_002, …) sized to fit one tape each.
Already-.gz files are copied as-is. Everything else is compressed with zlib deflate
(Z_BEST_SPEED, windowBits=15+16 for gzip output).

---

## Paths

- Project: /home/linuxhomes/namlb/Documents/QT/MakeTapeFolders
- Build dir: /home/linuxhomes/namlb/Documents/QT/MakeTapeFolders/build/Desktop_Qt_6_11_1-Debug
- Build command: cd into build dir, run: cmake --build .
- DB: ~/.local/share/MakeTapeFolders/session.db
- Source NAS drives: /home/data/raw1 (2013,2015,2019,2020,2021,2022), /home/data/raw2 (2022,2023), /home/data/raw3 (2023,2024,2025,2026,emily)
- Destination staging: /home/data/raw4/Tape

---

## DB state at handoff

- raw1: 1,452,442 files indexed, 25 files in done table (backup barely started)
- raw2: 1,416,425 files indexed, 0 done
- raw3: 4,693,548 files indexed, 0 done

It is safe to delete the DB and rescan all three sources since almost nothing has been transferred yet:
  rm ~/.local/share/MakeTapeFolders/session.db

Scan dialog starts at /home/data. Each source (raw1, raw2, raw3) must be scanned separately.

---

## DB schema (CURRENT — must be updated per plan below)

CREATE TABLE files (
    source_root TEXT NOT NULL,
    src         TEXT NOT NULL,
    size        INTEGER,
    ext         TEXT,
    folder      TEXT,
    mtime       INTEGER,
    scanned_at  INTEGER,
    PRIMARY KEY (source_root, src));

CREATE INDEX idx_files_folder_src ON files(source_root, folder, src);

CREATE TABLE done (
    source_root TEXT NOT NULL,
    src         TEXT NOT NULL,
    dst         TEXT,
    bytes       INTEGER,
    gz_bytes    INTEGER,
    ratio       REAL,
    PRIMARY KEY (source_root, src));

---

## DB schema (TARGET — add timing columns to done)

CREATE TABLE done (
    source_root TEXT NOT NULL,
    src         TEXT NOT NULL,
    dst         TEXT,
    bytes       INTEGER,
    gz_bytes    INTEGER,
    ratio       REAL,
    read_ms     INTEGER,
    compress_ms INTEGER,
    write_ms    INTEGER,
    PRIMARY KEY (source_root, src));

The prepared statement doneInsertStmt in the constructor must be updated to 9 params:
  INSERT OR IGNORE INTO done(source_root,src,dst,bytes,gz_bytes,ratio,read_ms,compress_ms,write_ms) VALUES(?,?,?,?,?,?,?,?,?)

---

## Current code — what is already working

Tab 1 (Tape Source Index):
- Scan source dirs into DB with source_root column. All three sources go into same DB, kept separate by source_root.
- Combobox populated from: SELECT DISTINCT source_root FROM files ORDER BY source_root
- Stats query per selected source (count, total size, done count, done size)
- 8 preset SQL queries with [SOURCE] placeholder replaced at run time
- Subfolder refresh (re-scans a single subfolder without wiping other source data)
- Scan dialog starts at /home/data

Tab 2 (Tape Backup Preparation):
- Source selected from combobox (same list as Tab 1)
- Destination path persisted in QSettings
- Folder prefix (default "tape") and max folder size (default 12 TB) persisted
- Tape size tracking uses exact tar-on-tape formula: 512 + ((size+511)/512)*512 per file, plus 1024 EOA overhead
- Folder rotation when currentTarEst + nextFileTarBytes > maxFolderSize
- On resume: gzip-verifies ALL done entries (magic bytes + ISIZE check), removes failures from done so they are retried. Uses isValidGzip(path, origSize) for compressed files, isValidGzip(path, -1) for copied .gz files. Deleted bad destination files.
- Folder fill on resume computed from done table SQL, not filesystem scan (instant)
- GUI shows live elapsed timer during verification phase, updates every 500 files
- Only total verification time logged to qDebug at end (not per-file)
- Stop button sets std::atomic<bool> stopRequested

Key helper functions that must NOT be changed:
- static qint64 tarFileBytes(qint64 fileSize) — 512 + ((fileSize+511)/512)*512
- static bool isValidGzip(const QString& path, qint64 expectedOriginalSize = -1)
- static void dbExec(sqlite3* db, const char* sql)
- nextFolderPath() lambda inside runBackup() — derives next folder number from currentFolder name
- All GUI lambdas in runBackup(): setStatus, setCounts, setProgressVal, setProgressMax, setFolderProgress, setTarEst, reenable

SQLite notes:
- journal_mode=DELETE (not WAL — WAL creates -shm file which hangs on NFS)
- PRAGMA synchronous=NORMAL
- All source NAS paths on NFS — do not use QFileInfo::exists() pre-checks, just try to open directly
- doneInsertStmt is prepared in constructor and used only from the writer thread

QSettings keys: source, destination, folderPrefix, maxFolderSize (stored as TB string), activeTab, geometry

---

## What needs to be implemented: multi-threaded compression pipeline

The current runBackup() main loop (lines 1048 to 1267 of mainwindow.cpp) is sequential:
read one file, compress it, write it, next file. CPU (zlib deflate) is the bottleneck.
Replace it with a 3-stage pipeline.

### New includes needed (add to top of mainwindow.cpp)

#include <QMutex>
#include <QThread>
#include <QWaitCondition>
#include <QRegularExpression>
#include <map>

### Shared data structure (define inside runBackup(), before writer thread starts)

struct WriteItem {
    int        seq;
    QString    rel;         // relative src path (for done INSERT)
    QString    gzName;      // rel+".gz" if compressed, rel if alreadyGz
    qint64     srcSize;
    bool       alreadyGz;
    QByteArray gzData;
    bool       ok;
    qint64     read_ms;     // set by reader
    qint64     compress_ms; // set by compression worker
};

QMutex                  writeMutex;
QWaitCondition          writeCV;
QWaitCondition          ramCV;
std::map<int,WriteItem> writeMap;
int                     nextWriteSeq = 0;
std::atomic<qint64>     ramInFlight{0};
std::atomic<bool>       readerDone{false};
std::atomic<qint64>     statReadMs{0}, statCompressMs{0}, statWriteMs{0};
std::atomic<int>        readerStalls{0};
std::atomic<int>        writeMapDepthMax{0};

### Thread count and RAM budget (add after existing init block)

int nThreads = qMax(1, QThread::idealThreadCount() - 1);
QThreadPool compressPool;
compressPool.setMaxThreadCount(nThreads);

qint64 ramLimit = 28LL * 1024 * 1024 * 1024;
QFile mi("/proc/meminfo");
if (mi.open(QIODevice::ReadOnly)) {
    for (QString line; !(line = mi.readLine()).isEmpty(); ) {
        if (line.startsWith("MemAvailable:")) {
            qint64 kb = line.split(QRegularExpression("\\s+")).at(1).toLongLong();
            qint64 safe = kb * 1024 - 2LL*1024*1024*1024;
            ramLimit = qMin(ramLimit, qMax(512LL*1024*1024, safe));
            break;
        }
    }
}
qDebug() << "[BACKUP] threads:" << nThreads
         << "ramLimit:" << QLocale().formattedDataSize(ramLimit,2,QLocale::DataSizeSIFormat);

### Stage 1 — Reader (replaces the old while sqlite3_step loop)

int seq = 0;
dbExec(sessionDb, "BEGIN TRANSACTION");

while (sqlite3_step(rem) == SQLITE_ROW) {
    if (stopRequested.load()) break;

    QString rel    = QString::fromUtf8((const char*)sqlite3_column_text(rem, 0));
    qint64 srcSize = sqlite3_column_int64(rem, 1);
    const QString srcPath = sourceRoot + "/" + rel;
    bool alreadyGz = QFileInfo(rel).suffix().toLower() == "gz";
    QString gzName = rel + (alreadyGz ? "" : ".gz");

    // Block until RAM budget allows loading this file
    {
        QMutexLocker lk(&writeMutex);
        if (ramInFlight.loadAcquire() + srcSize > ramLimit) {
            readerStalls.fetch_add(1);
            while (ramInFlight.loadAcquire() + srcSize > ramLimit && !stopRequested.load())
                ramCV.wait(&writeMutex, 200);
        }
    }
    if (stopRequested.load()) break;

    // Read entire file into RAM
    QElapsedTimer readTimer; readTimer.start();
    QFile inF(srcPath);
    if (!inF.open(QIODevice::ReadOnly)) {
        qDebug() << "[BACKUP] ERROR open:" << srcPath;
        ++seq; continue;
    }
    QByteArray inputData = inF.readAll();
    inF.close();
    qint64 rmx = readTimer.elapsed();
    if (inputData.isEmpty()) { ++seq; continue; }

    ramInFlight.fetchAndAddOrdered(srcSize);

    // Submit compression task
    int capturedSeq = seq;
    QtConcurrent::run(&compressPool, [=, &writeMutex, &writeCV, &writeMap, &writeMapDepthMax,
                                      &statReadMs, &statCompressMs, &stopRequested]() {
        WriteItem w;
        w.seq       = capturedSeq;
        w.rel       = rel;
        w.gzName    = gzName;
        w.srcSize   = srcSize;
        w.alreadyGz = alreadyGz;
        w.read_ms   = rmx;
        w.ok        = false;

        QElapsedTimer ct; ct.start();
        if (alreadyGz) {
            w.gzData = inputData;
            w.ok     = true;
        } else {
            z_stream zs = {};
            if (deflateInit2(&zs, Z_BEST_SPEED, Z_DEFLATED, 15+16, 8, Z_DEFAULT_STRATEGY) == Z_OK) {
                QByteArray out;
                out.reserve(inputData.size() / 2);
                QByteArray outBuf(4 << 20, 0);
                bool zerr = false;
                zs.next_in  = (Bytef*)inputData.constData();
                zs.avail_in = (uInt)inputData.size();
                do {
                    if (stopRequested.load()) { zerr = true; break; }
                    zs.next_out  = (Bytef*)outBuf.data();
                    zs.avail_out = (uInt)outBuf.size();
                    int ret = deflate(&zs, zs.avail_in == 0 ? Z_FINISH : Z_NO_FLUSH);
                    if (ret == Z_STREAM_ERROR) { zerr = true; break; }
                    out.append(outBuf.constData(), outBuf.size() - (int)zs.avail_out);
                } while (zs.avail_out == 0 || zs.avail_in > 0);
                deflateEnd(&zs);
                if (!zerr) { w.gzData = out; w.ok = true; }
            }
        }
        w.compress_ms = ct.elapsed();

        // Log per-file compression stats
        double rspeed = rmx > 0 ? (srcSize / 1e6) / (rmx / 1000.0) : 0;
        double cspeed = w.compress_ms > 0 ? (srcSize / 1e6) / (w.compress_ms / 1000.0) : 0;
        double ratio  = (w.gzData.size() > 0 && !alreadyGz)
                        ? (double)srcSize / w.gzData.size() : 1.0;
        qDebug() << "[COMPRESS] seq=" << capturedSeq
                 << "| read=" << rmx << "ms" << QString::number(rspeed,'f',1) << "MB/s"
                 << "| compress=" << w.compress_ms << "ms" << QString::number(cspeed,'f',1) << "MB/s"
                 << "| ratio=" << QString::number(ratio,'f',2) + "x";

        statReadMs.fetch_add(rmx);
        statCompressMs.fetch_add(w.compress_ms);

        QMutexLocker lk(&writeMutex);
        writeMap[capturedSeq] = std::move(w);
        int depth = (int)writeMap.size();
        if (depth > writeMapDepthMax.load()) writeMapDepthMax.store(depth);
        writeCV.notify_all();
    });

    ++seq;
    setStatus(QString("[%1/%2]  %3  (%4)")
        .arg(seq).arg(totalRemaining).arg(rel)
        .arg(QLocale().formattedDataSize(srcSize,2,QLocale::DataSizeSIFormat)));
}

// Signal writer: no more items coming
readerDone.store(true);
{
    QMutexLocker lk(&writeMutex);
    writeCV.notify_all();
}

### Stage 3 — Writer thread (start this BEFORE the reader loop above)

// Folder state variables moved here (same as current code):
// currentFolder, currentActualBytes, currentTarEst, foldersCompleted
// (set up from resume logic exactly as current code does it)

QThread* writerThread = QThread::create([&]() {
    while (true) {
        WriteItem w;
        {
            QMutexLocker lk(&writeMutex);
            writeCV.wait(&writeMutex, [&]{
                return writeMap.count(nextWriteSeq) > 0 || (readerDone.load() && writeMap.empty());
            });
            if (writeMap.empty() && readerDone.load()) break;
            auto it = writeMap.find(nextWriteSeq);
            if (it == writeMap.end()) continue;
            w = std::move(it->second);
            writeMap.erase(it);
        }

        if (!w.ok) {
            qDebug() << "[BACKUP] compress FAILED:" << w.rel;
            ramInFlight.fetchAndAddOrdered(-w.srcSize);
            ramCV.notify_all();
            ++nextWriteSeq;
            continue;
        }

        // Folder-full check (writer decides because gz size now known)
        qint64 tarBytes = tarFileBytes((qint64)w.gzData.size());
        if (currentTarEst + tarBytes > maxFolderSize) {
            ++foldersCompleted;
            currentFolder = nextFolderPath();
            QDir().mkpath(currentFolder);
            currentActualBytes = 0;
            currentTarEst      = 1024;
            qDebug() << "[BACKUP] new folder:" << currentFolder;
            setFolderProgress(0, foldersCompleted+1, foldersCompleted+1);
        }

        // Ensure subdir exists
        QString relDir = QFileInfo(w.gzName).path();
        if (!relDir.isEmpty() && relDir != ".")
            QDir().mkpath(currentFolder + "/" + relDir);

        QString destFile = currentFolder + "/" + w.gzName;

        // Write
        QElapsedTimer wt; wt.start();
        bool writeOk = false;
        {
            QFile outF(destFile);
            if (outF.open(QIODevice::WriteOnly)) {
                writeOk = (outF.write(w.gzData) == (qint64)w.gzData.size());
                outF.close();
            }
        }
        qint64 write_ms = wt.elapsed();
        if (!writeOk) {
            QFile::remove(destFile);
            qDebug() << "[BACKUP] write FAILED:" << destFile;
            ramInFlight.fetchAndAddOrdered(-w.srcSize);
            ramCV.notify_all();
            ++nextWriteSeq;
            continue;
        }

        // Gzip validation for compressed files
        if (!w.alreadyGz && !isValidGzip(destFile, w.srcSize)) {
            QFile::remove(destFile);
            qDebug() << "[BACKUP] INVALID gzip:" << destFile;
            ramInFlight.fetchAndAddOrdered(-w.srcSize);
            ramCV.notify_all();
            ++nextWriteSeq;
            continue;
        }

        // Update folder tracking
        qint64 gzSize = (qint64)w.gzData.size();
        currentActualBytes += gzSize;
        currentTarEst      += tarFileBytes(gzSize);

        // INSERT into done (9 params)
        double ratio = (gzSize > 0 && !w.alreadyGz) ? (double)w.srcSize / gzSize : 1.0;
        QString dstRelPath = QDir(destBase).relativeFilePath(destFile);
        QByteArray relB = w.rel.toUtf8();
        QByteArray dstB = dstRelPath.toUtf8();
        sqlite3_bind_text  (doneInsertStmt, 1, sourceRootB.constData(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text  (doneInsertStmt, 2, relB.constData(),        -1, SQLITE_TRANSIENT);
        sqlite3_bind_text  (doneInsertStmt, 3, dstB.constData(),        -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64 (doneInsertStmt, 4, w.srcSize);
        sqlite3_bind_int64 (doneInsertStmt, 5, gzSize);
        sqlite3_bind_double(doneInsertStmt, 6, ratio);
        sqlite3_bind_int64 (doneInsertStmt, 7, w.read_ms);
        sqlite3_bind_int64 (doneInsertStmt, 8, w.compress_ms);
        sqlite3_bind_int64 (doneInsertStmt, 9, write_ms);
        sqlite3_step(doneInsertStmt);
        sqlite3_reset(doneInsertStmt);
        dbExec(sessionDb, "COMMIT");
        dbExec(sessionDb, "BEGIN TRANSACTION");

        statWriteMs.fetch_add(write_ms);
        ++filesProcessed;

        // Release RAM budget
        ramInFlight.fetchAndAddOrdered(-w.srcSize);
        {
            QMutexLocker lk(&writeMutex);
            ramCV.notify_all();
        }

        // GUI update every 10 files
        if (filesProcessed % 10 == 0) {
            setProgressVal(filesProcessed);
            setTarEst(currentActualBytes, currentTarEst);
            setFolderProgress(currentActualBytes, foldersCompleted+1, foldersCompleted+1);
            double elapsed = totalTimer.elapsed() / 1000.0;
            double fps     = elapsed > 0 ? filesProcessed / elapsed : 0;
            double mbps    = elapsed > 0 ? (totalBytesWritten / 1e6) / elapsed : 0;
            int remaining  = totalRemaining - filesProcessed;
            double eta     = fps > 0 ? remaining / fps : 0;
            setCounts(QString("Done: %1/%2 | Written: %3 | Ratio: %4x | %5 f/s %6 MB/s | ETA: %7 min | Folder: %8")
                .arg(filesProcessed).arg(totalRemaining)
                .arg(QLocale().formattedDataSize(totalBytesWritten))
                .arg(QString::number(ratio,'f',2))
                .arg(QString::number(fps,'f',1))
                .arg(QString::number(mbps,'f',1))
                .arg(QString::number(eta/60,'f',0))
                .arg(QDir(currentFolder).dirName()));
        }

        // Summary every 1000 files
        if (filesProcessed % 1000 == 0) {
            double elapsed = totalTimer.elapsed() / 1000.0;
            double avgRead     = statReadMs.load()     / (double)filesProcessed;
            double avgCompress = statCompressMs.load() / (double)filesProcessed;
            double avgWrite    = statWriteMs.load()    / (double)filesProcessed;
            qDebug() << "[BACKUP]" << filesProcessed << "/" << totalRemaining
                     << "| read_avg=" << QString::number(avgRead/1000,'f',2) << "s"
                     << "| compress_avg=" << QString::number(avgCompress/1000,'f',2) << "s"
                     << "| write_avg=" << QString::number(avgWrite/1000,'f',2) << "s"
                     << "| RAM_inflight=" << QLocale().formattedDataSize(ramInFlight.load())
                     << "| writeMap_depth_max=" << writeMapDepthMax.load()
                     << "| reader_stalls=" << readerStalls.load()
                     << "| elapsed=" << QString::number(elapsed,'f',0) << "s";
        }

        ++nextWriteSeq;
    }

    dbExec(sessionDb, "COMMIT");
});
writerThread->start();

// ... (reader loop goes here) ...

writerThread->wait();
delete writerThread;

### Final summary (keep existing qDebug block, add pipeline stats)

qDebug() << "[BACKUP DONE] files=" << filesProcessed
         << "| read=" << QLocale().formattedDataSize(totalBytesRead)
         << "written=" << QLocale().formattedDataSize(totalBytesWritten)
         << "| avg read=" << QString::number(statReadMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
         << "compress=" << QString::number(statCompressMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
         << "write=" << QString::number(statWriteMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
         << "| stalls=" << readerStalls.load()
         << "| elapsed=" << QString::number(totalTimer.elapsed()/1000.0,'f',1) << "s";

---

## Important rules (do not break these)

- journal_mode=DELETE, not WAL (NFS incompatibility)
- Never use QFileInfo::exists() before opening files on NFS — just open directly and handle failure
- source_root column in both files and done tables — NEVER remove it, all queries must filter by source_root
- doneInsertStmt is prepared in the constructor — if schema changes (adding columns), re-prepare it there
- Scan dialog must start at /home/data
- Already-.gz files are COPIED not re-compressed — detected by QFileInfo(rel).suffix().toLower() == "gz"
- isValidGzip called with srcSize for compressed files, -1 for copied .gz files (srcSize is gz size not uncompressed)
- Folder fill on resume comes from done table SQL query, not filesystem scan
- Writer thread is the ONLY thread that touches SQLite after init (no locking needed for DB ops)
- Stop: stopRequested checked in reader before each file, in compression worker inside deflate loop, writer exits when readerDone && writeMap.empty()
