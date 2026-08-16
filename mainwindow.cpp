#include "mainwindow.h"
#include "./ui_mainwindow.h"
#include <QCloseEvent>
#include <QFileDialog>
#include <QDir>
#include <QDirIterator>
#include <QFileInfo>
#include <QDateTime>
#include <QElapsedTimer>
#include <QMessageBox>
#include <QStandardPaths>
#include <QTableWidgetItem>
#include <QThreadPool>
#include <QLocale>
#include <QtConcurrent/QtConcurrent>
#include <zlib.h>
#include <QMutex>
#include <QThread>
#include <QWaitCondition>
#include <QRegularExpression>
#include <map>
#include <QSet>
#include <sys/vfs.h>
#include <cstdlib>
#include <climits>
#include <thread>
#include <chrono>

#include "dbconfigdialog.h"

// ---------------------------------------------------------------------------
// runBackup() phase tracking — read by initiateShutdown() so its status
// messages reflect what's actually running instead of always saying
// "waiting for writer" (only true once Phase 3 starts).
// ---------------------------------------------------------------------------

static constexpr int kBackupPhaseIdle          = 0;
static constexpr int kBackupPhaseVerify        = 1; // Phase 1: validate done table
static constexpr int kBackupPhaseOrphanScan    = 2; // Phase 2: orphan scan + size check
static constexpr int kBackupPhasePipeline      = 3; // Phase 3: read/compress/write pipeline

static const char* backupPhaseLabel(int phase)
{
    switch (phase) {
        case kBackupPhaseVerify:     return "Phase 1 (verifying done table)";
        case kBackupPhaseOrphanScan: return "Phase 2 (orphan scan)";
        case kBackupPhasePipeline:   return "Phase 3 (write pipeline)";
        default:                     return "no backup phase";
    }
}

// ---------------------------------------------------------------------------
// RAII heartbeat — logs "<label> still running… Ns elapsed" every ~2s from a
// side thread while a single blocking call (one DB query, one directory
// listing) is in flight on the calling thread. A "took Nms" log printed
// after the call returns is silent for the entire duration of a SINGLE slow
// call with no internal loop to hook progress into — this fills that gap.
// Construct right before the blocking call, let it go out of scope right
// after; the destructor stops the side thread and joins it.
// ---------------------------------------------------------------------------
class QueryHeartbeat {
public:
    explicit QueryHeartbeat(QString label) : m_label(std::move(label))
    {
        m_timer.start();
        m_thread = std::thread([this] {
            while (!m_stop.load()) {
                for (int i = 0; i < 20 && !m_stop.load(); ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (!m_stop.load())
                    qDebug().noquote() << QString("[BACKUP] %1 — still running… %2 s elapsed")
                        .arg(m_label).arg(m_timer.elapsed() / 1000.0, 0, 'f', 1);
            }
        });
    }
    ~QueryHeartbeat()
    {
        m_stop.store(true);
        if (m_thread.joinable()) m_thread.join();
    }
    QueryHeartbeat(const QueryHeartbeat&) = delete;
    QueryHeartbeat& operator=(const QueryHeartbeat&) = delete;

private:
    QString m_label;
    QElapsedTimer m_timer;
    std::thread m_thread;
    std::atomic<bool> m_stop{false};
};

// ---------------------------------------------------------------------------
// Two independent on-tape capacity models, both computed for every file:
//   tar:  512-byte header + data rounded up to a 512-byte boundary (models a
//         tar stream, padded once at the very end to an LTO block).
//   LTFS: each file's data block-aligned individually (LTFS is a real
//         filesystem on block-addressable media, unlike a tar byte stream)
//         plus a per-file index/metadata overhead constant. The block
//         constant is reused from tar's own LTO-block rounding; the index
//         overhead is the genuinely uncertain part and is a user-editable,
//         QSettings-persisted value (see lineEditLtfsOverhead) rather than a
//         hardcoded guess — recalibrate it from a real df-before/after
//         measurement on an actual tape write.
// Neither model is assumed to bound the other — a folder proven to fit under
// one estimate is not guaranteed to fit under the other.
// ---------------------------------------------------------------------------

static constexpr qint64 kLtoBlockBytes = 524288; // 512 KiB LTO block

static qint64 tarFileBytes(qint64 fileSize)
{
    return 512 + ((fileSize + 511) / 512) * 512;
}

static qint64 ltfsFileBytes(qint64 fileSize, qint64 ltfsIndexOverheadBytes)
{
    qint64 dataBlocks = ((fileSize + kLtoBlockBytes - 1) / kLtoBlockBytes) * kLtoBlockBytes;
    return dataBlocks + ltfsIndexOverheadBytes;
}

// Gzip validation — checks magic header + ISIZE footer.
// Pass expectedOriginalSize >= 0 to also verify ISIZE matches (for freshly compressed files).
// Pass -1 to skip ISIZE check (for copied .gz files where uncompressed size is unknown).
static bool isValidGzip(const QString& path, qint64 expectedOriginalSize = -1)
{
    QFile f(path);
    if (!f.open(QIODevice::ReadOnly) || f.size() < 18) return false;
    QByteArray hdr = f.read(2);
    if ((uchar)hdr[0] != 0x1f || (uchar)hdr[1] != 0x8b) return false;
    if (expectedOriginalSize < 0) return true;
    if (!f.seek(f.size() - 4)) return false;
    QByteArray tail = f.read(4);
    quint32 isize = (uchar)tail[0] | ((uchar)tail[1]<<8) | ((uchar)tail[2]<<16) | ((uchar)tail[3]<<24);
    return isize == (quint32)(expectedOriginalSize & 0xFFFFFFFF);
}

// Scan existing folder and return raw/tar/LTFS byte totals for resume.
struct FolderSizes {
    qint64 actualBytes = 0; // raw sum of file sizes — informational only, never a hard limit
    qint64 tarBytes    = 1024;
    qint64 ltfsBytes   = 0;
};

static FolderSizes folderSizes(const QString& folderPath, qint64 ltfsIndexOverheadBytes)
{
    FolderSizes fs;
    if (!QDir(folderPath).exists()) return fs;
    QDirIterator it(folderPath,
                    QDir::Files | QDir::NoSymLinks | QDir::NoDotAndDotDot,
                    QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        qint64 sz = it.fileInfo().size();
        fs.actualBytes += sz;
        fs.tarBytes    += tarFileBytes(sz);
        fs.ltfsBytes   += ltfsFileBytes(sz, ltfsIndexOverheadBytes);
    }
    fs.tarBytes = ((fs.tarBytes + kLtoBlockBytes - 1) / kLtoBlockBytes) * kLtoBlockBytes;
    // Not applying the same rounding to ltfsBytes — that models tar's own
    // end-of-archive padding, which has no LTFS equivalent; every file's
    // contribution to ltfsBytes is already block-aligned individually.
    return fs;
}

static constexpr const char* kHardLimitModelSettingsKey = "hardLimitModel";
static constexpr const char* kLtfsOverheadSettingsKey   = "ltfsIndexOverheadBytes";

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
    , ui(new Ui::MainWindow)
    , settings("MakeTapeFolders", "MakeTapeFolders")
{
    qDebug() << "[INIT 1] setupUi…";
    ui->setupUi(this);
    if (settings.contains("geometry")) restoreGeometry(settings.value("geometry").toByteArray());
    qDebug() << "[INIT 2] setupUi done";

    qDebug() << "[INIT 3] opening active DB backend…";
    openActiveBackendOrExit();
    qDebug() << "[INIT 9] schema ready";

    qDebug() << "[INIT 13] restoring QSettings…";
    {
        QString src   = settings.value("source").toString();
        QString dest  = settings.value("destination").toString();
        QString pfx   = settings.value("folderPrefix", "tape").toString();
        // 11.7, not 12: the nominal 12TB LTO figure isn't the real usable
        // capacity — 11.7TB is the actual measured usable capacity from a
        // real tape (du + reported free space after a full write), not a
        // vendor round number.
        QString maxSz = settings.value("maxFolderSize", "11.7").toString();
        if (!src.isEmpty())  { sourcedir.setPath(src); }
        if (!dest.isEmpty()) { destinationdir.setPath(dest); ui->labelDest->setText(dest); }
        ui->lineEditPrefix->setText(pfx);
        ui->lineEditMaxSize->setText(maxSz);

        ui->lineEditLtfsOverhead->setText(settings.value(kLtfsOverheadSettingsKey, "1024").toString());
        QString hlm = settings.value(kHardLimitModelSettingsKey, "tar").toString();
        activeHardLimitModel.store(hlm == "ltfs" ? HardLimitModel::Ltfs : HardLimitModel::Tar);
        updateHardLimitButtons();
    }
    qDebug() << "[INIT 14] QSettings done";

    connect(ui->lineEditPrefix, &QLineEdit::textChanged, this, [this](const QString& t){
        settings.setValue("folderPrefix", t);
    });

    qApp->setStyleSheet(R"(
QMainWindow, QWidget#centralwidget { background: #1e1e2e; }
QLabel { color: #a6adc8; }
QComboBox, QLineEdit {
    background: #313244; border: 1px solid #45475a; border-radius: 3px;
    color: #cdd6f4; padding: 2px 6px;
}
QPushButton {
    background: #45475a; border: 1px solid #585b70; border-radius: 3px;
    color: #cdd6f4; padding: 3px 10px;
}
QPushButton:disabled { background: #2a2a3a; border-color: #3a3a4a; color: #585b70; }
QPushButton#pushButtonStart         { background: #a6e3a1; color: #1e1e2e; font-weight: bold; }
QPushButton#pushButtonStart:disabled{ background: #2a3a2a; border-color: #3a4a3a; color: #4a6a4a; }
QPushButton#pushButtonStop          { background: #f38ba8; color: #1e1e2e; font-weight: bold; }
QPushButton#pushButtonStop:disabled { background: #2a1a1e; border-color: #3a2a2e; color: #6e4855; }

QPushButton#pushButtonUseSqlite:checked, QPushButton#pushButtonUseMariaDb:checked,
QPushButton#pushButtonLimitByTar:checked, QPushButton#pushButtonLimitByLtfs:checked {
    background: #a6e3a1; color: #1e1e2e; border: 1px solid #a6e3a1; font-weight: bold;
}

QGroupBox { border: 1px solid #313244; border-radius: 5px; margin-top: 14px;
            background: #1e1e2e; color: #cdd6f4; }
QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left;
                   padding: 0 6px; color: #cdd6f4; font-weight: bold; }

QGroupBox#groupBoxVerify   { border-color: #6c4e8f; }
QGroupBox#groupBoxVerify::title  { color: #cba6f7; }
QGroupBox#groupBoxOrphan   { border-color: #8f5a2a; }
QGroupBox#groupBoxOrphan::title  { color: #fab387; }
QGroupBox#groupBoxPipeline { border-color: #2a8f35; }
QGroupBox#groupBoxPipeline::title { color: #a6e3a1; }

QGroupBox#groupBoxRead     { background: #1c2a3f; border-color: #2a5a8f; }
QGroupBox#groupBoxRead::title    { color: #89b4fa; }
QGroupBox#groupBoxCompress { background: #2a1c1f; border-color: #8f2a35; }
QGroupBox#groupBoxCompress::title { color: #f38ba8; }
QGroupBox#groupBoxQueue    { background: #1f1a2a; border-color: #5f2a8f; }
QGroupBox#groupBoxQueue::title   { color: #cba6f7; }
QGroupBox#groupBoxWrite    { background: #1c2a1f; border-color: #2a8f35; }
QGroupBox#groupBoxWrite::title   { color: #a6e3a1; }

QLabel#labelReadCount, QLabel#labelCompressCount,
QLabel#labelQueueCount, QLabel#labelWriteCount { font-size: 36pt; font-weight: 800; }
QLabel#labelReadCount    { color: #89b4fa; }
QLabel#labelCompressCount { color: #f38ba8; }
QLabel#labelQueueCount   { color: #cba6f7; }
QLabel#labelWriteCount   { color: #a6e3a1; }
QLabel#labelReadDetail, QLabel#labelCompressDetail,
QLabel#labelQueueDetail, QLabel#labelWriteDetail { color: #a6adc8; font-size: 10pt; }

QLabel#labelBackupStatus { color: #89b4fa; font-size: 10pt; }
QLabel#labelStats { color: #a6adc8; font-size: 9pt; font-family: monospace; }
QLabel#labelScanStatus { color: #a6adc8; }
QLabel#labelStopWarning {
    color: #f38ba8; font-size: 11pt; font-weight: bold;
    background: #3a0a0a; border: 1px solid #f38ba8; border-radius: 3px; padding: 4px 8px;
}

QProgressBar { background: #313244; border-radius: 3px; border: none;
               text-align: center; color: #cdd6f4; font-size: 9pt; height: 22px; }
QProgressBar#progressBarVerify::chunk  { background: #9370bc; border-radius: 3px; }
QProgressBar#progressBarOrphan::chunk  { background: #b8842a; border-radius: 3px; }
QProgressBar#progressBarBackup::chunk  { background: #40a060; border-radius: 3px; }
QProgressBar#progressBarFolder::chunk  { background: #2a6abf; border-radius: 3px; }
QProgressBar#progressBarScan::chunk    { background: #585b70; border-radius: 3px; }
QGroupBox#groupBoxShutdown            { border-color: #f38ba8; }
QGroupBox#groupBoxShutdown::title     { color: #f38ba8; }
QPlainTextEdit#plainTextEditShutdown  {
    background: #1a0f0f; color: #cdd6f4;
    font-family: monospace; font-size: 9pt;
    border: 1px solid #45475a; border-radius: 3px;
}
QProgressBar#progressBarShutdown::chunk { background: #f38ba8; border-radius: 3px; }
)");

    on_lineEditMaxSize_textChanged(ui->lineEditMaxSize->text());
    updateBackendButtons();

    // Start is disabled in the .ui file — only enabled when DB stats query finishes
    m_dbLoadTimer.start();
    m_dbLoadDispTimer = new QTimer(this);
    connect(m_dbLoadDispTimer, &QTimer::timeout, this, [this] {
        ui->labelScanStatus->setText(
            QString("Loading database… %1 s").arg(m_dbLoadTimer.elapsed() / 1000.0, 0, 'f', 1));
    });
    m_dbLoadDispTimer->start(250);

    qDebug() << "[INIT 15] calling populateSourceRoots…";
    populateSourceRoots();
    qDebug() << "[INIT 16] constructor done — window should appear now";
}

// ---------------------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------------------

MainWindow::~MainWindow()
{
    qDebug() << "[STOP] app destructor — closing";
    stopRequested.store(true);
    if (m_db) m_db->interrupt();                          // abort any running DB query
    QThreadPool::globalInstance()->waitForDone(3000);     // let DB bg tasks exit
    if (scanFuture.isRunning())   scanFuture.waitForFinished();
    if (backupFuture.isRunning()) backupFuture.waitForFinished();
    if (m_db) m_db->closeThreadConnection();               // this (GUI) thread's connection
    m_db.reset();
    delete ui;
}

// ---------------------------------------------------------------------------
// Clean shutdown — Phase 4 panel
// ---------------------------------------------------------------------------

void MainWindow::closeEvent(QCloseEvent* event)
{
    if (m_shuttingDown) { event->ignore(); return; }

    // Kill any in-progress DB query immediately
    if (m_db) m_db->interrupt();

    if (!backupFuture.isRunning()) {
        settings.setValue("geometry", saveGeometry());
        event->accept();
        return;
    }
    event->ignore();
    initiateShutdown();
}

void MainWindow::initiateShutdown()
{
    if (m_shuttingDown) return;
    m_shuttingDown = true;

    ui->groupBoxShutdown->setVisible(true);
    ui->progressBarShutdown->setValue(0);
    ui->plainTextEditShutdown->clear();
    ui->pushButtonStart->setEnabled(false);
    ui->pushButtonStop->setEnabled(false);

    auto log = [this](const QString& msg, int step = -1) {
        QMetaObject::invokeMethod(this, [this, msg, step] {
            ui->plainTextEditShutdown->appendPlainText(msg);
            if (step >= 0) ui->progressBarShutdown->setValue(step);
        }, Qt::QueuedConnection);
    };

    // Qt6: QFuture::waitForFinished() has no timeout — poll manually
    auto waitWithTimeout = [this](int timeout_ms) -> bool {
        QElapsedTimer t; t.start();
        while (backupFuture.isRunning() && t.elapsed() < timeout_ms)
            QThread::msleep(200);
        return !backupFuture.isRunning();
    };

    QThreadPool::globalInstance()->start([this, log, waitWithTimeout] {
        log("Shutdown initiated.", 0);

        // Step 1: signal stop — reader checks stopRequested every iteration;
        // compress lambdas check it inside the deflate loop (per 4 MB chunk).
        // All CVs have 200 ms timeouts so they self-wake without explicit wakeAll.
        log("  [1/4] Signalling all threads to stop...", 1);
        qDebug() << "[STOP] window closed while backup running — initiating shutdown";
        stopRequested.store(true);
        log("         Stop signal sent.");

        int phase = currentBackupPhase.load();
        log("  [2/4] Waiting for compress pool...", 2);

        if (phase != kBackupPhasePipeline) {
            // Phase 1/2 never touches the write pipeline (it starts at
            // Phase 3) and holds nothing that needs a graceful drain:
            // Phase 1 is read-only, and Phase 2's inserts sit in an
            // uncommitted SQLite transaction that gets safely rolled back
            // on next open if never committed — so there is nothing to
            // lose by exiting now. A single blocking DB/NFS call inside
            // Phase 1/2 (e.g. countDoneInFolder(), QFileInfo::exists())
            // cannot be cancelled from here regardless of how long we
            // wait, so waiting doesn't buy anything either — close/stop
            // should be near-instant, not spend even 10s hoping a call
            // that has already run for 30+ seconds finishes on cue.
            log(QString("  [3/4] %1 holds no pipeline data — closing now...")
                    .arg(backupPhaseLabel(phase)), 3);
            if (waitWithTimeout(300)) {
                log("         Backup thread returned cleanly.", 4);
            } else {
                log(QString("         %1 still running a blocking call that can't be "
                    "cancelled — exiting immediately (nothing to lose).")
                        .arg(backupPhaseLabel(phase)), 4);
                qDebug() << "[STOP] closing immediately —" << backupPhaseLabel(phase)
                         << "not in write pipeline";
                std::_Exit(0);
            }
        } else {
            int files = filesInWriteMap.load() + filesBeingWritten.load();
            int writer_timeout_ms = files > 0 ? files * 60000 : 10000;
            log(QString("  [3/4] Waiting for writer (%1 file%2, max %3 min)...")
                    .arg(files).arg(files == 1 ? "" : "s")
                    .arg(writer_timeout_ms / 60000), 3);

            if (waitWithTimeout(writer_timeout_ms)) {
                log("         All threads done.", 4);
            } else {
                log(QString("         Writer timeout after %1 min — forcing stop...")
                        .arg(writer_timeout_ms / 60000));
                forceKillWriter.store(true);
                // Writer checks forceKillWriter every 200 ms in its wait loop
                if (waitWithTimeout(60000)) {
                    log("         Writer stopped (force-killed).", 4);
                } else {
                    // forceKillWriter only stops the writer thread — it does nothing
                    // for a read stuck inside a blocking NFS syscall, which is what
                    // is actually still running at this point. That read cannot be
                    // cancelled from here, and letting this function return to
                    // close() would just re-enter closeEvent() (backupFuture is
                    // still "running" forever), which calls initiateShutdown() again
                    // — an infinite retry loop, not a graceful wait. There is no
                    // safe graceful path left, so terminate the process immediately
                    // instead of looping. Each successful file already committed
                    // its own DB transaction, so the DB is consistent up to the
                    // last file actually written.
                    log("         WARNING: writer thread still stuck (likely a blocked NFS "
                        "read/stat that cannot be cancelled) — force-quitting now instead of "
                        "hanging indefinitely.", 4);
                    qDebug() << "[STOP] backup thread unresponsive after force-kill — hard exit";
                    std::_Exit(0);
                }
            }
        }

        // Step 4: runBackup() has returned — all stack locals (writeMap, compressPool,
        // writerThread, gzData QByteArrays) are destroyed by RAII → RAM freed.
        log("  [4/4] RAM released — shutdown complete.", 4);
        settings.setValue("geometry", saveGeometry());

        QMetaObject::invokeMethod(this, [this] {
            m_shuttingDown = false;
            close();
        }, Qt::QueuedConnection);
    });
}

// ---------------------------------------------------------------------------
// Tab 1 — Populate source root dropdown from DB and show per-root stats
// ---------------------------------------------------------------------------

void MainWindow::populateSourceRoots()
{
    QString lastSrc = settings.value("source").toString();
    QThreadPool::globalInstance()->start([this, lastSrc]() {
        QElapsedTimer rootsTimer; rootsTimer.start();
        QStringList roots;
        if (m_db) {
            QueryHeartbeat hb("populateSourceRoots: SELECT DISTINCT source_root");
            roots = m_db->sourceRoots();
        }
        qint64 rootsMs = rootsTimer.elapsed();
        qDebug() << "[DB LOAD] found" << roots.size() << "source root(s) —"
                 << "SELECT DISTINCT source_root query took" << rootsMs << "ms";
        QMetaObject::invokeMethod(this, [this, roots, lastSrc]() {
            ui->comboBoxSourceRoot->blockSignals(true);
            ui->comboBoxSourceRoot->clear();
            for (const QString& r : roots) ui->comboBoxSourceRoot->addItem(r);
            int idx = ui->comboBoxSourceRoot->findText(lastSrc);
            ui->comboBoxSourceRoot->setCurrentIndex(idx >= 0 ? idx : 0);
            ui->comboBoxSourceRoot->blockSignals(false);
            on_comboBoxSourceRoot_currentIndexChanged(
                qMax(0, ui->comboBoxSourceRoot->currentIndex()));
        }, Qt::QueuedConnection);
    });
}

// ---------------------------------------------------------------------------
// Tab 1 — Source root selection → update stats label + SQL filter
// ---------------------------------------------------------------------------

void MainWindow::on_comboBoxSourceRoot_currentIndexChanged(int index)
{
    if (!m_db) return;
    QString root = (index >= 0) ? ui->comboBoxSourceRoot->itemText(index) : QString();

    if (!root.isEmpty()) {
        sourcedir.setPath(root);
        settings.setValue("source", root);
    }

    ui->labelBackupStats->setText("Loading…");
    ui->pushButtonStart->setEnabled(false);
    m_dbLoadTimer.restart();
    if (m_dbLoadDispTimer) m_dbLoadDispTimer->start(250);

    QString rootForStats = root;
    QThreadPool::globalInstance()->start([this, rootForStats]() {
        // Two simple indexed queries — avoids slow LEFT JOIN on large tables
        SourceStats stats;
        {
            QueryHeartbeat hb(QString("statsForSource(%1)").arg(rootForStats));
            stats = m_db->statsForSource(rootForStats);
        }
        qint64 totalFiles = stats.totalFiles, totalSize = stats.totalSize,
               doneFiles  = stats.doneFiles,  doneSize  = stats.doneSize;

        QString statsText = QString("%1 files — %2   |   Done: %3 files — %4")
            .arg(QLocale().toString(totalFiles))
            .arg(QLocale().formattedDataSize(totalSize, 2))
            .arg(QLocale().toString(doneFiles))
            .arg(QLocale().formattedDataSize(doneSize, 2));

        qint64 loadMs = m_dbLoadTimer.elapsed();
        qDebug() << "[DB LOAD] counted" << totalFiles << "indexed /" << doneFiles
                 << "done for source" << rootForStats << "—"
                 << "COUNT/SUM stats query took" << loadMs << "ms";

        QMetaObject::invokeMethod(this, [this, statsText, loadMs]{
            if (m_dbLoadDispTimer) m_dbLoadDispTimer->stop();
            ui->labelBackupStats->setText(statsText);
            ui->pushButtonScanSource->setEnabled(true);
            ui->pushButtonScanSubfolder->setEnabled(true);
            ui->comboBoxSourceRoot->setEnabled(true);
            ui->pushButtonDest->setEnabled(true);
            ui->lineEditPrefix->setEnabled(true);
            ui->lineEditMaxSize->setEnabled(true);
            if (!backupFuture.isRunning() && !scanFuture.isRunning())
                ui->pushButtonStart->setEnabled(true);
            // pushButtonStop stays disabled — only enabled when backup is running
            ui->labelScanStatus->setText(
                QString("Ready  (DB loaded in %1 s)").arg(loadMs / 1000.0, 0, 'f', 2));
        }, Qt::QueuedConnection);
    });
}


// ---------------------------------------------------------------------------
// Tab 1 — Scan source (independent folder picker, background thread)
// ---------------------------------------------------------------------------

void MainWindow::on_pushButtonScanSource_clicked()
{
    if (!m_db) { QMessageBox::critical(this, "Scan", "Database not open."); return; }
    if (scanFuture.isRunning()) {
        QMessageBox::information(this, "Scan", "A scan is already in progress.");
        return;
    }

    QFileDialog dialog(this);
    dialog.setOptions(QFileDialog::HideNameFilterDetails | QFileDialog::DontUseNativeDialog);
    dialog.setFileMode(QFileDialog::Directory);
    dialog.setLabelText(QFileDialog::FileName, "Source folder to scan");
    dialog.setDirectory("/home/data");
    if (!dialog.exec()) return;
    QStringList sel = dialog.selectedFiles();
    if (sel.isEmpty()) return;

    QString src = sel[0];
    sourcedir.setPath(src);
    settings.setValue("source", src);
    ui->comboBoxSourceRoot->blockSignals(true);
    ui->comboBoxSourceRoot->clear();
    ui->comboBoxSourceRoot->addItem(src);
    ui->comboBoxSourceRoot->setCurrentIndex(0);
    ui->comboBoxSourceRoot->blockSignals(false);
    scanFuture = QtConcurrent::run([this, src]() { runScan(src, src); });
}

// ---------------------------------------------------------------------------
// Tab 1 — Refresh subfolder
// ---------------------------------------------------------------------------

void MainWindow::on_pushButtonScanSubfolder_clicked()
{
    if (!m_db) { QMessageBox::critical(this, "Scan", "Database not open."); return; }
    if (scanFuture.isRunning()) {
        QMessageBox::information(this, "Scan", "A scan is already in progress.");
        return;
    }

    QFileDialog dialog(this);
    dialog.setOptions(QFileDialog::HideNameFilterDetails | QFileDialog::DontUseNativeDialog);
    dialog.setFileMode(QFileDialog::Directory);
    dialog.setLabelText(QFileDialog::FileName, "Subfolder to refresh");
    dialog.setDirectory(sourcedir.path());
    if (!dialog.exec()) return;
    QStringList sel = dialog.selectedFiles();
    if (sel.isEmpty()) return;
    QString subfolder = sel[0];

    QString sourceRoot = sourcedir.path();
    if (sourceRoot.isEmpty()) {
        QMessageBox::warning(this, "Refresh subfolder",
            "No source directory set. Use 'Scan source' first.");
        return;
    }
    if (!subfolder.startsWith(sourceRoot + "/") && subfolder != sourceRoot) {
        QMessageBox::warning(this, "Refresh subfolder",
            "Selected folder is not under the current source:\n" + subfolder +
            "\n\nCurrent source: " + sourceRoot);
        return;
    }

    qDebug() << "[SCAN] partial refresh — subfolder:" << subfolder << "root:" << sourceRoot;
    scanFuture = QtConcurrent::run([this, subfolder, sourceRoot]() {
        runScan(subfolder, sourceRoot);
    });
}

// ---------------------------------------------------------------------------
// Tab 1 — Two-phase background scan
// Phase 1: fast count (no stat) → sets progress bar max
// Phase 2: full stat + bulk INSERT OR REPLACE in 1000-row batches
// ---------------------------------------------------------------------------

void MainWindow::runScan(const QString& scanRoot, const QString& sourceRoot)
{
    const bool isPartial = (scanRoot != sourceRoot);

    auto setEnabled = [this](bool en) {
        QMetaObject::invokeMethod(ui->pushButtonScanSource,     [this,en]{ ui->pushButtonScanSource->setEnabled(en); },     Qt::QueuedConnection);
        QMetaObject::invokeMethod(ui->pushButtonScanSubfolder,  [this,en]{ ui->pushButtonScanSubfolder->setEnabled(en); },  Qt::QueuedConnection);
        QMetaObject::invokeMethod(ui->pushButtonStart,          [this,en]{ ui->pushButtonStart->setEnabled(en); },          Qt::QueuedConnection);
        QMetaObject::invokeMethod(ui->pushButtonUseSqlite,      [this,en]{ ui->pushButtonUseSqlite->setEnabled(en); },      Qt::QueuedConnection);
        QMetaObject::invokeMethod(ui->pushButtonUseMariaDb,     [this,en]{ ui->pushButtonUseMariaDb->setEnabled(en); },     Qt::QueuedConnection);
    };
    auto setStatus = [this](const QString& s) {
        QMetaObject::invokeMethod(ui->labelScanStatus, [this,s]{ ui->labelScanStatus->setText(s); }, Qt::QueuedConnection);
    };
    auto setProgressVal = [this](int v) {
        QMetaObject::invokeMethod(ui->progressBarScan, [this,v]{ ui->progressBarScan->setValue(v); }, Qt::QueuedConnection);
    };
    auto setProgressMax = [this](int m) {
        QMetaObject::invokeMethod(ui->progressBarScan, [this,m]{ ui->progressBarScan->setMaximum(m); }, Qt::QueuedConnection);
    };

    setEnabled(false);

    // ── Phase 1: count only (no stat — fast on any medium) ────────────────
    setStatus("Phase 1/2 — counting files…");
    setProgressMax(0); // indeterminate bounce

    qint64 totalCount = 0;
    {
        QDirIterator counter(scanRoot,
                             QDir::Files | QDir::NoSymLinks | QDir::NoDotAndDotDot,
                             QDirIterator::Subdirectories);
        while (counter.hasNext()) { counter.next(); ++totalCount; }
    }

    qDebug() << "[SCAN] phase 1:" << totalCount << "files in" << scanRoot;
    setProgressMax((int)totalCount);
    setProgressVal(0);
    setStatus(QString("Phase 2/2 — indexing %1 files…").arg(totalCount));

    // ── Delete stale entries for the scanned subtree ───────────────────────
    {
        if (isPartial) {
            QString relPrefix = scanRoot.mid(sourceRoot.length());
            if (relPrefix.startsWith('/')) relPrefix = relPrefix.mid(1);
            m_db->deleteScanForSubfolder(sourceRoot, relPrefix + "/%");
        } else {
            m_db->deleteScanForSource(sourceRoot);
        }
    }

    // ── Phase 2: walk + stat + bulk insert ────────────────────────────────
    QElapsedTimer timer;
    timer.start();

    m_db->beginTxn();

    qint64 filesIndexed = 0, totalBytes = 0, batchStart = 0;

    QDirIterator it(scanRoot,
                    QDir::Files | QDir::NoSymLinks | QDir::NoDotAndDotDot,
                    QDirIterator::Subdirectories);

    while (it.hasNext()) {
        it.next();
        QFileInfo fi  = it.fileInfo();
        qint64    sz  = fi.size();
        qint64    mt  = fi.lastModified().toSecsSinceEpoch();
        qint64    now = QDateTime::currentSecsSinceEpoch();

        QString rel = it.filePath().mid(sourceRoot.length());
        if (rel.startsWith('/')) rel = rel.mid(1);

        FileRow row;
        row.source_root = sourceRoot;
        row.src          = rel;
        row.size          = sz;
        row.ext           = fi.suffix().toLower();
        row.folder        = rel.section('/', 0, 0);
        row.mtime          = mt;
        row.scanned_at     = now;
        m_db->insertScannedFile(row);

        ++filesIndexed;
        totalBytes += sz;

        if (filesIndexed % 1000 == 0) {
            m_db->flushTxn();

            qint64 elapsedMs = timer.elapsed();
            double elapsedS  = elapsedMs / 1000.0;
            double fps  = elapsedS > 0 ? filesIndexed / elapsedS : 0;
            double mbps = elapsedS > 0 ? (totalBytes / 1e6) / elapsedS : 0;
            double batchMs = elapsedMs - batchStart;
            batchStart = elapsedMs;

            qDebug() << "[SCAN]" << filesIndexed << "/" << totalCount
                     << "| cumul:" << QString::number(fps,'f',0) << "files/s"
                     << QString::number(mbps,'f',1) << "MB/s"
                     << "| batch 1000 in" << QString::number(batchMs,'f',0) << "ms"
                     << "| elapsed" << QString::number(elapsedS,'f',1) << "s";

            setProgressVal((int)filesIndexed);
            setStatus(QString("Indexing: %1 / %2  —  %3 files/s  —  %4 MB/s")
                .arg(filesIndexed).arg(totalCount)
                .arg(QString::number(fps,'f',0))
                .arg(QString::number(mbps,'f',1)));
        }
    }

    m_db->commitTxn();

    double totalS = timer.elapsed() / 1000.0;
    double fps  = totalS > 0 ? filesIndexed / totalS : 0;
    double mbps = totalS > 0 ? (totalBytes / 1e6) / totalS : 0;

    qDebug() << "[SCAN DONE] files:" << filesIndexed
             << "size:"  << QLocale().formattedDataSize(totalBytes)
             << "time:"  << QString::number(totalS,'f',1) << "s"
             << "avg:"   << QString::number(fps,'f',0) << "files/s"
             << "|"      << QString::number(mbps,'f',1) << "MB/s";

    setProgressVal((int)filesIndexed);
    setStatus(QString("Done: %1 files, %2 — %3 s — avg %4 files/s, %5 MB/s")
        .arg(filesIndexed)
        .arg(QLocale().formattedDataSize(totalBytes))
        .arg(QString::number(totalS,'f',1))
        .arg(QString::number(fps,'f',0))
        .arg(QString::number(mbps,'f',1)));
    setEnabled(true);

    // Refresh source root dropdown (adds new root if this was a new directory)
    QMetaObject::invokeMethod(this, [this]{
        populateSourceRoots();
    }, Qt::QueuedConnection);
}

// ---------------------------------------------------------------------------
// Destination directory picker (persists to QSettings)
// ---------------------------------------------------------------------------

void MainWindow::on_pushButtonDest_clicked()
{
    QFileDialog dialog(this);
    dialog.setOptions(QFileDialog::HideNameFilterDetails | QFileDialog::DontUseNativeDialog);
    dialog.setFileMode(QFileDialog::Directory);
    dialog.setLabelText(QFileDialog::FileName, "Destination base directory (tape folders are created here)");
    dialog.setDirectory(destinationdir.path());
    if (!dialog.exec()) return;
    QStringList sel = dialog.selectedFiles();
    if (sel.isEmpty()) return;

    destinationdir.setPath(sel[0]);
    ui->labelDest->setText(destinationdir.path());
    settings.setValue("destination", destinationdir.path());
}

// ---------------------------------------------------------------------------
// Tab 2 — Max folder size display + save
// ---------------------------------------------------------------------------

void MainWindow::on_lineEditMaxSize_textChanged(const QString &arg1)
{
    bool ok;
    double tb = arg1.toDouble(&ok);
    if (ok && tb > 0 && tb <= 20.0) {
        qint64 bytes = (qint64)(tb * 1000000000000LL);
        ui->labelMaxFormatted->setText(QString("= %1 bytes").arg(QLocale().toString(bytes)));
        settings.setValue("maxFolderSize", arg1);
    } else {
        ui->labelMaxFormatted->setText(ok ? "Too large (max 20)" : "Invalid");
    }
}

// ---------------------------------------------------------------------------
// Tab 2 — Start backup
// ---------------------------------------------------------------------------

void MainWindow::on_pushButtonStart_clicked()
{
    if (sourcedir.path().isEmpty() || !sourcedir.exists()) {
        QMessageBox::critical(this, "Error", "Select a valid source directory first.");
        return;
    }
    if (destinationdir.path().isEmpty() || !destinationdir.exists()) {
        QMessageBox::critical(this, "Error", "Select a valid destination directory first.");
        return;
    }
    if (!m_db) {
        QMessageBox::critical(this, "Error", "Session database not open.");
        return;
    }
    if (scanFuture.isRunning()) {
        QMessageBox::warning(this, "Error", "A database scan is in progress. Wait for it to finish.");
        return;
    }
    if (backupFuture.isRunning()) {
        QMessageBox::information(this, "Backup", "Backup is already running.");
        return;
    }

    // Capture UI values on the main thread before handing off to the background thread
    QString prefix = ui->lineEditPrefix->text().trimmed();
    if (prefix.isEmpty()) prefix = "tape";
    bool ok;
    double tb = ui->lineEditMaxSize->text().toDouble(&ok);
    if (!ok || tb <= 0 || tb > 20.0) {
        QMessageBox::critical(this, "Error", "Invalid tape size — enter a number between 1 and 20 (TB).");
        return;
    }
    qint64 maxFolderSize = (qint64)(tb * 1000000000000LL);

    bool okOverhead;
    qint64 ltfsIndexOverheadBytes = ui->lineEditLtfsOverhead->text().toLongLong(&okOverhead);
    if (!okOverhead || ltfsIndexOverheadBytes < 0 || ltfsIndexOverheadBytes > 1048576) {
        QMessageBox::critical(this, "Error",
            "Invalid LTFS index overhead — enter a non-negative integer number of bytes (max 1048576).");
        return;
    }

    stopRequested.store(false);
    ui->pushButtonStart->setEnabled(false);
    ui->pushButtonStop->setEnabled(true);
    ui->comboBoxSourceRoot->setEnabled(false);
    ui->pushButtonDest->setEnabled(false);
    ui->pushButtonScanSource->setEnabled(false);
    ui->pushButtonScanSubfolder->setEnabled(false);
    ui->pushButtonUseSqlite->setEnabled(false);
    ui->pushButtonUseMariaDb->setEnabled(false);
    ui->lineEditLtfsOverhead->setEnabled(false);
    ui->labelBackupStatus->setText("Starting…");
    ui->progressBarBackup->setValue(0);
    ui->progressBarBackup->setMaximum(100);

    backupFuture = QtConcurrent::run([this, prefix, maxFolderSize, ltfsIndexOverheadBytes]() {
        runBackup(prefix, maxFolderSize, ltfsIndexOverheadBytes);
    });
}

// ---------------------------------------------------------------------------
// Tab 2 — Stop backup
// ---------------------------------------------------------------------------

void MainWindow::on_pushButtonStop_clicked()
{
    qDebug() << "[STOP] Stop button clicked";
    stopRequested.store(true);
    ui->pushButtonStop->setEnabled(false);
    int phase = currentBackupPhase.load();
    if (phase == kBackupPhasePipeline) {
        ui->labelBackupStatus->setText("Stopping — files still in pipeline are finishing, DO NOT CLOSE the app…");
    } else {
        // Phase 1/2 has no pipeline to drain — whatever single DB/NFS call
        // is currently blocking can't be interrupted by this flag, so this
        // will only take effect once that call returns on its own. Say so
        // instead of claiming files are "in pipeline" when none exist yet.
        ui->labelBackupStatus->setText(
            QString("Stopping — will take effect once the current %1 step finishes "
                    "(can't be interrupted mid-call). Close the window to exit immediately.")
                .arg(backupPhaseLabel(phase)));
    }
}

// ---------------------------------------------------------------------------
// Tab 2 — Backup worker (background thread, sequential)
//
// For each remaining file (ordered by folder then name for sequential disk reads):
//   - .gz source → copy as-is (no re-compression)
//   - everything else → deflateInit2 with windowBits=15+16 (proper gzip)
// Folder-full check uses exact tar-on-tape byte count (no guessing).
// Writes one entry to done table per successful file (committed every 100 files).
// ---------------------------------------------------------------------------

void MainWindow::runBackup(const QString& prefix, qint64 maxFolderSize, qint64 ltfsIndexOverheadBytes)
{
    const QString sourceRoot = sourcedir.path();
    qDebug() << "[BACKUP] sourceRoot:" << sourceRoot;

    const QString destBase   = destinationdir.path();

    auto detectNFS = [](const QString& path) -> bool {
        struct statfs sfs;
        return (statfs(path.toLocal8Bit().constData(), &sfs) == 0 && sfs.f_type == 0x6969);
    };
    const bool srcIsNFS  = detectNFS(sourceRoot);
    const bool destIsNFS = detectNFS(destBase);

    auto setStatus = [this](const QString& s) {
        QMetaObject::invokeMethod(ui->labelBackupStatus,
            [this,s]{ ui->labelBackupStatus->setText(s); }, Qt::QueuedConnection);
    };
    auto setProgressVal = [this](int v) {
        QMetaObject::invokeMethod(ui->progressBarBackup, [this, v] {
            int m = ui->progressBarBackup->maximum();
            double pct = (m > 0) ? 100.0 * v / m : 0.0;
            ui->progressBarBackup->setFormat(
                QString("%1 / %2 files  (%3%)")
                    .arg(v).arg(m).arg(QString::number(pct, 'f', 2)));
            ui->progressBarBackup->setValue(v);
        }, Qt::QueuedConnection);
    };
    auto setProgressMax = [this](int m) {
        QMetaObject::invokeMethod(ui->progressBarBackup,
            [this,m]{ ui->progressBarBackup->setMaximum(m); }, Qt::QueuedConnection);
    };
    // Single source of truth for "data + overhead = total / max" — used by
    // both the folder progress bar and the estimate label so they can never
    // show disagreeing numbers for the same underlying state.
    auto capacityLine = [maxFolderSize](qint64 rawBytes, qint64 total, HardLimitModel model) {
        qint64 overhead = total - rawBytes;
        double pct = maxFolderSize > 0 ? 100.0 * total / maxFolderSize : 0.0;
        return QString("Data: %1  +  Overhead: %2  =  %3  /  %4 max  (%5%)  [%6]")
            .arg(QLocale().formattedDataSize(rawBytes,  2, QLocale::DataSizeSIFormat))
            .arg(QLocale().formattedDataSize(overhead,  2, QLocale::DataSizeSIFormat))
            .arg(QLocale().formattedDataSize(total,     2, QLocale::DataSizeSIFormat))
            .arg(QLocale().formattedDataSize(maxFolderSize, 2, QLocale::DataSizeSIFormat))
            .arg(QString::number(pct, 'f', 2))
            .arg(model == HardLimitModel::Ltfs ? "LTFS" : "Tar");
    };
    auto setFolderProgress = [this, maxFolderSize, capacityLine](qint64 rawBytes, qint64 activeTotal, HardLimitModel model, int folderNum, int foldersTotal, int fileCount) {
        int maxMB = (int)(maxFolderSize / 1000000LL);
        int curMB = (int)(activeTotal   / 1000000LL);
        QString fmt = QString("Folder %1 of %2+  (%3 files)  —  %4")
            .arg(folderNum).arg(foldersTotal).arg(fileCount)
            .arg(capacityLine(rawBytes, activeTotal, model));
        QMetaObject::invokeMethod(ui->progressBarFolder, [this, maxMB, curMB, fmt]{
            ui->progressBarFolder->setMaximum(maxMB);
            ui->progressBarFolder->setValue(curMB);
            ui->progressBarFolder->setFormat(fmt);
        }, Qt::QueuedConnection);
    };
    auto setTarEst = [this, capacityLine](qint64 tar, qint64 ltfs, qint64 rawBytes, HardLimitModel activeModel) {
        QString tarLine  = capacityLine(rawBytes, tar,  HardLimitModel::Tar);
        QString ltfsLine = capacityLine(rawBytes, ltfs, HardLimitModel::Ltfs);
        QString s = (activeModel == HardLimitModel::Ltfs)
            ? QString("%1   |   other model — %2").arg(ltfsLine, tarLine)
            : QString("%1   |   other model — %2").arg(tarLine, ltfsLine);
        QMetaObject::invokeMethod(ui->labelTarEstimate,
            [this,s]{ ui->labelTarEstimate->setText(s); }, Qt::QueuedConnection);
    };
    auto reenable = [this]() {
        QMetaObject::invokeMethod(this, [this]{
            ui->pushButtonStart->setEnabled(true);
            ui->pushButtonStop->setEnabled(false);
            ui->comboBoxSourceRoot->setEnabled(true);
            ui->pushButtonDest->setEnabled(true);
            ui->pushButtonScanSource->setEnabled(true);
            ui->pushButtonScanSubfolder->setEnabled(true);
            ui->pushButtonUseSqlite->setEnabled(true);
            ui->pushButtonUseMariaDb->setEnabled(true);
            ui->lineEditLtfsOverhead->setEnabled(true);
            if (m_pipelineTimer) {
                m_pipelineTimer->stop();
                m_pipelineTimer->deleteLater();
                m_pipelineTimer = nullptr;
            }
            // Stopping the timer only stops future updates — it leaves
            // whatever counts were on screen at the last tick, which then
            // sit there looking like a live in-flight state (queued/writing/
            // active) forever, even though the pipeline is fully idle.
            // Reset to the true idle state explicitly.
            ui->labelReadCount->setText("0");
            ui->labelReadDetail->setText("");
            ui->labelCompressCount->setText("0");
            ui->labelCompressDetail->setText("");
            ui->labelQueueCount->setText("0");
            ui->labelQueueDetail->setText("");
            ui->labelWriteCount->setText("0");
            ui->labelWriteDetail->setText("");
            ui->labelStopWarning->setVisible(false);
        }, Qt::QueuedConnection);
        currentBackupPhase.store(kBackupPhaseIdle);
    };

    // ── Count indexed files to detect missing DB scan ─────────────────────
    {
        qint64 totalIndexed;
        {
            QueryHeartbeat hb("countIndexedFiles");
            totalIndexed = m_db->countIndexedFiles(sourceRoot);
        }
        if (totalIndexed == 0) {
            setStatus("No files indexed for this source — scan it first.");
            reenable();
            return;
        }
    }

    currentBackupPhase.store(kBackupPhaseVerify);
    qDebug() << "[BACKUP] ── Phase 1: validate done table ──────────────────────────";
    // progressBarVerify/labelVerifyStats are only ever written inside the
    // per-item verify loop below, which runs zero times when there's
    // nothing to verify (folderDoneCount == 0, e.g. right after a done-table
    // reset) — leaving whatever was on screen from a previous run
    // displayed unchanged, making it look like Phase 1 never ran while
    // Phase 2 starts moving right after it. Reset explicitly so Phase 1
    // shows a real, current, empty state regardless of how much work it
    // ends up finding.
    QMetaObject::invokeMethod(this, [this] {
        ui->progressBarVerify->setMinimum(0);
        ui->progressBarVerify->setMaximum(1);
        ui->progressBarVerify->setValue(0);
        ui->labelVerifyStats->setText("");
    }, Qt::QueuedConnection);
    // Only validate files in the current (last) tape folder — completed tapes are not re-checked.
    QStringList existingFoldersP1 = QDir(destBase).entryList(
        QStringList() << (prefix + "_???"), QDir::Dirs, QDir::Name);
    QString currentFolderName = existingFoldersP1.isEmpty() ? QString() : existingFoldersP1.last();
    qDebug() << "[BACKUP] Phase 1: current folder for validation:" << currentFolderName;

    int folderDoneCount = 0;
    {
        if (currentFolderName.isEmpty()) {
            qDebug() << "[BACKUP] Phase 1: no existing folders — nothing to validate";
        } else {
        QString folderPattern = currentFolderName + "/%";
        qDebug() << "[BACKUP] Phase 1: counting done rows for" << currentFolderName << "...";
        QElapsedTimer stepTimer; stepTimer.start();
        {
            QueryHeartbeat hb(QString("Phase 1: countDoneInFolder(%1)").arg(currentFolderName));
            folderDoneCount = (int)m_db->countDoneInFolder(sourceRoot, folderPattern);
        }
        qDebug() << "[BACKUP] Phase 1: countDoneInFolder took" << stepTimer.elapsed() << "ms —"
                 << folderDoneCount << "rows";

        setStatus(QString("Verifying %1 files in current tape folder %2…").arg(folderDoneCount).arg(currentFolderName));

        // Load all rows first (SQLite access must be single-threaded)
        struct VerifyItem { QString src, dstFull; qint64 origSize, gzBytes; bool wasAlreadyGz; };
        std::vector<VerifyItem> verifyItems;
        verifyItems.reserve(folderDoneCount);
        {
            qDebug() << "[BACKUP] Phase 1: loading" << folderDoneCount << "done rows for verification...";
            stepTimer.restart();
            QVector<DoneVerifyItem> rows;
            {
                QueryHeartbeat hb(QString("Phase 1: loadDoneForVerification(%1)").arg(currentFolderName));
                rows = m_db->loadDoneForVerification(sourceRoot, folderPattern);
            }
            qDebug() << "[BACKUP] Phase 1: loadDoneForVerification took" << stepTimer.elapsed() << "ms —"
                     << rows.size() << "rows loaded";
            for (const DoneVerifyItem& row : rows) {
                if (row.src.isEmpty() || row.dst.isEmpty()) continue;
                verifyItems.push_back({
                    row.src,
                    destBase + "/" + row.dst,
                    row.bytes,
                    row.gzBytes,
                    row.src.endsWith(".gz", Qt::CaseInsensitive)
                });
            }
        }

        // Nothing to verify — the per-item loop below never runs, so
        // without an explicit message here Phase 1's own section shows
        // nothing durable: the shared labelBackupStatus line above did get
        // "Verifying 0 files..." for a moment, but Phase 2 overwrites that
        // shared line almost immediately, leaving no lasting evidence in
        // Phase 1's own dedicated widgets that it ran and found nothing.
        if (verifyItems.empty()) {
            QMetaObject::invokeMethod(this, [this, folderDoneCount, currentFolderName] {
                ui->progressBarVerify->setMinimum(0);
                ui->progressBarVerify->setMaximum(1);
                ui->progressBarVerify->setValue(1);
                ui->labelVerifyStats->setText(
                    QString("%1: %2 done rows — nothing to verify")
                        .arg(currentFolderName).arg(folderDoneCount));
            }, Qt::QueuedConnection);
        }

        // Verify in parallel — each check is independent (NFS stat + read 6 bytes)
        QMutex                              removeMutex;
        QList<QPair<QString,QString>>       toRemove;  // {src, dstFull}
        std::atomic<int>    checked{0};
        std::atomic<qint64> bytesChecked{0};
        std::atomic<qint64> lastConsoleLogMs{0};
        QElapsedTimer verifyTimer;
        verifyTimer.start();
        qDebug() << "[BACKUP] Phase 1: starting verification of" << verifyItems.size() << "files...";

        QThreadPool verifyPool;
        verifyPool.setMaxThreadCount(32);

        for (auto item : verifyItems) {   // capture by value — loop var changes each iteration
            if (stopRequested.load()) break;
            verifyPool.start([this, item, &checked, &bytesChecked, &lastConsoleLogMs,
                              &removeMutex, &toRemove, &verifyTimer, folderDoneCount] {
                // Tasks already queued (not yet started) when stop is
                // requested skip their blocking file work entirely instead
                // of running to completion — bounds how long
                // verifyPool.waitForDone() below takes to drain.
                if (stopRequested.load()) return;
                bool ok = QFileInfo::exists(item.dstFull) &&
                          isValidGzip(item.dstFull, item.wasAlreadyGz ? -1 : item.origSize);
                if (!ok) {
                    QMutexLocker lk(&removeMutex);
                    toRemove << qMakePair(item.src, item.dstFull);
                } else {
                    bytesChecked.fetch_add(item.wasAlreadyGz ? item.origSize : item.gzBytes);
                }
                int c = ++checked;
                // Console progress fires every ~2s of wall time, regardless
                // of file count, so small batches (fewer than the old fixed
                // "every 5000 files" threshold) still produce visible
                // output instead of going silent until they finish.
                qint64 nowMs = verifyTimer.elapsed();
                qint64 prevLogMs = lastConsoleLogMs.load();
                bool timeToLog = (nowMs - prevLogMs) >= 2000
                    && lastConsoleLogMs.compare_exchange_strong(prevLogMs, nowMs);
                if (timeToLog || c == folderDoneCount) {
                    int bad; { QMutexLocker lk(&removeMutex); bad = toRemove.size(); }
                    qDebug() << "[VERIFY]" << c << "/" << folderDoneCount
                             << "| bad:" << bad
                             << "| elapsed:" << QString::number(nowMs / 1000.0, 'f', 1) << "s";
                }
                if (c % 50 == 0 || c == folderDoneCount) {
                    double el = verifyTimer.elapsed() / 1000.0;
                    int bad; { QMutexLocker lk(&removeMutex); bad = toRemove.size(); }
                    qint64 bc = bytesChecked.load();
                    QMetaObject::invokeMethod(this, [this, c, folderDoneCount, bad, el, bc] {
                        ui->progressBarVerify->setMaximum(folderDoneCount);
                        ui->progressBarVerify->setValue(c);
                        ui->labelVerifyStats->setText(
                            QString("%1 / %2 checked  ·  %3 bad  ·  %4 s")
                                .arg(c).arg(folderDoneCount).arg(bad).arg(QString::number(el,'f',1)));
                        ui->labelBackupStatus->setText(
                            QString("Verifying %1 / %2  (%3)  —  %4 bad  —  %5 s")
                                .arg(c).arg(folderDoneCount)
                                .arg(QLocale().formattedDataSize(bc, 2, QLocale::DataSizeSIFormat))
                                .arg(bad).arg(QString::number(el, 'f', 1)));
                    }, Qt::QueuedConnection);
                }
            });
        }
        verifyPool.waitForDone();

        // Remove bad destination files (single-threaded, after pool is done)
        for (auto& [src, dstFull] : toRemove)
            QFile::remove(dstFull);

        double totalS = verifyTimer.elapsed() / 1000.0;
        qDebug() << "[BACKUP] verify done:" << checked << "checked,"
                 << toRemove.size() << "failed, took"
                 << QString::number(totalS, 'f', 1) << "s";

        setStatus(QString("Verified %1 files (%2) in %3 s  —  %4 removed and re-queued")
            .arg(checked.load())
            .arg(QLocale().formattedDataSize(bytesChecked.load(), 2, QLocale::DataSizeSIFormat))
            .arg(QString::number(totalS, 'f', 1))
            .arg(toRemove.size()));

        if (!toRemove.isEmpty()) {
            QVector<QString> badSrcs;
            badSrcs.reserve(toRemove.size());
            for (auto& [src, dst_] : toRemove) badSrcs.push_back(src);
            m_db->deleteBadDoneEntries(sourceRoot, badSrcs);
        }
        } // end else (currentFolderName not empty)
    }

    if (stopRequested.load()) { setStatus("Stopped."); reenable(); return; }

    currentBackupPhase.store(kBackupPhaseOrphanScan);
    qDebug() << "[BACKUP] ── Phase 2: orphan scan + size check ────────────────────";
    // ── Phase 2: scan destination folders — size-check + orphan reconcile ────
    //
    // For each tape_NNN folder:
    //   1. Sum gz_bytes from done table for that folder → "what done table expects on disk"
    //   2. Walk all files on disk: accumulate real disk bytes
    //   3. Report the delta (mismatch = something to investigate)
    //   4. For every file NOT in done:
    //        - valid gzip + source in files table → INSERT into done
    //        - invalid gzip → delete
    //        - valid but no source entry → leave, count as no-src
    {
        setStatus("Scanning destination folders…");

        // All dst values already known-good from Phase 1 (fast O(1) membership test)
        qDebug() << "[BACKUP] Phase 2: loading all known dst paths for" << sourceRoot << "...";
        QElapsedTimer phase2Timer; phase2Timer.start();
        QSet<QString> knownDst;
        {
            QueryHeartbeat hb("Phase 2: allDstForSource");
            knownDst = m_db->allDstForSource(sourceRoot);
        }
        qDebug() << "[BACKUP] Phase 2: allDstForSource took" << phase2Timer.elapsed() << "ms —"
                 << knownDst.size() << "known dst paths";

        phase2Timer.restart();
        QStringList tapeFolders;
        {
            QueryHeartbeat hb("Phase 2: tape folder listing");
            QDirIterator dit(destBase, QDir::Dirs | QDir::NoSymLinks | QDir::NoDotAndDotDot);
            QRegularExpression tapeRe("^" + QRegularExpression::escape(prefix) + "_\\d+$");
            while (dit.hasNext()) {
                dit.next();
                if (tapeRe.match(dit.fileName()).hasMatch())
                    tapeFolders << dit.fileName();
            }
            tapeFolders.sort();
        }
        qDebug() << "[BACKUP] Phase 2: tape folder listing took" << phase2Timer.elapsed() << "ms —"
                 << tapeFolders.size() << "folder(s) found";
        if (tapeFolders.isEmpty()) {
            qDebug() << "[RECONCILE] no tape folders found — nothing to reconcile";
        } else {
            // Only reconcile the current (highest-numbered) folder.
            // Sealed folders are already complete; scanning them every run is wrong.
            tapeFolders = QStringList{ tapeFolders.last() };
        }
        qDebug() << "[RECONCILE] reconciling current folder:" << (tapeFolders.isEmpty() ? "(none)" : tapeFolders.first());

        int    totalOrphansAdded   = 0;
        int    totalOrphansInvalid = 0;
        int    totalOrphansNoSrc   = 0;
        qint64 totalBytesAdded     = 0;
        QElapsedTimer orphanTimer;
        orphanTimer.start();

        m_db->beginTxn();
        int batchCount = 0;

        for (const QString& folder : tapeFolders) {
            if (stopRequested.load()) break;
            QString folderPath = destBase + "/" + folder;

            // ── Step 1: what the done table expects for this folder ──────────
            qint64 doneGzSum  = 0;
            int    doneCount  = 0;
            {
                QString pat = folder + "/%";
                phase2Timer.restart();
                FolderTotals totals;
                {
                    QueryHeartbeat hb(QString("Phase 2: sumDoneBytesForFolder(%1)").arg(folder));
                    totals = m_db->sumDoneBytesForFolder(sourceRoot, pat);
                }
                doneGzSum = totals.gzBytesSum;
                doneCount = totals.count;
                qDebug() << "[BACKUP] Phase 2:" << folder << "sumDoneBytesForFolder took"
                         << phase2Timer.elapsed() << "ms —" << doneCount << "done rows";
            }

            // ── Steps 2-4: walk disk, accumulate sizes, reconcile orphans ────
            qint64 diskBytes         = 0;
            int    diskCount         = 0;
            int    folderOrphChecked = 0;
            int    folderOrphAdded   = 0;
            int    folderOrphInvalid = 0;
            int    folderOrphNoSrc   = 0;
            qint64 lastConsoleLogMs  = 0; // time-based console progress, see below

            {
                // The real file count on disk isn't known until the walk
                // below finishes — doneCount (the done-table row count going
                // in) is not that number and can be far off it (e.g. 0 right
                // after a reset), so using it as the bar's maximum previously
                // left the bar clamped near-empty for the whole walk even as
                // labelOrphanStats reported real, accurate progress. Busy/
                // indeterminate mode (min=max=0) is honest about not knowing
                // the total; set to a real determinate 100% once the walk
                // completes, below.
                QMetaObject::invokeMethod(this, [this] {
                    ui->progressBarOrphan->setMinimum(0);
                    ui->progressBarOrphan->setMaximum(0);
                    ui->labelOrphanStats->setText("");
                }, Qt::QueuedConnection);
            }

            QDirIterator it(folderPath,
                            QDir::Files | QDir::NoSymLinks | QDir::NoDotAndDotDot,
                            QDirIterator::Subdirectories);
            while (it.hasNext()) {
                if (stopRequested.load()) break;
                it.next();
                QString absPath  = it.filePath();
                qint64  fileSize = it.fileInfo().size();
                diskBytes += fileSize;
                ++diskCount;

                // Console progress every ~2s of wall time, keyed to every
                // file walked (not just orphans) — most files in a folder
                // are already known-good and never reach the orphan-count
                // logging below, so without this the console goes silent
                // for the entire walk on a folder with few/no orphans.
                {
                    qint64 nowMs = orphanTimer.elapsed();
                    if (nowMs - lastConsoleLogMs >= 2000) {
                        lastConsoleLogMs = nowMs;
                        qDebug() << "[RECONCILE]" << folder
                                 << "walked:" << diskCount
                                 << "| orphans checked:" << folderOrphChecked
                                 << "added:" << folderOrphAdded
                                 << "deleted:" << folderOrphInvalid
                                 << "| elapsed:" << QString::number(nowMs / 1000.0, 'f', 1) << "s";
                    }
                }

                if (diskCount % 50 == 0) {
                    int dc = diskCount; QString fn = folder;
                    QMetaObject::invokeMethod(this, [this, dc, fn] {
                        ui->labelOrphanStats->setText(
                            QString("%1: %2 files checked").arg(fn).arg(dc));
                    }, Qt::QueuedConnection);
                }

                QString dstRel = QDir(destBase).relativeFilePath(absPath);

                if (knownDst.contains(dstRel)) {
                    // Already in done (verified in Phase 1) — count it, move on
                    double elapsed = orphanTimer.elapsed() / 1000.0;
                    setStatus(QString(
                        "%1  |  disk so far: %2 (%3 files)  done-table: %4 (%5 files)  |  %6 s  |  %7")
                        .arg(folder)
                        .arg(QLocale().formattedDataSize(diskBytes, 2, QLocale::DataSizeSIFormat))
                        .arg(diskCount)
                        .arg(QLocale().formattedDataSize(doneGzSum, 2, QLocale::DataSizeSIFormat))
                        .arg(doneCount)
                        .arg(QString::number(elapsed, 'f', 1))
                        .arg(QFileInfo(absPath).fileName()));
                    continue;
                }

                // ── Orphan file: not recorded in done ────────────────────────
                // Guard the blocking isValidGzip() read below — cheap now that
                // sourceFileSize()/insertOrphanDone() are fast, but this still
                // avoids starting a fresh orphan's file read after a stop.
                if (stopRequested.load()) break;
                QString fileRel = absPath.mid(folderPath.length());
                if (fileRel.startsWith('/')) fileRel = fileRel.mid(1);

                QString srcCandidate;
                qint64  srcSize      = -1;
                bool    wasAlreadyGz = false;

                auto tryLookup = [&](const QString& candidate, bool alreadyGz) -> bool {
                    qint64 sz = 0;
                    bool found = m_db->sourceFileSize(sourceRoot, candidate, &sz);
                    if (found) {
                        srcSize      = sz;
                        wasAlreadyGz = alreadyGz;
                        srcCandidate = candidate;
                    }
                    return found;
                };

                if (fileRel.endsWith(".gz", Qt::CaseInsensitive)) {
                    QString stripped = fileRel.left(fileRel.length() - 3);
                    if (!tryLookup(stripped, false))     // non-gz source, we compressed it
                        tryLookup(fileRel, true);        // source was already .gz
                } else {
                    tryLookup(fileRel, false);
                }

                // Full ISIZE check when we know srcSize; magic-only otherwise
                bool valid = isValidGzip(absPath,
                    (wasAlreadyGz || srcCandidate.isEmpty()) ? -1 : srcSize);

                ++folderOrphChecked;

                if (valid && !srcCandidate.isEmpty()) {
                    double ratio = (srcSize > 0 && !wasAlreadyGz)
                                   ? (double)srcSize / (double)fileSize : 1.0;
                    m_db->insertOrphanDone(sourceRoot, srcCandidate, dstRel,
                        wasAlreadyGz ? fileSize : srcSize, fileSize, ratio);
                    knownDst.insert(dstRel); // prevent double-counting on rescan
                    ++folderOrphAdded;
                    ++totalOrphansAdded;
                    ++batchCount;
                    totalBytesAdded += fileSize;
                    if (batchCount % 200 == 0) {
                        m_db->flushTxn();
                    }
                } else if (!valid) {
                    QFile::remove(absPath);
                    diskBytes -= fileSize; // removed — don't count in delta
                    --diskCount;
                    ++folderOrphInvalid;
                    ++totalOrphansInvalid;
                } else {
                    // valid gzip but source not in files table — leave it
                    ++folderOrphNoSrc;
                    ++totalOrphansNoSrc;
                }

                double elapsed = orphanTimer.elapsed() / 1000.0;
                setStatus(QString(
                    "%1  orphan: %2 added  %3 deleted  %4 no-src  "
                    "|  disk: %5 (%6 files)  done-table: %7 (%8 files)  |  %9 s  |  %10")
                    .arg(folder)
                    .arg(folderOrphAdded).arg(folderOrphInvalid).arg(folderOrphNoSrc)
                    .arg(QLocale().formattedDataSize(diskBytes, 2, QLocale::DataSizeSIFormat))
                    .arg(diskCount)
                    .arg(QLocale().formattedDataSize(doneGzSum, 2, QLocale::DataSizeSIFormat))
                    .arg(doneCount)
                    .arg(QString::number(elapsed, 'f', 1))
                    .arg(QFileInfo(absPath).fileName()));
            }

            // Walk finished. On a genuine finish, diskCount is the folder's
            // real total, so a full determinate bar is accurate. If
            // stopRequested fired mid-walk instead, diskCount is only how
            // far the walk got before being cut off, not the folder's real
            // total (126,312 files vs. the 528 actually walked, e.g.) —
            // there is no true denominator to show a meaningful fraction
            // against, so showing any percentage here (full or partial)
            // would misrepresent a truncated scan as measured progress.
            // Leave it empty/indeterminate instead; labelOrphanStats
            // already reports the real partial counts as plain numbers.
            {
                int  dc      = diskCount;
                bool stopped = stopRequested.load();
                QMetaObject::invokeMethod(this, [this, dc, stopped] {
                    if (stopped) {
                        ui->progressBarOrphan->setMinimum(0);
                        ui->progressBarOrphan->setMaximum(1);
                        ui->progressBarOrphan->setValue(0);
                    } else {
                        ui->progressBarOrphan->setMinimum(0);
                        ui->progressBarOrphan->setMaximum(qMax(dc, 1));
                        ui->progressBarOrphan->setValue(dc);
                    }
                }, Qt::QueuedConnection);
            }

            // ── Per-folder size comparison (the mismatch check) ──────────────
            qint64 delta = diskBytes - doneGzSum;
            QString deltaSign = (delta >= 0) ? "+" : "";
            QString folderSummary = QString(
                "%1  disk=%2 (%3 files)  done-table=%4 (%5 files)  delta=%6%7  "
                "orphans: %8 checked  %9 added  %10 deleted  %11 no-src")
                .arg(folder)
                .arg(QLocale().formattedDataSize(diskBytes, 2, QLocale::DataSizeSIFormat))
                .arg(diskCount)
                .arg(QLocale().formattedDataSize(doneGzSum, 2, QLocale::DataSizeSIFormat))
                .arg(doneCount)
                .arg(deltaSign)
                .arg(QLocale().formattedDataSize(qAbs(delta), 2, QLocale::DataSizeSIFormat))
                .arg(folderOrphChecked)
                .arg(folderOrphAdded)
                .arg(folderOrphInvalid)
                .arg(folderOrphNoSrc);

            qDebug() << "[RECONCILE]" << folderSummary;
            {
                QString fs = folderSummary;
                QMetaObject::invokeMethod(this, [this, fs] {
                    ui->labelOrphanStats->setText(fs);
                }, Qt::QueuedConnection);
            }
        }

        m_db->commitTxn();

        folderDoneCount += totalOrphansAdded;
        double totalS = orphanTimer.elapsed() / 1000.0;
        // stopRequested here means the walk above was cut short — whatever
        // was found is real, but it's a partial pass over the folder, not a
        // completed one, and the wording needs to say so instead of
        // claiming "complete" for a scan that covered a fraction of the
        // folder's actual file count.
        bool reconcileStopped = stopRequested.load();
        qDebug() << (reconcileStopped ? "[RECONCILE] stopped early:" : "[RECONCILE] complete:")
                 << "added=" << totalOrphansAdded
                 << "(" << QLocale().formattedDataSize(totalBytesAdded) << ")"
                 << "deleted=" << totalOrphansInvalid
                 << "no-src=" << totalOrphansNoSrc
                 << "took" << QString::number(totalS, 'f', 1) << "s";

        setStatus(QString(
            "Folder reconcile %1: %2 added to done (%3)  |  %4 deleted (corrupt)  |  %5 no-src  |  %6 s")
            .arg(reconcileStopped ? "(stopped early — partial)" : "complete")
            .arg(totalOrphansAdded)
            .arg(QLocale().formattedDataSize(totalBytesAdded, 2, QLocale::DataSizeSIFormat))
            .arg(totalOrphansInvalid)
            .arg(totalOrphansNoSrc)
            .arg(QString::number(totalS, 'f', 1)));
    }

    if (stopRequested.load()) { setStatus("Stopped."); reenable(); return; }

    // ── Count remaining files + bytes for progress bar ───────────────────
    qint64 totalRemaining    = 0;
    qint64 totalSourceBytes  = 0;
    {
        QElapsedTimer t; t.start();
        {
            QueryHeartbeat hb("countRemaining");
            totalRemaining = m_db->countRemaining(sourceRoot);
        }
        {
            QueryHeartbeat hb("sumRemainingBytes");
            totalSourceBytes = m_db->sumRemainingBytes(sourceRoot);
        }

        qDebug() << "[BACKUP] remaining:" << totalRemaining << "files,"
                 << QLocale().formattedDataSize(totalSourceBytes) << "took" << t.elapsed() << "ms";
    }

    if (totalRemaining == 0) {
        setStatus("All files done — nothing to do.");
        reenable();
        return;
    }

    setProgressMax((int)totalRemaining);
    setProgressVal(0);
    setStatus(QString("Starting — %1 files remaining").arg(QLocale().toString(totalRemaining)));
    qDebug() << "[BACKUP] starting —" << totalRemaining << "files remaining for" << sourceRoot;
    qDebug() << "[BACKUP] sourceRoot exists:" << QDir(sourceRoot).exists();
    {
        QStringList sample = QDir(sourceRoot).entryList(QDir::Files | QDir::Dirs | QDir::NoDotAndDotDot);
        qDebug() << "[BACKUP] sourceRoot top-level entries (first 5):" << sample.mid(0,5);
    }

    // ── Open streaming cursor ─────────────────────────────────────────────
    qDebug() << "[BACKUP] opening streaming cursor…";
    DbBackend::RemainingCursor rem = m_db->openRemainingCursor(sourceRoot);
    qDebug() << "[BACKUP] cursor prepared, waiting for first row…";

    // ── Find current destination folder (last existing, or create first) ──
    QStringList existingFolders = QDir(destBase).entryList(
        QStringList() << (prefix + "_???"), QDir::Dirs, QDir::Name);

    QString currentFolder;
    qint64  currentRawBytes;
    qint64  currentTarEst;
    qint64  currentLtfsEst;
    int     foldersCompleted;      // folders fully written before the current one
    int     currentFolderFileCount = 0; // files written into the current folder this session

    if (existingFolders.isEmpty()) {
        currentFolder      = destBase + "/" + prefix + "_001";
        QDir().mkpath(currentFolder);
        currentRawBytes        = 0;
        currentTarEst          = 1024;
        currentLtfsEst         = 0;
        foldersCompleted      = 0;
        currentFolderFileCount = 0;
        qDebug() << "[BACKUP] created first folder:" << currentFolder;
    } else {
        foldersCompleted = existingFolders.size() - 1;
        currentFolder    = destBase + "/" + existingFolders.last();

        // Scan filesystem for actual folder fill — the DB may be incomplete if a
        // previous run was interrupted before committing all done entries.
        setStatus(QString("Scanning folder fill for %1 (may take a moment on NFS)…")
            .arg(existingFolders.last()));
        FolderSizes fs          = folderSizes(currentFolder, ltfsIndexOverheadBytes);
        currentRawBytes         = fs.actualBytes;
        currentTarEst           = fs.tarBytes;
        currentLtfsEst          = fs.ltfsBytes;
        currentFolderFileCount = 0; // counts files added this session; prior files not re-counted

        qDebug() << "[BACKUP] resuming — folders completed:" << foldersCompleted
                 << "current:" << currentFolder
                 << "tar:"  << QLocale().formattedDataSize(currentTarEst)
                 << "ltfs:" << QLocale().formattedDataSize(currentLtfsEst);
    }
    HardLimitModel activeModel = activeHardLimitModel.load();
    qint64 activeBytes = (activeModel == HardLimitModel::Ltfs) ? currentLtfsEst : currentTarEst;
    setTarEst(currentTarEst, currentLtfsEst, currentRawBytes, activeModel);
    setFolderProgress(currentRawBytes, activeBytes, activeModel, foldersCompleted + 1, foldersCompleted + 1, currentFolderFileCount);

    if (existingFolders.isEmpty()) {
        setStatus(QString("Starting fresh — first folder: %1  |  %2 files to process")
            .arg(QDir(currentFolder).dirName())
            .arg(QLocale().toString(totalRemaining)));
    } else {
        setStatus(QString("Resuming — folder: %1  |  filled: %2 tar / %3 LTFS  |  folders done: %4")
            .arg(QDir(currentFolder).dirName())
            .arg(QLocale().formattedDataSize(currentTarEst, 2, QLocale::DataSizeSIFormat))
            .arg(QLocale().formattedDataSize(currentLtfsEst, 2, QLocale::DataSizeSIFormat))
            .arg(foldersCompleted));
    }
    {
        QString statsInit = QString("Previously done: %1 files  |  Remaining: %2  |  Current folder: %3  (%4 filled)")
            .arg(QLocale().toString((qint64)folderDoneCount))
            .arg(QLocale().toString(totalRemaining))
            .arg(QDir(currentFolder).dirName())
            .arg(QLocale().formattedDataSize(activeBytes, 2, QLocale::DataSizeSIFormat));
        QMetaObject::invokeMethod(this, [this, statsInit] {
            ui->labelStats->setText(statsInit);
        }, Qt::QueuedConnection);
    }

    // Returns the next numbered folder path (e.g. tape_003 → tape_004)
    auto nextFolderPath = [&]() -> QString {
        QString name = QDir(currentFolder).dirName();
        int sep = name.lastIndexOf('_');
        int num = (sep >= 0) ? name.mid(sep + 1).toInt() : 0;
        return destBase + "/" + prefix + "_" + QString("%1").arg(num + 1, 3, 10, QChar('0'));
    };

    // ── Thread count and RAM budget ───────────────────────────────────────
    int nThreads = qMax(1, QThread::idealThreadCount() - 1);
    QThreadPool compressPool;
    compressPool.setMaxThreadCount(nThreads);

    QThreadPool readPool;
    readPool.setMaxThreadCount(6);

    qint64 ramLimit = 64LL * 1024 * 1024 * 1024;
    {
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
    }
    qDebug() << "[BACKUP] threads:" << nThreads
             << "ramLimit:" << QLocale().formattedDataSize(ramLimit,2,QLocale::DataSizeSIFormat);

    // ── Shared pipeline data ──────────────────────────────────────────────
    struct WriteItem {
        int        seq         = 0;
        QString    rel;
        QString    gzName;
        qint64     srcSize     = 0;
        bool       alreadyGz   = false;
        QByteArray gzData;
        bool       ok          = false;
        qint64     read_ms     = 0;
        qint64     compress_ms = 0;
        bool       alreadyReported = false;  // true if addFailed() was already called for this seq
        QString    failReason;               // specific reason, used instead of a generic message
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
    std::atomic<int>        filesInCompressPipeline{0};  // queued in pool + actively compressing
    std::atomic<int>        filesCompressedAtomic{0};   // total files that finished compression
    std::atomic<int>        filesWriteFailed{0};
    std::atomic<int>        filesBeingRead{0};
    std::atomic<int>        filesSkippedStop{0};  // bailed before reading started, due to stopRequested

    struct FailedItem { QString srcPath; qint64 srcSize; bool alreadyGz; QString reason; };
    QMutex                  failedMutex;
    std::vector<FailedItem> failedItems;
    auto addFailed = [&](const QString& rel, qint64 srcSize, bool alreadyGz,
                         const QString& reason, const QString& destEvidence = {}) {
        QString srcPath = sourceRoot + "/" + rel;
        QFileInfo si(srcPath);
        QString msg = QString("[FAILED] %1\n"
                              "  src    : %2\n"
                              "  exists : %3  |  size on disk: %4  |  readable: %5")
            .arg(reason).arg(srcPath)
            .arg(si.exists() ? "yes" : "NO")
            .arg(si.size())
            .arg(si.isReadable() ? "yes" : "NO");
        if (!destEvidence.isEmpty()) msg += "\n  dest   : " + destEvidence;
        qDebug().noquote() << msg;
        QMutexLocker lk(&failedMutex);
        failedItems.push_back({srcPath, srcSize, alreadyGz, reason});
        filesWriteFailed.store((int)failedItems.size());
    };
    // filesInWriteMap and filesBeingWritten are MainWindow members (needed by shutdown)
    filesInWriteMap.store(0);
    filesBeingWritten.store(0);
    forceKillWriter.store(false);
    std::atomic<int>        filesProcessedAtomic{0};
    std::atomic<qint64>     bytesWrittenAtomic{0};
    std::atomic<qint64>     bytesReadAtomic{0};

    QElapsedTimer totalTimer;
    totalTimer.start();
    qint64 totalBytesRead = 0, totalBytesWritten = 0;
    int    filesProcessed = 0, filesCopied = 0, filesGzipped = 0;

    // ── Stage 3: Writer thread ────────────────────────────────────────────
    // Per-file [ROTATE-CHECK]/[WRITE]/[SIZE] logging is throttled to once
    // every ~2s of wall time on the routine success path — same convention
    // as the Phase 2 reconcile console log — so a run of hundreds of
    // thousands of files doesn't flood stdout. Rotations, overwrites, and
    // failures always log regardless of the throttle, since those are
    // exactly what a post-hoc analysis needs.
    QElapsedTimer writerLogTimer; writerLogTimer.start();
    qint64 lastWriterLogMs = -2000; // force the first file to log
    QThread* writerThread = QThread::create([&]() {
        while (true) {
            WriteItem w;
            {
                QMutexLocker lk(&writeMutex);
                while (writeMap.count(nextWriteSeq) == 0 &&
                       !(readerDone.load() && writeMap.empty()) &&
                       !forceKillWriter.load()) {
                    writeCV.wait(&writeMutex, 200);
                }
                if ((writeMap.empty() && readerDone.load()) || forceKillWriter.load()) break;
                auto it = writeMap.find(nextWriteSeq);
                if (it == writeMap.end()) continue;
                w = std::move(it->second);
                writeMap.erase(it);
            }
            filesInWriteMap.fetch_add(-1);
            filesBeingWritten.fetch_add(1);  // entering write stage (no gap in counting)

            if (!w.ok) {
                filesBeingWritten.fetch_add(-1);
                if (!w.alreadyReported) {
                    // Read failures already called addFailed() and never added
                    // to ramInFlight — only compress failures need this here.
                    addFailed(w.rel, w.srcSize, w.alreadyGz,
                              w.failReason.isEmpty() ? "compression failed" : w.failReason);
                    ramInFlight.fetch_add(-w.srcSize);
                    { QMutexLocker lk(&writeMutex); ramCV.wakeAll(); }
                }
                ++nextWriteSeq;
                continue;
            }

            // Folder-full check (writer decides because gz size is now known).
            // Gated on whichever model the user has selected as the hard
            // limit — both models' running totals are kept accurate
            // regardless, so this can be flipped mid-run safely.
            //
            // If a file already exists at this path in the current folder
            // (reprocessed after its done-row was lost, e.g. via orphan
            // merge), writing it again doesn't grow the folder by the new
            // file's full size — it replaces what's already counted. The
            // check and the running-total update below both have to net
            // out the old contribution first, or the tracked total runs
            // ahead of real disk usage and the folder seals early.
            QString prospectiveDestFile = currentFolder + "/" + w.gzName;
            QFileInfo prospectiveInfo(prospectiveDestFile);
            bool   prospectiveExists = prospectiveInfo.exists();
            qint64 prospectiveOldSize = prospectiveExists ? prospectiveInfo.size() : 0;
            qint64 oldTarContribution  = prospectiveExists ? tarFileBytes(prospectiveOldSize) : 0;
            qint64 oldLtfsContribution = prospectiveExists
                ? ltfsFileBytes(prospectiveOldSize, ltfsIndexOverheadBytes) : 0;

            qint64 gzSizeForCheck = (qint64)w.gzData.size();
            qint64 nextTarBytes   = tarFileBytes(gzSizeForCheck);
            qint64 nextLtfsBytes  = ltfsFileBytes(gzSizeForCheck, ltfsIndexOverheadBytes);
            HardLimitModel rotateCheckModel = activeHardLimitModel.load();
            qint64 projectedTotal = (rotateCheckModel == HardLimitModel::Ltfs)
                ? (currentLtfsEst - oldLtfsContribution + nextLtfsBytes)
                : (currentTarEst  - oldTarContribution  + nextTarBytes);
            bool wouldExceed = projectedTotal > maxFolderSize;

            qint64 nowLogMs = writerLogTimer.elapsed();
            bool logThisFile = wouldExceed || prospectiveExists
                || (nowLogMs - lastWriterLogMs >= 2000);
            if (logThisFile) lastWriterLogMs = nowLogMs;

            if (logThisFile) qDebug() << "[ROTATE-CHECK] seq=" << w.seq << "folder=" << currentFolder
                     << "model=" << (rotateCheckModel == HardLimitModel::Ltfs ? "LTFS" : "Tar")
                     << "currentTarEst=" << currentTarEst << "currentLtfsEst=" << currentLtfsEst
                     << "overwrite=" << prospectiveExists << "oldTar=" << oldTarContribution
                     << "oldLtfs=" << oldLtfsContribution
                     << "nextTarBytes=" << nextTarBytes << "nextLtfsBytes=" << nextLtfsBytes
                     << "projectedTotal=" << projectedTotal << "maxFolderSize=" << maxFolderSize
                     << "wouldExceed=" << wouldExceed;
            if (wouldExceed) {
                ++foldersCompleted;
                QString sealedFolder = currentFolder;
                qint64  sealedTar    = currentTarEst;
                qint64  sealedLtfs   = currentLtfsEst;
                qint64  sealedRaw    = currentRawBytes;
                int     sealedFiles  = currentFolderFileCount;
                currentFolder = nextFolderPath();
                QDir().mkpath(currentFolder);
                currentRawBytes        = 0;
                currentTarEst          = 1024;
                currentLtfsEst         = 0;
                currentFolderFileCount = 0;
                // Rotated to a fresh, just-created folder — the overwrite
                // figures above were computed against the OLD folder and no
                // longer apply to where this file is actually landing now.
                prospectiveExists     = false;
                prospectiveOldSize    = 0;
                oldTarContribution    = 0;
                oldLtfsContribution   = 0;
                qDebug() << "[BACKUP] SEALED folder:" << sealedFolder
                         << "| files=" << sealedFiles
                         << "| raw=" << sealedRaw << "tar=" << sealedTar << "ltfs=" << sealedLtfs
                         << "| new folder:" << currentFolder
                         << "(limit model:" << (rotateCheckModel == HardLimitModel::Ltfs ? "LTFS" : "Tar") << ")";
                setFolderProgress(0, 0, rotateCheckModel, foldersCompleted+1, foldersCompleted+1, 0);
            }

            // Ensure subdir exists
            QString relDir = QFileInfo(w.gzName).path();
            if (!relDir.isEmpty() && relDir != ".")
                QDir().mkpath(currentFolder + "/" + relDir);

            QString destFile = currentFolder + "/" + w.gzName;
            qint64  gzSize   = (qint64)w.gzData.size();

            // Write (filesBeingWritten already incremented above).
            //
            // The one and only success signal is: the bytes were handed to
            // write(), and close() confirms the OS/NFS actually committed
            // them — nothing is re-opened or re-read afterward to "double
            // check". On NFS, write() returning success only means the
            // client's page cache accepted the data; a server-side commit
            // failure (quota, network blip, stale handle) is only reported
            // via close()'s error state, which is why that's checked
            // explicitly instead of being discarded.
            QElapsedTimer wt; wt.start();
            qint64  bytesWritten = -1;
            bool    closeOk      = false;
            QString closeErr;
            {
                QFile outF(destFile);
                if (outF.open(QIODevice::WriteOnly)) {
                    bytesWritten = outF.write(w.gzData);
                    outF.close();
                    closeOk = (outF.error() == QFile::NoError);
                    if (!closeOk) closeErr = outF.errorString();
                } else {
                    closeErr = outF.errorString();
                }
            }
            qint64 write_ms = wt.elapsed();
            bool writeOk = closeOk && (bytesWritten == gzSize);
            if (logThisFile || !writeOk) qDebug() << "[WRITE] seq=" << w.seq << "dest=" << destFile
                     << "gzSize=" << gzSize << "bytesWritten=" << bytesWritten
                     << "closeOk=" << closeOk << (closeErr.isEmpty() ? "" : closeErr)
                     << "writeOk=" << writeOk << write_ms << "ms";
            if (!writeOk) {
                filesBeingWritten.fetch_add(-1);
                QString destEvidence = QString("%1  bytes written: %2  (expected %3)  close error: %4")
                    .arg(destFile).arg(bytesWritten).arg(gzSize)
                    .arg(closeErr.isEmpty() ? "none" : closeErr);
                QFile::remove(destFile);
                addFailed(w.rel, w.srcSize, w.alreadyGz, "write failed", destEvidence);
                ramInFlight.fetch_add(-w.srcSize);
                { QMutexLocker lk(&writeMutex); ramCV.wakeAll(); }
                ++nextWriteSeq;
                continue;
            }
            filesBeingWritten.fetch_add(-1);  // success: leaving write stage

            // Update tracking — the gzip size was already known before the
            // write; a confirmed write+close is success, full stop.
            //
            // Net out whatever this write replaced (prospectiveOldSize/
            // oldTarContribution/oldLtfsContribution, computed above against
            // the same destFile before the rotate check) instead of adding
            // the new size on top of a total that may already include the
            // old copy of this exact file.
            qint64 tarAdd  = tarFileBytes(gzSize)  - oldTarContribution;
            qint64 ltfsAdd = ltfsFileBytes(gzSize, ltfsIndexOverheadBytes) - oldLtfsContribution;
            currentRawBytes += gzSize - prospectiveOldSize;
            currentTarEst   += tarAdd;
            currentLtfsEst  += ltfsAdd;
            ++currentFolderFileCount;
            totalBytesRead     += w.srcSize;
            totalBytesWritten  += gzSize;
            if (w.alreadyGz) ++filesCopied; else ++filesGzipped;
            if (logThisFile) qDebug() << "[SIZE] seq=" << w.seq << "folder=" << currentFolder
                     << "+gz=" << gzSize << "overwrote=" << prospectiveExists
                     << "oldSize=" << prospectiveOldSize
                     << "tar+=" << tarAdd << "ltfs+=" << ltfsAdd
                     << "-> tarTotal=" << currentTarEst << "ltfsTotal=" << currentLtfsEst
                     << "filesThisFolder=" << currentFolderFileCount;

            // INSERT into done (9 params)
            double ratio = (gzSize > 0 && !w.alreadyGz) ? (double)w.srcSize / gzSize : 1.0;
            QString dstRelPath = QDir(destBase).relativeFilePath(destFile);
            DoneRow doneRow;
            doneRow.source_root = sourceRoot;
            doneRow.src          = w.rel;
            doneRow.dst           = dstRelPath;
            doneRow.bytes         = w.srcSize;
            doneRow.gz_bytes      = gzSize;
            doneRow.ratio         = ratio;
            doneRow.read_ms       = w.read_ms;
            doneRow.compress_ms   = w.compress_ms;
            doneRow.write_ms      = write_ms;
            m_db->insertDone(doneRow);
            m_db->flushTxn();
            if (logThisFile) qDebug() << "[DONE] seq=" << w.seq << "src=" << w.rel << "dst=" << dstRelPath
                     << "bytes=" << w.srcSize << "gz_bytes=" << gzSize << "committed to done table";

            statWriteMs.fetch_add(write_ms);
            ++filesProcessed;
            filesProcessedAtomic.store(filesProcessed);
            bytesWrittenAtomic.store(totalBytesWritten);
            bytesReadAtomic.store(totalBytesRead);

            // Release RAM budget
            ramInFlight.fetch_add(-w.srcSize);
            { QMutexLocker lk(&writeMutex); ramCV.wakeAll(); }

            // Status: what was just written
            setStatus(QString("Writing [%1/%2]: %3  (%4 → %5 gz, ratio %6x, %7 ms write)")
                .arg(filesProcessed).arg(totalRemaining)
                .arg(QFileInfo(w.rel).fileName())
                .arg(QLocale().formattedDataSize(w.srcSize, 2, QLocale::DataSizeSIFormat))
                .arg(QLocale().formattedDataSize(gzSize, 2, QLocale::DataSizeSIFormat))
                .arg(QString::number(ratio, 'f', 2))
                .arg(write_ms));

            {
                HardLimitModel m = activeHardLimitModel.load();
                qint64 activeBytesNow = (m == HardLimitModel::Ltfs) ? currentLtfsEst : currentTarEst;
                setTarEst(currentTarEst, currentLtfsEst, currentRawBytes, m);
                setFolderProgress(currentRawBytes, activeBytesNow, m, foldersCompleted+1, foldersCompleted+1, currentFolderFileCount);
            }

            if (filesProcessed % 1000 == 0) {
                double elapsed     = totalTimer.elapsed() / 1000.0;
                double avgRead     = statReadMs.load()     / (double)filesProcessed;
                double avgCompress = statCompressMs.load() / (double)filesProcessed;
                double avgWrite    = statWriteMs.load()    / (double)filesProcessed;
                qDebug() << "[BACKUP]" << filesProcessed << "/" << totalRemaining
                         << "| read_avg="     << QString::number(avgRead/1000,'f',2)     << "s"
                         << "| compress_avg=" << QString::number(avgCompress/1000,'f',2) << "s"
                         << "| write_avg="    << QString::number(avgWrite/1000,'f',2)    << "s"
                         << "| RAM_inflight=" << QLocale().formattedDataSize(ramInFlight.load())
                         << "| writeMap_depth_max=" << writeMapDepthMax.load()
                         << "| reader_stalls="      << readerStalls.load()
                         << "| elapsed=" << QString::number(elapsed,'f',0) << "s";
            }

            ++nextWriteSeq;
        }

        // If force-killed, free RAM for any items still in writeMap
        if (forceKillWriter.load()) {
            QMutexLocker lk(&writeMutex);
            for (auto& [seq, item] : writeMap) {
                ramInFlight.fetch_add(-item.srcSize);
                filesInWriteMap.fetch_add(-1);
            }
            writeMap.clear();
            ramCV.wakeAll();
        }

        m_db->commitTxn();
        m_db->closeThreadConnection();   // this thread (writerThread) is about to exit
    });
    currentBackupPhase.store(kBackupPhasePipeline);
    qDebug() << "[BACKUP] ── Phase 3: pipeline starting ───────────────────────────";
    qDebug() << "[BACKUP] remaining:" << totalRemaining
             << "| source:" << (srcIsNFS ? "NFS" : "local")
             << "| dest:" << (destIsNFS ? "NFS" : "local")
             << "| RAM limit:" << QLocale().formattedDataSize(ramLimit, 2, QLocale::DataSizeSIFormat)
             << "| read threads:" << readPool.maxThreadCount()
             << "| compress threads:" << compressPool.maxThreadCount();

    // Start QTimer on main thread to poll atomics every 250ms
    int seq = 0;   // declared here (not at the reader loop) so the pipeline
                   // timer lambda below can capture it by reference
    int pipelineLogTick = 0;
    QMetaObject::invokeMethod(this, [&] {
        m_pipelineTimer = new QTimer(this);
        connect(m_pipelineTimer, &QTimer::timeout, this, [&] {
            int reading     = filesBeingRead.load();
            int compressing = filesInCompressPipeline.load();   // queued in pool + active threads
            int queued      = filesInWriteMap.load();           // waiting for writer
            int activeWrite = filesBeingWritten.load();         // 0 or 1
            int writing     = queued + activeWrite;             // ALL files in write stage

            // Full-pipeline sanity check, logged every ~2s: every seq submitted
            // must be in exactly one of {reading, compressing, queued, writing,
            // succeeded, genuinely-failed, stop-skipped}. If the accounted total
            // doesn't equal the submitted total, a counter is provably broken —
            // this makes that visible instead of assumed.
            if (++pipelineLogTick % 8 == 0) {
                int submitted   = seq;   // racy plain-int read, diagnostic only
                int succeeded   = filesProcessedAtomic.load();
                int failed      = filesWriteFailed.load();
                int stopSkipped = filesSkippedStop.load();
                int inPipeline  = reading + compressing + queued + activeWrite;
                int accounted   = inPipeline + succeeded + failed + stopSkipped;
                qDebug() << "[PIPELINE]" << "submitted=" << submitted
                         << "| reading=" << reading << "compressing=" << compressing
                         << "queued=" << queued << "writing=" << activeWrite
                         << "| succeeded=" << succeeded << "failed=" << failed
                         << "stopSkipped=" << stopSkipped
                         << "| accounted=" << accounted
                         << (accounted == submitted ? "OK" : "MISMATCH — a counter is wrong, diff=")
                         << (accounted == submitted ? 0 : submitted - accounted);
            }
            // reading + compressing + writing = total files in RAM
            int fp          = qMax(1, filesProcessedAtomic.load());
            int fc          = qMax(1, filesCompressedAtomic.load());
            qint64 avgRd    = statReadMs.load()     / fc;
            qint64 avgCp    = statCompressMs.load() / fc;
            qint64 avgWr    = statWriteMs.load()    / fp;

            ui->labelReadCount->setText(QString::number(reading));
            ui->labelReadDetail->setText(
                QString("%1 threads active  ·  avg %2 ms  ·  %3")
                    .arg(readPool.activeThreadCount()).arg(avgRd).arg(srcIsNFS ? "NFS" : "local"));

            ui->labelCompressCount->setText(QString::number(compressing));
            ui->labelCompressDetail->setText(
                QString("%1 threads active  ·  avg %2 ms")
                    .arg(compressPool.activeThreadCount()).arg(avgCp));

            // Write Queue: how many are waiting (sub-info)
            ui->labelQueueCount->setText(QString::number(queued));
            ui->labelQueueDetail->setText(
                QLocale().formattedDataSize(ramInFlight.load(), 2, QLocale::DataSizeSIFormat)
                + " in RAM");

            // Writing: ALL files in write stage (waiting + active)
            ui->labelWriteCount->setText(QString::number(writing));
            ui->labelWriteDetail->setText(
                QString("%1 active  ·  avg %2 ms  ·  %3")
                    .arg(activeWrite).arg(avgWr).arg(destIsNFS ? "NFS" : "local"));
            bool bottleneck = destIsNFS && avgWr > avgCp * 3 && avgWr > 500;
            ui->labelWriteDetail->setStyleSheet(bottleneck ? "color:#f38ba8;" : "");

            int vv = filesProcessedAtomic.load();
            int mx = (int)totalRemaining;
            double pct = mx > 0 ? 100.0 * vv / mx : 0.0;
            ui->progressBarBackup->setFormat(
                QString("%1 / %2 files  (%3%)  —  %4 / %5")
                    .arg(vv).arg(mx)
                    .arg(QString::number(pct, 'f', 2))
                    .arg(QLocale().formattedDataSize(bytesWrittenAtomic.load(), 2, QLocale::DataSizeSIFormat))
                    .arg(QLocale().formattedDataSize(totalSourceBytes, 2, QLocale::DataSizeSIFormat)));
            ui->progressBarBackup->setMaximum(mx);
            ui->progressBarBackup->setValue(vv);

            double elapsed = totalTimer.elapsed() / 1000.0;
            double fps  = (elapsed > 0 && vv > 0) ? vv / elapsed : 0;
            double mbps = elapsed > 0 ? bytesWrittenAtomic.load() / 1e6 / elapsed : 0;
            double ratioX = bytesWrittenAtomic.load() > 0
                ? (double)bytesReadAtomic.load() / bytesWrittenAtomic.load() : 0;
            ui->labelStats->setText(
                QString("done: %1  |  %2 f/s  |  %3 MB/s  |  ratio: %4x"
                        "  |  ETA: %5 min  |  RAM: %6  |  failed: %7  |  folder: %8")
                    .arg(QLocale().toString((qint64)(folderDoneCount + vv)))
                    .arg(QString::number(fps, 'f', 2))
                    .arg(QString::number(mbps, 'f', 1))
                    .arg(QString::number(ratioX, 'f', 2))
                    .arg(fps > 0 ? QString::number((mx - vv) / fps / 60, 'f', 0) : "—")
                    .arg(QLocale().formattedDataSize(ramInFlight.load(), 2, QLocale::DataSizeSIFormat))
                    .arg(filesWriteFailed.load())
                    .arg(QDir(currentFolder).dirName()));

            if (stopRequested.load()) {
                int inFlight = reading + compressing + writing;
                ui->labelStopWarning->setText(
                    QString("⚠  STOPPING — %1 file%2 still in pipeline"
                            "  (%3 reading  ·  %4 compressing  ·  %5 writing)"
                            "  — DO NOT CLOSE")
                        .arg(inFlight).arg(inFlight == 1 ? "" : "s")
                        .arg(reading).arg(compressing).arg(writing));
                ui->labelStopWarning->setVisible(true);
            }
        });
        m_pipelineTimer->start(250);
    }, Qt::BlockingQueuedConnection);

    writerThread->start();
    qDebug() << "[BACKUP] writer thread started";

    // ── Stage 1: Reader loop ──────────────────────────────────────────────
    qDebug() << "[BACKUP] reader loop starting";
    m_db->beginTxn();   // wraps the streaming cursor in a consistent read snapshot

    QString rel;
    qint64 srcSize = 0;
    while (rem.next(rel, srcSize)) {
        if (stopRequested.load()) break;

        const QString srcPath = sourceRoot + "/" + rel;
        bool alreadyGz = QFileInfo(rel).suffix().toLower() == "gz";
        QString gzName = rel + (alreadyGz ? "" : ".gz");

        // Block until RAM budget allows loading this file, and until the read
        // pool has room — otherwise the main loop races through the whole
        // remaining file list and queues it all at once (filesBeingRead would
        // show the entire backlog instead of the 2-3 files actually reading).
        auto readBacklogFull = [&] {
            return filesBeingRead.load() >= readPool.maxThreadCount();
        };
        {
            QMutexLocker lk(&writeMutex);
            if (ramInFlight.load() + srcSize > ramLimit || readBacklogFull()) {
                readerStalls.fetch_add(1);
                while ((ramInFlight.load() + srcSize > ramLimit || readBacklogFull())
                       && !stopRequested.load())
                    ramCV.wait(&writeMutex, 200);
            }
        }
        if (stopRequested.load()) break;

        // Stage 1 — Reading: increment on entry, decrement on exit.
        // The actual read now runs on readPool (2-3 threads) instead of
        // inline here, so multiple files can be read concurrently.
        filesBeingRead.fetch_add(1);
        int capturedSeq = seq;
        readPool.start([=, &compressPool, &writeMutex, &writeCV, &writeMap,
                        &writeMapDepthMax, &statReadMs, &statCompressMs,
                        &filesInCompressPipeline, &filesCompressedAtomic,
                        &filesBeingRead, &ramInFlight, &filesSkippedStop]() mutable {
            // A read failure still has to occupy its seq slot in writeMap —
            // otherwise the writer thread waits forever for a seq that will
            // never arrive, while later (successful) files pile up unwritten.
            auto pushReadFailure = [&] {
                WriteItem w;
                w.seq             = capturedSeq;
                w.rel             = rel;
                w.gzName          = gzName;
                w.srcSize         = 0;   // never entered ramInFlight — nothing to release
                w.alreadyGz       = alreadyGz;
                w.ok              = false;
                w.alreadyReported = true;   // addFailed() already called below
                filesInWriteMap.fetch_add(1);   // enter Write Queue, same as a successful compress
                QMutexLocker lk(&writeMutex);
                writeMap[capturedSeq] = std::move(w);
                writeCV.wakeAll();
            };

            if (this->stopRequested.load()) {
                filesSkippedStop.fetch_add(1);
                qDebug() << "[READ] seq=" << capturedSeq << "skipped — stopRequested"
                         << "(total skipped:" << filesSkippedStop.load() << ")";
                filesBeingRead.fetch_add(-1);
                pushReadFailure();
                return;
            }

            qDebug() << "[READ] seq=" << capturedSeq << "starting:" << rel
                     << "(" << QLocale().formattedDataSize(srcSize, 2, QLocale::DataSizeSIFormat) << ")";
            QElapsedTimer readTimer; readTimer.start();
            QFile inF(srcPath);
            if (!inF.open(QIODevice::ReadOnly)) {
                filesBeingRead.fetch_add(-1);
                addFailed(rel, srcSize, alreadyGz, "cannot open source file");
                pushReadFailure();
                return;
            }

            // Read in chunks instead of one blocking readAll() call, so actual
            // throughput (or a true stall at a specific byte offset) is visible
            // WHILE the read is happening, not just inferred after it finally
            // returns. Each QFile::read() below is itself still a blocking NFS
            // call and can't be interrupted mid-chunk, but a 64 MB granularity
            // means a genuine stall shows up within seconds instead of minutes.
            const qint64 chunkSize = 64LL << 20;
            QByteArray inputData;
            inputData.reserve((int)qMin(srcSize, (qint64)INT_MAX));
            QByteArray chunk(chunkSize, Qt::Uninitialized);
            qint64 totalRead   = 0;
            qint64 lastLogMs   = 0;
            qint64 lastLogByte = 0;
            bool   readError   = false;
            while (true) {
                qint64 n = inF.read(chunk.data(), chunkSize);
                if (n < 0) { readError = true; break; }
                if (n == 0) break;   // EOF
                inputData.append(chunk.constData(), (int)n);
                totalRead += n;
                qint64 nowMs = readTimer.elapsed();
                if (nowMs - lastLogMs >= 2000) {
                    double curMBs = ((totalRead - lastLogByte) / 1e6) / ((nowMs - lastLogMs) / 1000.0);
                    double avgMBs = nowMs > 0 ? (totalRead / 1e6) / (nowMs / 1000.0) : 0;
                    qDebug() << "[READ] seq=" << capturedSeq << "progress:"
                             << QLocale().formattedDataSize(totalRead, 2, QLocale::DataSizeSIFormat)
                             << "/" << QLocale().formattedDataSize(srcSize, 2, QLocale::DataSizeSIFormat)
                             << "| current:" << QString::number(curMBs, 'f', 1) << "MB/s"
                             << "| avg:" << QString::number(avgMBs, 'f', 1) << "MB/s";
                    lastLogMs   = nowMs;
                    lastLogByte = totalRead;
                }
            }
            inF.close();
            qint64 rmx = readTimer.elapsed();
            if (readError) {
                filesBeingRead.fetch_add(-1);
                addFailed(rel, srcSize, alreadyGz,
                          QString("read error after %1 of %2")
                              .arg(QLocale().formattedDataSize(totalRead))
                              .arg(QLocale().formattedDataSize(srcSize)));
                pushReadFailure();
                return;
            }
            if (inputData.isEmpty()) {
                filesBeingRead.fetch_add(-1);
                addFailed(rel, srcSize, alreadyGz, "source file empty");
                pushReadFailure();
                return;
            }
            double doneAvgMBs = rmx > 0 ? (totalRead / 1e6) / (rmx / 1000.0) : 0;
            qDebug() << "[READ] seq=" << capturedSeq << "done in" << rmx << "ms"
                     << "| avg" << QString::number(doneAvgMBs, 'f', 1) << "MB/s";

            // File fully in RAM — leave Read stage, enter Compress stage
            ramInFlight.fetch_add(srcSize);
            filesBeingRead.fetch_add(-1);
            filesInCompressPipeline.fetch_add(1);

            // Submit compression task (captures per-file state by value, shared structures by ref)
            compressPool.start([=, &compressPool, &writeMutex, &writeCV, &writeMap,
                                &writeMapDepthMax, &statReadMs, &statCompressMs,
                                &filesInCompressPipeline, &filesCompressedAtomic]() mutable {
                WriteItem w;
                w.seq        = capturedSeq;
                w.rel        = rel;
                w.gzName     = gzName;
                w.srcSize    = srcSize;
                w.alreadyGz  = alreadyGz;
                w.read_ms    = rmx;
                w.ok         = false;

                QElapsedTimer ct; ct.start();
                if (alreadyGz) {
                    w.gzData = inputData;
                    w.ok     = true;
                } else {
                    z_stream zs = {};
                    if (deflateInit2(&zs, Z_BEST_SPEED, Z_DEFLATED,
                                     15+16, 8, Z_DEFAULT_STRATEGY) == Z_OK) {
                        QByteArray out;
                        out.reserve(inputData.size() / 2);
                        QByteArray outBuf(4 << 20, 0);
                        bool zerr = false;
                        zs.next_in  = (Bytef*)inputData.constData();
                        zs.avail_in = (uInt)inputData.size();
                        int ret = Z_OK;
                        do {
                            if (this->stopRequested.load()) {
                                zerr = true;
                                w.failReason = "aborted: stopRequested during deflate";
                                break;
                            }
                            zs.next_out  = (Bytef*)outBuf.data();
                            zs.avail_out = (uInt)outBuf.size();
                            ret = deflate(&zs, zs.avail_in == 0 ? Z_FINISH : Z_NO_FLUSH);
                            if (ret == Z_STREAM_ERROR) {
                                zerr = true;
                                w.failReason = "Z_STREAM_ERROR from deflate()";
                                break;
                            }
                            out.append(outBuf.constData(), outBuf.size() - (int)zs.avail_out);
                        } while (ret != Z_STREAM_END);
                        deflateEnd(&zs);
                        if (!zerr) { w.gzData = out; w.ok = true; }
                    } else {
                        w.failReason = "deflateInit2 failed";
                    }
                }
                w.compress_ms = ct.elapsed();

                double rspeed = rmx > 0 ? (srcSize / 1e6) / (rmx / 1000.0) : 0;
                double cspeed = w.compress_ms > 0 ? (srcSize / 1e6) / (w.compress_ms / 1000.0) : 0;
                double ratio  = (w.gzData.size() > 0 && !alreadyGz)
                                ? (double)srcSize / w.gzData.size() : 1.0;

                if (w.ok && !alreadyGz) {
                    setStatus(QString("Compressed [%1/%2]: %3  (%4 → %5, %6x, %7 ms @ %8 MB/s)")
                        .arg(capturedSeq).arg(totalRemaining)
                        .arg(QFileInfo(rel).fileName())
                        .arg(QLocale().formattedDataSize(srcSize, 2, QLocale::DataSizeSIFormat))
                        .arg(QLocale().formattedDataSize((qint64)w.gzData.size(), 2, QLocale::DataSizeSIFormat))
                        .arg(QString::number(ratio, 'f', 2))
                        .arg(w.compress_ms)
                        .arg(QString::number(cspeed, 'f', 1)));
                } else if (!w.ok) {
                    setStatus(QString("Compress FAILED [%1/%2]: %3")
                        .arg(capturedSeq).arg(totalRemaining)
                        .arg(QFileInfo(rel).fileName()));
                }

                statReadMs.fetch_add(rmx);
                statCompressMs.fetch_add(w.compress_ms);
                filesCompressedAtomic.fetch_add(1);     // total compressed (for correct avg)
                filesInCompressPipeline.fetch_add(-1);  // leave Compress stage
                filesInWriteMap.fetch_add(1);           // enter Write Queue
                QMutexLocker lk(&writeMutex);
                writeMap[capturedSeq] = std::move(w);
                int depth = (int)writeMap.size();
                if (depth > writeMapDepthMax.load()) writeMapDepthMax.store(depth);
                writeCV.wakeAll();
            });
        });

        ++seq;
        setStatus(QString("Reading [%1/%2]: %3  (%4)")
            .arg(seq).arg(totalRemaining).arg(rel)
            .arg(QLocale().formattedDataSize(srcSize,2,QLocale::DataSizeSIFormat)));
    }

    qDebug() << "[BACKUP] reader loop done — seq:" << seq
             << "| draining read pool (" << filesBeingRead.load() << "still reading )…";
    // Drain all reads first (each read may still be about to submit a compress
    // task), then drain compression tasks before signalling writer. Poll with
    // a heartbeat instead of a silent waitForDone() so a long-but-progressing
    // drain is visibly different from one that's actually stuck.
    {
        QElapsedTimer dt; dt.start();
        while (readPool.activeThreadCount() > 0) {
            qDebug() << "[BACKUP] draining read pool —" << readPool.activeThreadCount()
                     << "threads active," << filesBeingRead.load() << "files reading, elapsed"
                     << QString::number(dt.elapsed() / 1000.0, 'f', 1) << "s";
            QThread::msleep(2000);
        }
        readPool.waitForDone();
    }
    qDebug() << "[BACKUP] read pool drained — draining compress pool ("
             << filesInCompressPipeline.load() << "still compressing )…";
    {
        QElapsedTimer dt; dt.start();
        while (compressPool.activeThreadCount() > 0) {
            qDebug() << "[BACKUP] draining compress pool —" << compressPool.activeThreadCount()
                     << "threads active," << filesInCompressPipeline.load()
                     << "files compressing, elapsed"
                     << QString::number(dt.elapsed() / 1000.0, 'f', 1) << "s";
            QThread::msleep(2000);
        }
        compressPool.waitForDone();
    }
    m_db->commitTxn();   // release the reader's read-snapshot transaction
    qDebug() << "[BACKUP] compress pool drained — signalling writer";

    readerDone.store(true);
    {
        QMutexLocker lk(&writeMutex);
        writeCV.wakeAll();
    }

    writerThread->wait();
    qDebug() << "[BACKUP] writer thread done";
    delete writerThread;

    QMetaObject::invokeMethod(this, [this] {
        if (m_pipelineTimer) {
            m_pipelineTimer->stop();
            m_pipelineTimer->deleteLater();
            m_pipelineTimer = nullptr;
        }
        ui->labelStopWarning->setVisible(false);
    }, Qt::BlockingQueuedConnection);

    // ── Final summary ─────────────────────────────────────────────────────
    double totalS = totalTimer.elapsed() / 1000.0;
    double ratio  = totalBytesWritten > 0 ? (double)totalBytesRead / totalBytesWritten : 0;

    qDebug() << "[BACKUP DONE] files=" << filesProcessed
             << "| read="    << QLocale().formattedDataSize(totalBytesRead)
             << "written="   << QLocale().formattedDataSize(totalBytesWritten)
             << "| avg read="
             << QString::number(statReadMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
             << "compress="
             << QString::number(statCompressMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
             << "write="
             << QString::number(statWriteMs.load()/qMax(1,filesProcessed)/1000.0,'f',2) << "s"
             << "| stalls="  << readerStalls.load()
             << "| elapsed=" << QString::number(totalTimer.elapsed()/1000.0,'f',1) << "s";

    if (!stopRequested.load()) {
        setStatus(QString("Completed %1 files in %2 s  —  ratio %3x  —  written %4")
            .arg(filesProcessed)
            .arg(QString::number(totalS,'f',1))
            .arg(QString::number(ratio,'f',2))
            .arg(QLocale().formattedDataSize(totalBytesWritten)));
    } else {
        // A stop mid-pipeline previously left whatever the last per-file
        // "Writing [X/Y]: ..." status happened to be as the permanent
        // on-screen text — nothing here ever replaced it, so it looked like
        // a live in-flight state forever after the run had actually
        // stopped. Give stop the same explicit final message completion
        // already gets.
        setStatus(QString("Stopped after %1 files in %2 s  —  written %3")
            .arg(filesProcessed)
            .arg(QString::number(totalS,'f',1))
            .arg(QLocale().formattedDataSize(totalBytesWritten)));
    }

    // labelStats has the same problem as labelBackupStatus did — it's only
    // ever written by m_pipelineTimer's 250ms tick, which reenable() (below)
    // is about to stop for good. Without a final write here it freezes on
    // whatever the last tick happened to show, using the same live-rate
    // fields (ETA, RAM in flight) that are meaningless once nothing is
    // running. Give it one last, explicitly final value instead.
    {
        qint64 finalDone = (qint64)(folderDoneCount + filesProcessed);
        QString finalFolder = QDir(currentFolder).dirName();
        QString finalState = stopRequested.load() ? "stopped" : "completed";
        QMetaObject::invokeMethod(this, [this, finalDone, finalFolder, finalState] {
            ui->labelStats->setText(
                QString("done: %1  |  %2  |  folder: %3")
                    .arg(QLocale().toString(finalDone))
                    .arg(finalState)
                    .arg(finalFolder));
        }, Qt::QueuedConnection);
    }

    setProgressVal(filesProcessed);
    setTarEst(currentTarEst, currentLtfsEst, currentRawBytes, activeHardLimitModel.load());
    reenable();
}

// ---------------------------------------------------------------------------
// Active database backend — persisted choice (default MariaDb), selected via
// the two mutually-exclusive backend buttons. Switching (or first startup)
// opens DbConfigDialog for connection details/creation/reconciliation.
// ---------------------------------------------------------------------------

static constexpr const char* kActiveBackendSettingsKey = "db/activeBackend";

void MainWindow::openActiveBackendOrExit()
{
    QString saved = settings.value(kActiveBackendSettingsKey, "mariadb").toString();
    DbBackend::Kind kind = (saved == "sqlite") ? DbBackend::Kind::Sqlite : DbBackend::Kind::MariaDb;

    m_db = std::make_unique<DbBackend>(kind);
    QString dbErr;
    if (!m_db->ensureSchema(&dbErr)) {
        // Never proceed with a broken/absent DB connection — this app's
        // entire purpose is tracking a 682TB archive job against this DB;
        // continuing with m_db == nullptr previously just meant every
        // subsequent operation silently no-op'd or crashed later instead of
        // failing clearly right here.
        QMessageBox::critical(this, "Database connection failed",
            QString("Cannot connect to the active database backend (%1):\n\n%2\n\n"
                    "The application cannot continue without a working database "
                    "connection and will now exit.")
                .arg(kind == DbBackend::Kind::Sqlite ? "SQLite" : "MariaDB")
                .arg(dbErr));
        qDebug() << "[INIT] FATAL: active backend connection failed:" << dbErr;
        ::exit(1);
    }
}

void MainWindow::switchActiveBackend(DbBackend::Kind kind)
{
    settings.setValue(kActiveBackendSettingsKey, kind == DbBackend::Kind::Sqlite ? "sqlite" : "mariadb");

    if (m_db) m_db->closeThreadConnection();
    m_db = std::make_unique<DbBackend>(kind);
    QString dbErr;
    if (!m_db->ensureSchema(&dbErr)) {
        QMessageBox::critical(this, "Database connection failed",
            "Cannot connect to the selected backend:\n" + dbErr);
        m_db.reset();
    }
    updateBackendButtons();
    populateSourceRoots();
}

void MainWindow::updateBackendButtons()
{
    bool isSqlite = m_db && m_db->kind() == DbBackend::Kind::Sqlite;
    ui->pushButtonUseSqlite->setChecked(isSqlite);
    ui->pushButtonUseMariaDb->setChecked(!isSqlite);
    // Text prefix as a guaranteed-visible selection indicator, independent
    // of whether the :checked stylesheet color renders as expected.
    ui->pushButtonUseSqlite->setText(isSqlite ? "✓ SQLite" : "SQLite");
    ui->pushButtonUseMariaDb->setText(!isSqlite ? "✓ MariaDB" : "MariaDB");
}

void MainWindow::on_pushButtonUseSqlite_clicked()
{
    if (m_db && m_db->kind() == DbBackend::Kind::Sqlite) {
        updateBackendButtons(); // already active, keep it checked
        return;
    }
    DbConfigDialog dlg(this);
    if (dlg.exec() == QDialog::Accepted && dlg.hasRequestedBackendSwitch()) {
        switchActiveBackend(dlg.requestedBackendKind());
    } else {
        updateBackendButtons(); // dialog cancelled — revert the button check state
    }
}

void MainWindow::on_pushButtonUseMariaDb_clicked()
{
    if (m_db && m_db->kind() == DbBackend::Kind::MariaDb) {
        updateBackendButtons();
        return;
    }
    DbConfigDialog dlg(this);
    if (dlg.exec() == QDialog::Accepted && dlg.hasRequestedBackendSwitch()) {
        switchActiveBackend(dlg.requestedBackendKind());
    } else {
        updateBackendButtons();
    }
}

// ---------------------------------------------------------------------------
// Capacity hard-limit model selector (Tar vs LTFS). Safe to flip mid-run —
// both models' running totals are kept up to date continuously regardless
// of which one is active, so switching just changes which total the
// rotation check and progress bar consult from that point on.
// ---------------------------------------------------------------------------

void MainWindow::setHardLimitModel(HardLimitModel m)
{
    activeHardLimitModel.store(m);
    settings.setValue(kHardLimitModelSettingsKey, m == HardLimitModel::Ltfs ? "ltfs" : "tar");
    updateHardLimitButtons();
}

void MainWindow::updateHardLimitButtons()
{
    bool isLtfs = activeHardLimitModel.load() == HardLimitModel::Ltfs;
    ui->pushButtonLimitByLtfs->setChecked(isLtfs);
    ui->pushButtonLimitByTar->setChecked(!isLtfs);
    ui->pushButtonLimitByTar->setText(!isLtfs ? "✓ Tar" : "Tar");
    ui->pushButtonLimitByLtfs->setText(isLtfs ? "✓ LTFS" : "LTFS");
}

void MainWindow::on_pushButtonLimitByTar_clicked()  { setHardLimitModel(HardLimitModel::Tar); }
void MainWindow::on_pushButtonLimitByLtfs_clicked() { setHardLimitModel(HardLimitModel::Ltfs); }

void MainWindow::on_lineEditLtfsOverhead_textChanged(const QString &arg1)
{
    bool ok;
    qint64 v = arg1.toLongLong(&ok);
    if (ok && v >= 0 && v <= 1048576) settings.setValue(kLtfsOverheadSettingsKey, arg1);
}
