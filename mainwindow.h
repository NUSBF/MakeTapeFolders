#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QDir>
#include <QElapsedTimer>
#include <QFuture>
#include <QSettings>
#include <QTimer>
#include <atomic>
#include <memory>

#include "db/DbBackend.h"

QT_BEGIN_NAMESPACE
namespace Ui { class MainWindow; }
QT_END_NAMESPACE

// Which capacity model governs folder rotation. Both models are always
// computed regardless of which is active — see mainwindow.cpp's
// tarFileBytes()/ltfsFileBytes().
enum class HardLimitModel { Tar, Ltfs };

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

private slots:
    void on_pushButtonScanSource_clicked();
    void on_pushButtonScanSubfolder_clicked();
    void on_comboBoxSourceRoot_currentIndexChanged(int index);

    void on_pushButtonDest_clicked();
    void on_lineEditMaxSize_textChanged(const QString &arg1);
    void on_pushButtonStart_clicked();
    void on_pushButtonStop_clicked();

    void on_pushButtonUseSqlite_clicked();
    void on_pushButtonUseMariaDb_clicked();

    void on_pushButtonLimitByTar_clicked();
    void on_pushButtonLimitByLtfs_clicked();
    void on_lineEditLtfsOverhead_textChanged(const QString &arg1);

    // ---- Rebuild from Source tab ------------------------------------------
    void on_comboBoxRebuildSourceRoot_currentIndexChanged(int index);
    void on_pushButtonRefreshTapeList_clicked();
    void on_pushButtonRebuildDest_clicked();
    void on_pushButtonScanRebuild_clicked();
    void on_pushButtonSaveMissingList_clicked();
    void on_pushButtonStartRebuild_clicked();
    void on_pushButtonStopRebuild_clicked();

private:
    void runScan(const QString& scanRoot, const QString& sourceRoot);
    void runBackup(const QString& prefix, qint64 maxFolderSize, qint64 ltfsIndexOverheadBytes);
    void populateSourceRoots();

    // Opens the currently-active backend (per QSettings, defaulting to
    // MariaDb) and connects. If it fails, per the "never fail silently"
    // rule: show a critical error and terminate the app rather than limp
    // along with m_db == nullptr.
    void openActiveBackendOrExit();
    void switchActiveBackend(DbBackend::Kind kind);
    void updateBackendButtons();

    void setHardLimitModel(HardLimitModel m);
    void updateHardLimitButtons();

    void closeEvent(QCloseEvent* event) override;
    void initiateShutdown();
    // Separate from initiateShutdown() deliberately: the rebuild pipeline
    // is a plain bounded QThreadPool with no persistent writer thread or
    // unbounded writeMap to force-kill, so it doesn't need (or fit) that
    // 4-step choreography — stopRequested + a bounded wait is sufficient.
    void initiateRebuildShutdown();

    // ---- Rebuild from Source ------------------------------------------------
    // Manifest entry produced by the Scan step — src/sizes come straight from
    // the done table (read-only), foundOnSource is a live filesystem check.
    // Both the scan and the rebuild pipeline consume this same list, so a
    // rebuild run never has to touch the database to know what to attempt.
    struct RebuildManifestItem {
        QString src;
        QString tapeName;
        qint64  bytes         = 0;
        qint64  gzBytes       = 0;
        bool    foundOnSource = false;
    };
    void runRebuildScan(const QString& sourceRoot, const QStringList& tapeNames);
    void runRebuild(const QString& sourceRoot, const QString& outputBase);
    void populateRebuildSourceRoots();
    void setRebuildControlsEnabled(bool en);

    Ui::MainWindow *ui;
    QSettings settings;

    QDir sourcedir{"/home/data"};
    QDir destinationdir{"/home/data"};

    QFuture<void> scanFuture;
    QFuture<void> backupFuture;

    // Rebuild tab — a separate output root from destinationdir (never the
    // live, currently-incrementing tape sequence), and its own futures.
    // Mutually exclusive with backupFuture by construction (confirmed
    // design decision): both would otherwise independently size a RAM
    // budget from the same MemAvailable and hit the same source NFS.
    // Shares stopRequested with the backup pipeline rather than duplicating
    // it — safe precisely because only one of the two can ever be running.
    QDir rebuildDestDir{"/home/data"};
    QFuture<void> rebuildScanFuture;
    QFuture<void> rebuildFuture;
    std::vector<RebuildManifestItem> m_rebuildManifest;

    std::atomic<bool> stopRequested{false};
    std::atomic<bool> forceKillWriter{false};
    bool m_shuttingDown = false;

    // Pipeline atomics promoted to members so shutdown can read them
    std::atomic<int> filesInWriteMap{0};
    std::atomic<int> filesBeingWritten{0};

    // Which stage of runBackup() is currently active, so initiateShutdown()
    // can report accurately instead of always saying "waiting for writer" —
    // that's only true once Phase 3 (the write pipeline) has actually
    // started. See BackupPhase in mainwindow.cpp.
    std::atomic<int> currentBackupPhase{0};

    // Which capacity model gates folder rotation right now — GUI thread
    // writes it (via the Tar/LTFS toggle buttons), the writer thread reads
    // it once per file in the rotation check. Safe to flip mid-run: both
    // models' running totals are kept up to date continuously regardless of
    // which one is active.
    std::atomic<HardLimitModel> activeHardLimitModel{HardLimitModel::Tar};

    QTimer*       m_pipelineTimer   = nullptr;
    QTimer*       m_rebuildPipelineTimer = nullptr;
    QTimer*       m_dbLoadDispTimer = nullptr;
    QElapsedTimer m_dbLoadTimer;

    std::unique_ptr<DbBackend> m_db;
};

#endif // MAINWINDOW_H
