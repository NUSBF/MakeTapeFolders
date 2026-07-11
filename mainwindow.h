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

private:
    void runScan(const QString& scanRoot, const QString& sourceRoot);
    void runBackup(const QString& prefix, qint64 maxFolderSize);
    void populateSourceRoots();

    // Opens the currently-active backend (per QSettings, defaulting to
    // MariaDb) and connects. If it fails, per the "never fail silently"
    // rule: show a critical error and terminate the app rather than limp
    // along with m_db == nullptr.
    void openActiveBackendOrExit();
    void switchActiveBackend(DbBackend::Kind kind);
    void updateBackendButtons();

    void closeEvent(QCloseEvent* event) override;
    void initiateShutdown();

    Ui::MainWindow *ui;
    QSettings settings;

    QDir sourcedir{"/home/data"};
    QDir destinationdir{"/home/data"};

    QFuture<void> scanFuture;
    QFuture<void> backupFuture;
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

    QTimer*       m_pipelineTimer   = nullptr;
    QTimer*       m_dbLoadDispTimer = nullptr;
    QElapsedTimer m_dbLoadTimer;

    std::unique_ptr<DbBackend> m_db;
};

#endif // MAINWINDOW_H
