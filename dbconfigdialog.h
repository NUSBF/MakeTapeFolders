#ifndef DBCONFIGDIALOG_H
#define DBCONFIGDIALOG_H

#include "db/DbBackend.h"

#include <QDialog>
#include <atomic>

class QLineEdit;
class QSpinBox;
class QLabel;
class QPushButton;
class QProgressBar;

// Database configuration + reconciliation dialog. Three tabs:
//   1. MariaDB connection details — edit/test/create schema
//   2. SQLite file location — pick/create
//   3. Reconcile — show current state of both DBs, sync one way or the other
//
// Opened when the user clicks the currently-inactive backend selector
// button in MainWindow. Accepting the dialog (via the per-tab "Use this as
// active backend" action) tells the caller which Kind to switch to.
class DbConfigDialog : public QDialog
{
    Q_OBJECT

public:
    explicit DbConfigDialog(QWidget* parent = nullptr);

    // Set after the dialog is accepted via one of the "Use this backend"
    // buttons — tells MainWindow which backend to actually switch to.
    bool hasRequestedBackendSwitch() const { return m_requestedSwitch; }
    DbBackend::Kind requestedBackendKind() const { return m_requestedKind; }

private slots:
    void onMariaTest();
    void onMariaCreateSchema();
    void onMariaSaveAndUse();
    void onSqliteBrowse();
    void onSqliteCreateSchema();
    void onSqliteSaveAndUse();
    void onRefreshMariaState();
    void onRefreshSqliteState();
    void onSyncSqliteToMaria();
    void onSyncMariaToSqlite();

private:
    // The sync runs on a QThreadPool task that captures `this` — closing the
    // dialog while it's in flight would let that task outlive the dialog
    // and use-after-free. Block closing until the sync finishes instead of
    // adding cross-thread cancellation machinery for something the user said
    // doesn't need to be robust yet.
    void closeEvent(QCloseEvent* event) override;
    void reject() override;

    QWidget* buildMariaTab();
    QWidget* buildSqliteTab();
    QWidget* buildReconcileTab();
    ConnectionConfig configFromMariaFields() const;
    void runSync(DbBackend::Kind fromKind, DbBackend::Kind toKind);
    static QString formatDbState(const DbState& s);

    // MariaDB tab widgets
    QLineEdit*   m_mariaHost = nullptr;
    QSpinBox*    m_mariaPort = nullptr;
    QLineEdit*   m_mariaDb   = nullptr;
    QLineEdit*   m_mariaUser = nullptr;
    QLineEdit*   m_mariaPass = nullptr;
    QLabel*      m_mariaStatus = nullptr;

    // SQLite tab widgets
    QLineEdit*   m_sqlitePath = nullptr;
    QLabel*      m_sqliteStatus = nullptr;

    // Reconcile tab widgets
    QLabel*      m_mariaStateLabel = nullptr;
    QLabel*      m_sqliteStateLabel = nullptr;
    QProgressBar* m_syncProgress = nullptr;
    QLabel*      m_syncStatus = nullptr;
    QPushButton* m_syncToMariaBtn = nullptr;
    QPushButton* m_syncToSqliteBtn = nullptr;

    std::atomic<bool> m_syncStopRequested{false};
    std::atomic<bool> m_syncInProgress{false};
    bool m_requestedSwitch = false;
    DbBackend::Kind m_requestedKind = DbBackend::Kind::MariaDb;
};

#endif // DBCONFIGDIALOG_H
