#include "dbconfigdialog.h"
#include "db/DbSync.h"

#include <QCloseEvent>
#include <QDateTime>
#include <QFileDialog>
#include <QFormLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QMessageBox>
#include <QMetaObject>
#include <QProgressBar>
#include <QPushButton>
#include <QSpinBox>
#include <QTabWidget>
#include <QThreadPool>
#include <QVBoxLayout>

DbConfigDialog::DbConfigDialog(QWidget* parent) : QDialog(parent)
{
    setWindowTitle("Database Configuration");
    resize(700, 500);

    auto* tabs = new QTabWidget(this);
    tabs->addTab(buildMariaTab(), "MariaDB");
    tabs->addTab(buildSqliteTab(), "SQLite");
    tabs->addTab(buildReconcileTab(), "Reconcile");

    auto* closeBtn = new QPushButton("Close", this);
    connect(closeBtn, &QPushButton::clicked, this, &QDialog::reject);

    auto* mainLayout = new QVBoxLayout(this);
    mainLayout->addWidget(tabs);
    auto* bottomRow = new QHBoxLayout();
    bottomRow->addStretch();
    bottomRow->addWidget(closeBtn);
    mainLayout->addLayout(bottomRow);
}

void DbConfigDialog::closeEvent(QCloseEvent* event)
{
    if (m_syncInProgress.load()) {
        QMessageBox::information(this, "Sync in progress",
            "A sync is still running — please wait for it to finish before closing.");
        event->ignore();
        return;
    }
    event->accept();
}

void DbConfigDialog::reject()
{
    if (m_syncInProgress.load()) {
        QMessageBox::information(this, "Sync in progress",
            "A sync is still running — please wait for it to finish before closing.");
        return;
    }
    QDialog::reject();
}

// ---------------------------------------------------------------------------
// MariaDB tab
// ---------------------------------------------------------------------------

QWidget* DbConfigDialog::buildMariaTab()
{
    ConnectionConfig cfg = ConnectionConfig::load();

    auto* w = new QWidget(this);
    auto* form = new QFormLayout();

    m_mariaHost = new QLineEdit(cfg.mariaHost, w);
    m_mariaPort = new QSpinBox(w);
    m_mariaPort->setRange(1, 65535);
    m_mariaPort->setValue(cfg.mariaPort);
    m_mariaDb   = new QLineEdit(cfg.mariaDb, w);
    m_mariaUser = new QLineEdit(cfg.mariaUser, w);
    m_mariaPass = new QLineEdit(cfg.mariaPass, w);
    m_mariaPass->setEchoMode(QLineEdit::Password);

    form->addRow("Host:", m_mariaHost);
    form->addRow("Port:", m_mariaPort);
    form->addRow("Database:", m_mariaDb);
    form->addRow("User:", m_mariaUser);
    form->addRow("Password:", m_mariaPass);

    auto* passNote = new QLabel(
        "Password is read from MTF_MARIADB_PASSWORD or "
        "~/.config/MakeTapeFolders/mariadb.env — not saved by this dialog. "
        "Edit it here only to test a different password; use the credentials "
        "file to change it permanently.", w);
    passNote->setWordWrap(true);
    passNote->setStyleSheet("color: gray; font-size: 9pt;");

    auto* testBtn   = new QPushButton("Test Connection", w);
    auto* createBtn = new QPushButton("Create Database Schema", w);
    auto* useBtn    = new QPushButton("Save && Use This as Active Backend", w);
    connect(testBtn, &QPushButton::clicked, this, &DbConfigDialog::onMariaTest);
    connect(createBtn, &QPushButton::clicked, this, &DbConfigDialog::onMariaCreateSchema);
    connect(useBtn, &QPushButton::clicked, this, &DbConfigDialog::onMariaSaveAndUse);

    m_mariaStatus = new QLabel("", w);
    m_mariaStatus->setWordWrap(true);

    auto* btnRow = new QHBoxLayout();
    btnRow->addWidget(testBtn);
    btnRow->addWidget(createBtn);
    btnRow->addStretch();

    auto* layout = new QVBoxLayout(w);
    layout->addLayout(form);
    layout->addWidget(passNote);
    layout->addLayout(btnRow);
    layout->addWidget(m_mariaStatus);
    layout->addStretch();
    layout->addWidget(useBtn);
    return w;
}

ConnectionConfig DbConfigDialog::configFromMariaFields() const
{
    ConnectionConfig cfg = ConnectionConfig::load(); // keep sqlitePath as-is
    cfg.mariaHost = m_mariaHost->text().trimmed();
    cfg.mariaPort = m_mariaPort->value();
    cfg.mariaDb   = m_mariaDb->text().trimmed();
    cfg.mariaUser = m_mariaUser->text().trimmed();
    cfg.mariaPass = m_mariaPass->text();
    return cfg;
}

void DbConfigDialog::onMariaTest()
{
    m_mariaStatus->setText("Testing…");
    DbBackend db(DbBackend::Kind::MariaDb, configFromMariaFields());
    DbState s = db.currentState();
    db.closeThreadConnection();
    m_mariaStatus->setText(formatDbState(s));
}

void DbConfigDialog::onMariaCreateSchema()
{
    m_mariaStatus->setText("Creating schema…");
    DbBackend db(DbBackend::Kind::MariaDb, configFromMariaFields());
    QString err;
    bool ok = db.ensureSchema(&err);
    db.closeThreadConnection();
    m_mariaStatus->setText(ok ? "Schema created/verified OK." : ("FAILED: " + err));
}

void DbConfigDialog::onMariaSaveAndUse()
{
    ConnectionConfig cfg = configFromMariaFields();
    // Verify it actually works before committing to it as the active backend
    // — never switch to a backend we haven't confirmed we can reach.
    DbBackend db(DbBackend::Kind::MariaDb, cfg);
    QString err;
    if (!db.ensureSchema(&err)) {
        db.closeThreadConnection();
        QMessageBox::critical(this, "Cannot use MariaDB",
            "Failed to connect/prepare schema:\n" + err);
        return;
    }
    db.closeThreadConnection();
    cfg.save();
    m_requestedSwitch = true;
    m_requestedKind = DbBackend::Kind::MariaDb;
    accept();
}

// ---------------------------------------------------------------------------
// SQLite tab
// ---------------------------------------------------------------------------

QWidget* DbConfigDialog::buildSqliteTab()
{
    ConnectionConfig cfg = ConnectionConfig::load();

    auto* w = new QWidget(this);
    m_sqlitePath = new QLineEdit(cfg.sqlitePath, w);
    auto* browseBtn = new QPushButton("Browse…", w);
    connect(browseBtn, &QPushButton::clicked, this, &DbConfigDialog::onSqliteBrowse);

    auto* pathRow = new QHBoxLayout();
    pathRow->addWidget(m_sqlitePath);
    pathRow->addWidget(browseBtn);

    auto* createBtn = new QPushButton("Create / Verify Schema Here", w);
    auto* useBtn    = new QPushButton("Save && Use This as Active Backend", w);
    connect(createBtn, &QPushButton::clicked, this, &DbConfigDialog::onSqliteCreateSchema);
    connect(useBtn, &QPushButton::clicked, this, &DbConfigDialog::onSqliteSaveAndUse);

    m_sqliteStatus = new QLabel("", w);
    m_sqliteStatus->setWordWrap(true);

    auto* layout = new QVBoxLayout(w);
    layout->addWidget(new QLabel("Database file path:", w));
    layout->addLayout(pathRow);
    layout->addWidget(createBtn);
    layout->addWidget(m_sqliteStatus);
    layout->addStretch();
    layout->addWidget(useBtn);
    return w;
}

void DbConfigDialog::onSqliteBrowse()
{
    QString path = QFileDialog::getSaveFileName(this, "SQLite database file",
        m_sqlitePath->text(), "SQLite DB (*.db);;All files (*)",
        nullptr, QFileDialog::DontConfirmOverwrite);
    if (!path.isEmpty()) m_sqlitePath->setText(path);
}

void DbConfigDialog::onSqliteCreateSchema()
{
    m_sqliteStatus->setText("Creating/verifying schema…");
    ConnectionConfig cfg = ConnectionConfig::load();
    cfg.sqlitePath = m_sqlitePath->text().trimmed();
    DbBackend db(DbBackend::Kind::Sqlite, cfg);
    QString err;
    bool ok = db.ensureSchema(&err);
    db.closeThreadConnection();
    m_sqliteStatus->setText(ok ? "Schema created/verified OK." : ("FAILED: " + err));
}

void DbConfigDialog::onSqliteSaveAndUse()
{
    ConnectionConfig cfg = ConnectionConfig::load();
    cfg.sqlitePath = m_sqlitePath->text().trimmed();
    DbBackend db(DbBackend::Kind::Sqlite, cfg);
    QString err;
    if (!db.ensureSchema(&err)) {
        db.closeThreadConnection();
        QMessageBox::critical(this, "Cannot use SQLite",
            "Failed to open/prepare schema:\n" + err);
        return;
    }
    db.closeThreadConnection();
    cfg.save();
    m_requestedSwitch = true;
    m_requestedKind = DbBackend::Kind::Sqlite;
    accept();
}

// ---------------------------------------------------------------------------
// Reconcile tab
// ---------------------------------------------------------------------------

QString DbConfigDialog::formatDbState(const DbState& s)
{
    if (!s.connected)
        return "NOT CONNECTED: " + s.connectError;
    if (!s.schemaExists)
        return "Connected — no schema yet (empty).";
    QString lastEntry = s.lastScannedAt > 0
        ? QDateTime::fromSecsSinceEpoch(s.lastScannedAt).toString(Qt::ISODate)
        : "—";
    return QString("Connected. files: %1  |  done: %2  |  last scan: %3")
        .arg(s.filesCount).arg(s.doneCount).arg(lastEntry);
}

QWidget* DbConfigDialog::buildReconcileTab()
{
    auto* w = new QWidget(this);

    auto* mariaGroup = new QGroupBox("MariaDB", w);
    auto* mariaRefresh = new QPushButton("Check Current State", mariaGroup);
    m_mariaStateLabel = new QLabel("(not checked yet)", mariaGroup);
    m_mariaStateLabel->setWordWrap(true);
    connect(mariaRefresh, &QPushButton::clicked, this, &DbConfigDialog::onRefreshMariaState);
    auto* mariaLayout = new QVBoxLayout(mariaGroup);
    mariaLayout->addWidget(mariaRefresh);
    mariaLayout->addWidget(m_mariaStateLabel);

    auto* sqliteGroup = new QGroupBox("SQLite", w);
    auto* sqliteRefresh = new QPushButton("Check Current State", sqliteGroup);
    m_sqliteStateLabel = new QLabel("(not checked yet)", sqliteGroup);
    m_sqliteStateLabel->setWordWrap(true);
    connect(sqliteRefresh, &QPushButton::clicked, this, &DbConfigDialog::onRefreshSqliteState);
    auto* sqliteLayout = new QVBoxLayout(sqliteGroup);
    sqliteLayout->addWidget(sqliteRefresh);
    sqliteLayout->addWidget(m_sqliteStateLabel);

    m_syncToMariaBtn  = new QPushButton("Sync SQLite → MariaDB", w);
    m_syncToSqliteBtn = new QPushButton("Sync MariaDB → SQLite", w);
    connect(m_syncToMariaBtn, &QPushButton::clicked, this, &DbConfigDialog::onSyncSqliteToMaria);
    connect(m_syncToSqliteBtn, &QPushButton::clicked, this, &DbConfigDialog::onSyncMariaToSqlite);

    m_syncProgress = new QProgressBar(w);
    m_syncProgress->setRange(0, 0); // indeterminate — row totals aren't known up front
    m_syncProgress->setVisible(false);
    m_syncStatus = new QLabel("", w);
    m_syncStatus->setWordWrap(true);

    auto* syncRow = new QHBoxLayout();
    syncRow->addWidget(m_syncToMariaBtn);
    syncRow->addWidget(m_syncToSqliteBtn);

    auto* layout = new QVBoxLayout(w);
    layout->addWidget(mariaGroup);
    layout->addWidget(sqliteGroup);
    layout->addLayout(syncRow);
    layout->addWidget(m_syncProgress);
    layout->addWidget(m_syncStatus);
    layout->addStretch();
    return w;
}

void DbConfigDialog::onRefreshMariaState()
{
    m_mariaStateLabel->setText("Checking…");
    DbBackend db(DbBackend::Kind::MariaDb);
    DbState s = db.currentState();
    db.closeThreadConnection();
    m_mariaStateLabel->setText(formatDbState(s));
}

void DbConfigDialog::onRefreshSqliteState()
{
    m_sqliteStateLabel->setText("Checking…");
    DbBackend db(DbBackend::Kind::Sqlite);
    DbState s = db.currentState();
    db.closeThreadConnection();
    m_sqliteStateLabel->setText(formatDbState(s));
}

void DbConfigDialog::runSync(DbBackend::Kind fromKind, DbBackend::Kind toKind)
{
    m_syncToMariaBtn->setEnabled(false);
    m_syncToSqliteBtn->setEnabled(false);
    m_syncProgress->setVisible(true);
    m_syncStatus->setText("Starting sync…");
    m_syncStopRequested.store(false);
    m_syncInProgress.store(true);

    QThreadPool::globalInstance()->start([this, fromKind, toKind]() {
        auto from = std::make_unique<DbBackend>(fromKind);
        auto to   = std::make_unique<DbBackend>(toKind);
        QString err;
        QString resultMsg;
        if (!from->ensureSchema(&err) || !to->ensureSchema(&err)) {
            resultMsg = "Sync FAILED: " + err;
        } else {
            DbSync::syncAll(*from, *to, [this](const DbSync::Progress& p) {
                QMetaObject::invokeMethod(this, [this, p] {
                    m_syncStatus->setText(QString("Syncing… %1 files, %2 done-records copied")
                        .arg(p.filesCopied).arg(p.doneCopied));
                }, Qt::QueuedConnection);
            }, m_syncStopRequested);
            resultMsg = "Sync complete.";
        }
        from->closeThreadConnection();
        to->closeThreadConnection();

        QMetaObject::invokeMethod(this, [this, resultMsg] {
            m_syncToMariaBtn->setEnabled(true);
            m_syncToSqliteBtn->setEnabled(true);
            m_syncProgress->setVisible(false);
            m_syncStatus->setText(resultMsg);
            m_syncInProgress.store(false);
        }, Qt::QueuedConnection);
    });
}

void DbConfigDialog::onSyncSqliteToMaria() { runSync(DbBackend::Kind::Sqlite, DbBackend::Kind::MariaDb); }
void DbConfigDialog::onSyncMariaToSqlite() { runSync(DbBackend::Kind::MariaDb, DbBackend::Kind::Sqlite); }
