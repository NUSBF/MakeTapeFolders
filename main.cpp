#include "mainwindow.h"

#include <QApplication>
#include <QByteArray>
#include <QDateTime>
#include <QFile>
#include <QMutex>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>

static QFile   g_logFile;
static QMutex  g_logMutex;

// Must run before QApplication is constructed: Qt reads QT_PLUGIN_PATH
// during early static initialization, and QCoreApplication::addLibraryPath()
// called afterward does not take priority over the SDK's own sqldrivers
// plugin for duplicate driver keys (verified empirically). See
// thirdparty/sqldrivers/README.md — the SDK's QMYSQL/QMARIADB plugin needs
// libmysqlclient.so.21, which isn't installed here (only MariaDB
// Connector/C is); our rebuilt copy lives next to this binary in
// sqldrivers/.
static void prependExeDirToPluginPath()
{
    char buf[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len <= 0) return;
    buf[len] = '\0';
    QString exe = QString::fromUtf8(buf);
    int slash = exe.lastIndexOf('/');
    if (slash < 0) return;
    QByteArray dir = exe.left(slash).toUtf8();
    QByteArray existing = qgetenv("QT_PLUGIN_PATH");
    qputenv("QT_PLUGIN_PATH", existing.isEmpty() ? dir : dir + ":" + existing);
}

static void messageHandler(QtMsgType type, const QMessageLogContext&, const QString& msg)
{
    const char* level = "DBG";
    if      (type == QtWarningMsg)  level = "WRN";
    else if (type == QtCriticalMsg) level = "ERR";
    else if (type == QtFatalMsg)    level = "FTL";

    QString line = QString("[%1] %2  %3\n")
        .arg(QDateTime::currentDateTime().toString("hh:mm:ss.zzz"))
        .arg(level)
        .arg(msg);

    QMutexLocker lk(&g_logMutex);
    if (g_logFile.isOpen()) {
        g_logFile.write(line.toUtf8());
        g_logFile.flush();
    }
    fprintf(stderr, "%s", line.toUtf8().constData());
}

int main(int argc, char *argv[])
{
    prependExeDirToPluginPath();
    QApplication a(argc, argv);

    // applicationDirPath() requires QApplication to exist first
    QString logPath = QApplication::applicationDirPath() + "/MakeTapeFolders_"
        + QDateTime::currentDateTime().toString("yyyyMMdd_HHmmss") + ".log";
    g_logFile.setFileName(logPath);
    (void)g_logFile.open(QIODevice::WriteOnly | QIODevice::Text);
    qInstallMessageHandler(messageHandler);
    qDebug() << "[LOG] Writing to:" << logPath;

    MainWindow w;
    w.setWindowTitle(QString("MakeTapeFolders  —  log: %1").arg(logPath));
    w.show();
    int ret = a.exec();

    g_logFile.close();
    return ret;
}
