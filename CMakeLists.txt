cmake_minimum_required(VERSION 3.16)

project(MakeTapeFolders VERSION 0.1 LANGUAGES CXX)

set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Widgets Concurrent Sql)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Widgets Concurrent Sql)
find_package(ZLIB REQUIRED)

set(PROJECT_SOURCES
        main.cpp
        mainwindow.cpp
        mainwindow.h
        mainwindow.ui
        db/DbTypes.h
        db/DbBackend.h
        db/DbBackend.cpp
        db/DbSync.h
        db/DbSync.cpp
)

if(${QT_VERSION_MAJOR} GREATER_EQUAL 6)
    qt_add_executable(MakeTapeFolders
        MANUAL_FINALIZATION
        ${PROJECT_SOURCES}
    )
# Define target properties for Android with Qt 6 as:
#    set_property(TARGET MakeTapeFolders APPEND PROPERTY QT_ANDROID_PACKAGE_SOURCE_DIR
#                 ${CMAKE_CURRENT_SOURCE_DIR}/android)
# For more information, see https://doc.qt.io/qt-6/qt-add-executable.html#target-creation
else()
    if(ANDROID)
        add_library(MakeTapeFolders SHARED
            ${PROJECT_SOURCES}
        )
# Define properties for Android with Qt 5 after find_package() calls as:
#    set(ANDROID_PACKAGE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/android")
    else()
        add_executable(MakeTapeFolders
            ${PROJECT_SOURCES}
        )
    endif()
endif()

target_link_libraries(MakeTapeFolders PRIVATE
    Qt${QT_VERSION_MAJOR}::Widgets
    Qt${QT_VERSION_MAJOR}::Concurrent
    Qt${QT_VERSION_MAJOR}::Sql)
target_link_libraries(MakeTapeFolders PRIVATE ZLIB::ZLIB)

# Qt for iOS sets MACOSX_BUNDLE_GUI_IDENTIFIER automatically since Qt 6.1.
# If you are developing for iOS or macOS you should consider setting an
# explicit, fixed bundle identifier manually though.
if(${QT_VERSION} VERSION_LESS 6.1.0)
  set(BUNDLE_ID_OPTION MACOSX_BUNDLE_GUI_IDENTIFIER com.example.MakeTapeFolders)
endif()
set_target_properties(MakeTapeFolders PROPERTIES
    ${BUNDLE_ID_OPTION}
    MACOSX_BUNDLE_BUNDLE_VERSION ${PROJECT_VERSION}
    MACOSX_BUNDLE_SHORT_VERSION_STRING ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}
    MACOSX_BUNDLE TRUE
    WIN32_EXECUTABLE TRUE
)

include(GNUInstallDirs)
install(TARGETS MakeTapeFolders
    BUNDLE DESTINATION .
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

if(QT_VERSION_MAJOR EQUAL 6)
    qt_finalize_executable(MakeTapeFolders)
endif()

# ---------------------------------------------------------------------------
# dbmigrate — standalone CLI to run DbBackend::ensureSchema() (schema
# creation / one-time SQLite src_hash migration) without needing a
# GUI/display. Not installed; a dev/ops tool only.
# ---------------------------------------------------------------------------
add_executable(dbmigrate
    tools/dbmigrate_main.cpp
    db/DbTypes.h
    db/DbBackend.h
    db/DbBackend.cpp
)
target_link_libraries(dbmigrate PRIVATE Qt${QT_VERSION_MAJOR}::Core Qt${QT_VERSION_MAJOR}::Sql)

# ---------------------------------------------------------------------------
# Deploy the rebuilt QMYSQL/QMARIADB driver plugin (see
# thirdparty/sqldrivers/README.md for why the SDK's own copy doesn't load)
# next to each executable so QCoreApplication::addLibraryPath() picks it up.
# ---------------------------------------------------------------------------
foreach(_mtf_target MakeTapeFolders dbmigrate)
    add_custom_command(TARGET ${_mtf_target} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory
            "$<TARGET_FILE_DIR:${_mtf_target}>/sqldrivers"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/sqldrivers/libqsqlmysql.so"
            "$<TARGET_FILE_DIR:${_mtf_target}>/sqldrivers/libqsqlmysql.so"
    )
endforeach()
