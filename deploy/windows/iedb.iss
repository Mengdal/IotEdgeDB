; Inno Setup script for IotEdgeDB (iedb) Windows installer
; Builds iedb-{version}-amd64.exe
; Usage (CI): iscc /DMyAppVersion=X.Y.Z iedb.iss
; Source files are expected in the ./build subdirectory (populated by CI).

#define MyAppName "IotEdgeDB"
#define MyAppExeName "iedb.exe"
; MyAppVersion is passed via /DMyAppVersion=... from CI

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
AppId={{8F3C5A1E-2B7D-4E9C-A1F0-6D3B8C2E5A9F}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher=LMGateway
AppPublisherURL=https://www.lmgateway.com/
AppSupportURL=https://github.com/Mengdal/IotEdgeDB/issues
AppUpdatesURL=https://github.com/Mengdal/IotEdgeDB/releases
VersionInfoVersion={#MyAppVersionInfo}
VersionInfoCompany=LMGateway
VersionInfoDescription=IotEdgeDB - High-performance time-series database
DefaultDirName={autopf}\iedb
DefaultGroupName=IotEdgeDB
; Request administrator so we can register a Windows service when chosen
PrivilegesRequired=admin
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
OutputDir=.
OutputBaseFilename=iedb-{#MyAppVersion}-amd64
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
; Uninstall previous version silently on upgrade
CloseApplications=yes
RestartApplications=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "installservice"; Description: "Register iedb as a Windows service (auto-start on boot)"; GroupDescription: "Service"; Flags: unchecked

[Files]
; Main binary
Source: "build/iedb.exe"; DestDir: "{app}"; Flags: ignoreversion
; Config (do not overwrite if user already customized it)
Source: "build/iedb.toml"; DestDir: "{app}"; Flags: ignoreversion onlyifdoesntexist
; Frontend (served from ./front relative to the install dir)
Source: "build/front/*"; DestDir: "{app}\front"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\IotEdgeDB"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"
Name: "{group}\Uninstall IotEdgeDB"; Filename: "{uninstallexe}"

[Run]
; Register as a Windows service only if the user checked the task.
; binPath points at the exe; AppDirectory makes it start in {app} so that
; ./front (served by s.app.Static("/", "./front")) resolves correctly.
Filename: "sc"; Parameters: "create iedb binPath= ""{app}\{#MyAppExeName}"" start= auto DisplayName= ""IotEdgeDB"" obj= ""LocalSystem"""; WorkingDir: "{app}"; StatusMsg: "Registering iedb service..."; Tasks: installservice
Filename: "sc"; Parameters: "start iedb"; WorkingDir: "{app}"; StatusMsg: "Starting iedb service..."; Tasks: installservice

[UninstallRun]
; Stop and remove the service on uninstall (only if it exists).
Filename: "sc"; Parameters: "stop iedb"; RunOnceId: "stopiedbsvc"; StatusMsg: "Stopping iedb service..."; Tasks: installservice
Filename: "sc"; Parameters: "delete iedb"; RunOnceId: "deliedbsvc"; StatusMsg: "Removing iedb service..."; Tasks: installservice

[UninstallDelete]
Type: filesandordirs; Name: "{app}\front"
