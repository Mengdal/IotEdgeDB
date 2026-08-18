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
; Installer window/taskbar/uninstall-list icon (from frontend favicon)
SetupIconFile=build\front\favicon.ico
; Let the user pick the UI language (Chinese simplified offered first)
ShowLanguageDialog=yes
; Uninstall previous version silently on upgrade
CloseApplications=yes
RestartApplications=no

[Languages]
Name: "chinesesimplified"; MessagesFile: "compiler:Languages\ChineseSimplified.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "installservice"; Description: "{cm:TaskInstallService}"; GroupDescription: "{cm:TaskServiceGroup}"; Flags: unchecked

[CustomMessages]
TaskInstallService=Register iedb as a Windows service (auto-start on boot)
TaskServiceGroup=Service
chinesesimplified.TaskInstallService=注册 iedb 为 Windows 服务（开机自启）
chinesesimplified.TaskServiceGroup=服务

[Files]
; Main binary
Source: "build/iedb.exe"; DestDir: "{app}"; Flags: ignoreversion
; Config (do not overwrite if user already customized it)
Source: "build/iedb.toml"; DestDir: "{app}"; Flags: ignoreversion onlyifdoesntexist
; Frontend (served from ./front relative to the install dir)
Source: "build/front/*"; DestDir: "{app}\front"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\IotEdgeDB"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"; IconFilename: "{app}\front\favicon.ico"
Name: "{group}\Uninstall IotEdgeDB"; Filename: "{uninstallexe}"

[Run]
; Register as a Windows service only if the user checked the task.
; binPath points at the exe; WorkingDir makes it start in {app} so that
; ./front (served by s.app.Static("/", "./front")) resolves correctly.
Filename: "sc"; Parameters: "create iedb binPath= ""{app}\{#MyAppExeName}"" start= auto DisplayName= ""IotEdgeDB"" obj= ""LocalSystem"""; WorkingDir: "{app}"; StatusMsg: "Registering iedb service..."; Tasks: installservice
Filename: "sc"; Parameters: "config iedb env= ""IEDB_DATABASE_TEMP_DIRECTORY=C:\LMgateway\iedb\.tmp"" env= ""IEDB_STORAGE_LOCAL_PATH=C:\LMgateway\iedb\data"" env= ""IEDB_COMPACTION_TEMP_DIRECTORY=C:\LMgateway\iedb\data\compaction"""; WorkingDir: "{app}"; StatusMsg: "Configuring iedb service env..."; Tasks: installservice
Filename: "sc"; Parameters: "description iedb ""IotEdgeDB - High-performance time-series database service"""; WorkingDir: "{app}"; StatusMsg: "Setting iedb service description..."; Tasks: installservice
Filename: "sc"; Parameters: "start iedb"; WorkingDir: "{app}"; StatusMsg: "Starting iedb service..."; Tasks: installservice

[UninstallRun]
; (service cleanup is handled in [Code] below, unconditionally — so a service
;  registered manually after install is also removed on uninstall)

[UninstallDelete]
Type: filesandordirs; Name: "{app}\front"
; Remove the iedb data directories created under C:\LMgateway\iedb by the installer
Type: filesandordirs; Name: "C:\LMgateway\iedb\.tmp"
Type: filesandordirs; Name: "C:\LMgateway\iedb\data"
Type: filesandordirs; Name: "C:\LMgateway\iedb"

[Code]
procedure CreateDataDir;
var
  BaseDir: String;
  ResultCode: Integer;
begin
  BaseDir := 'C:\LMgateway\iedb';
  // Create base + subdirectories if they don't exist
  if not DirExists(BaseDir) then
    CreateDir(BaseDir);
  if not DirExists(BaseDir + '\.tmp') then
    CreateDir(BaseDir + '\.tmp');
  if not DirExists(BaseDir + '\data') then
    CreateDir(BaseDir + '\data');
  if not DirExists(BaseDir + '\data\compaction') then
    CreateDir(BaseDir + '\data\compaction');
  // Grant Users group modify access so iedb can write when run manually
  // (the LocalSystem service can already write anywhere)
  Exec(ExpandConstant('{cmd}'), '/c icacls "' + BaseDir + '" /grant BUILTIN\Users:(OI)(CI)M /T', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
    CreateDataDir;
end;

// Returns true if the "iedb" Windows service exists in SCM.
function ServiceExists: Boolean;
var
  ResultCode: Integer;
begin
  Result := Exec(ExpandConstant('{cmd}'), '/c sc query iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) and (ResultCode = 0);
end;

// Unconditional service stop+delete on uninstall. This catches services that
// were registered manually after install (not via the install task) too.
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  ResultCode: Integer;
begin
  if CurUninstallStep = usUninstall then
  begin
    if ServiceExists then
    begin
      // Stop first, ignore "not started" errors
      Exec(ExpandConstant('{cmd}'), '/c sc stop iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
      // Mark for deletion (actual removal happens after process exit)
      Exec(ExpandConstant('{cmd}'), '/c sc delete iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
    end;
  end;
end;
