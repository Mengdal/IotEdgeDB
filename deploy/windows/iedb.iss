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
DefaultDirName=C:\LMgateway\iedb
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
Name: "installservice"; Description: "{cm:TaskInstallService}"; GroupDescription: "{cm:TaskServiceGroup}"

[CustomMessages]
TaskInstallService=Register iedb as a Windows service (auto-start on boot)
TaskServiceGroup=Service
OpenWebUI=Open IotEdgeDB web UI in browser
chinesesimplified.TaskInstallService=注册 iedb 为 Windows 服务（开机自启）
chinesesimplified.TaskServiceGroup=服务
chinesesimplified.OpenWebUI=在浏览器中打开 IotEdgeDB 管理界面

[Files]
; Main binary
Source: "build/iedb.exe"; DestDir: "{app}"; Flags: ignoreversion
; NSSM — service wrapper (iedb.exe is a console app, can't talk to SCM directly)
Source: "build/nssm.exe"; DestDir: "{app}"; Flags: ignoreversion
; Config (do not overwrite if user already customized it)
Source: "build/iedb.toml"; DestDir: "{app}"; Flags: ignoreversion onlyifdoesntexist
; Frontend (served from ./front relative to the install dir)
Source: "build/front/*"; DestDir: "{app}\front"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\IotEdgeDB"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"; IconFilename: "{app}\front\favicon.ico"
Name: "{group}\Uninstall IotEdgeDB"; Filename: "{uninstallexe}"

[Run]
; Register iedb as a Windows service via NSSM (only if the user checked the task).
; iedb.exe is a console app — NSSM wraps it so SCM gets proper status reports.
; All paths derive from {app} (the folder the user chose in the wizard).
Filename: "{app}\nssm.exe"; Parameters: "install iedb ""{app}\iedb.exe"""; WorkingDir: "{app}"; StatusMsg: "Registering iedb service..."; Tasks: installservice
; Point NSSM at the install dir so ./front and ./iedb.toml resolve correctly
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppDirectory ""{app}"""; WorkingDir: "{app}"; StatusMsg: "Setting service working directory..."; Tasks: installservice
; Auto-start on boot
Filename: "{app}\nssm.exe"; Parameters: "set iedb Start SERVICE_AUTO_START"; WorkingDir: "{app}"; StatusMsg: "Configuring service start mode..."; Tasks: installservice
; Run as LocalSystem (default for nssm install, set explicitly for clarity)
Filename: "{app}\nssm.exe"; Parameters: "set iedb ObjectName LocalSystem"; WorkingDir: "{app}"; StatusMsg: "Setting service account..."; Tasks: installservice
; Service display name & description
Filename: "{app}\nssm.exe"; Parameters: "set iedb DisplayName ""IotEdgeDB"""; WorkingDir: "{app}"; StatusMsg: "Setting service display name..."; Tasks: installservice
Filename: "{app}\nssm.exe"; Parameters: "set iedb Description ""IotEdgeDB - High-performance time-series database service"""; WorkingDir: "{app}"; StatusMsg: "Setting service description..."; Tasks: installservice
; Data directories passed via env vars (derived from {app})
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppEnvironmentExtra IEDB_DATABASE_TEMP_DIRECTORY={app}\.tmp IEDB_STORAGE_LOCAL_PATH={app}\data IEDB_COMPACTION_TEMP_DIRECTORY={app}\data\compaction"; WorkingDir: "{app}"; StatusMsg: "Configuring service environment..."; Tasks: installservice
; Redirect stdout/stderr to log files under {app}\logs so nothing is lost
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppStdout ""{app}\logs\iedb.out.log"""; WorkingDir: "{app}"; StatusMsg: "Configuring service logging..."; Tasks: installservice
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppStderr ""{app}\logs\iedb.err.log"""; WorkingDir: "{app}"; StatusMsg: "Configuring service logging..."; Tasks: installservice
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppRotateFiles 1"; WorkingDir: "{app}"; StatusMsg: "Configuring service logging..."; Tasks: installservice
Filename: "{app}\nssm.exe"; Parameters: "set iedb AppRotateBytes 10485760"; WorkingDir: "{app}"; StatusMsg: "Configuring service logging..."; Tasks: installservice
; Start the service
Filename: "{app}\nssm.exe"; Parameters: "start iedb"; WorkingDir: "{app}"; StatusMsg: "Starting iedb service..."; Tasks: installservice
; "Open IotEdgeDB in browser" checkbox on the Finish page.
; shellexec opens the URL in the user's default browser when the user
; clicks Finish with this checkbox ticked.
Filename: "http://localhost:8000"; Description: "{cm:OpenWebUI}"; Flags: postinstall shellexec skipifsilent

[UninstallRun]
; (service cleanup is handled in [Code] below, unconditionally — so a service
;  registered manually after install is also removed on uninstall)

[UninstallDelete]
Type: filesandordirs; Name: "{app}\front"
; Remove the iedb data directories created under the install dir by the installer
Type: filesandordirs; Name: "{app}\.tmp"
Type: filesandordirs; Name: "{app}\data"
Type: filesandordirs; Name: "{app}\logs"
; NOTE: {app} itself is removed in [Code] CurUninstallStepChanged, but ONLY if
; it is empty after the above subdirs and the standard file removal are done.
; This avoids nuking a user-selected parent folder (e.g. C:\LMgateway) while
; still cleaning up the default C:\LMgateway\iedb folder.

[Code]

// Returns true if the given directory has no files or subdirectories inside.
// Inno Setup's Pascal Script does not provide IsDirEmpty, so we check by
// looking for any first entry via FindFirst.
function IsDirEmpty(const Dir: String): Boolean;
var
  FindRec: TFindRec;
begin
  Result := True;
  if FindFirst(AddBackslash(Dir) + '*', FindRec) then
  begin
    repeat
      if (FindRec.Name <> '.') and (FindRec.Name <> '..') then
      begin
        Result := False;
        Break;
      end;
    until not FindNext(FindRec);
    FindClose(FindRec);
  end;
end;

procedure CreateDataDir;
var
  BaseDir: String;
  ResultCode: Integer;
begin
  // Derive the data directory from the install dir the user chose ({app}),
  // so everything (binary, config, front, data, tmp) lives under one folder.
  BaseDir := ExpandConstant('{app}');
  // Create base + subdirectories if they don't exist
  if not DirExists(BaseDir) then
    CreateDir(BaseDir);
  if not DirExists(BaseDir + '\.tmp') then
    CreateDir(BaseDir + '\.tmp');
  if not DirExists(BaseDir + '\data') then
    CreateDir(BaseDir + '\data');
  if not DirExists(BaseDir + '\data\compaction') then
    CreateDir(BaseDir + '\data\compaction');
  if not DirExists(BaseDir + '\logs') then
    CreateDir(BaseDir + '\logs');
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

// Unconditional service stop+remove on uninstall. This catches services that
// were registered manually after install (not via the install task) too.
// Uses nssm to stop/remove (consistent with how it was installed), falls back
// to sc if nssm.exe is missing.
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  ResultCode: Integer;
  NssmPath: String;
  AppDir: String;
begin
  if CurUninstallStep = usUninstall then
  begin
    if ServiceExists then
    begin
      NssmPath := ExpandConstant('{app}\nssm.exe');
      // Stop first, ignore errors (service may not be running)
      if FileExists(NssmPath) then
        Exec(NssmPath, 'stop iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode)
      else
        Exec(ExpandConstant('{cmd}'), '/c sc stop iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
      // Remove the service entry from SCM
      if FileExists(NssmPath) then
        Exec(NssmPath, 'remove iedb confirm', '', SW_HIDE, ewWaitUntilTerminated, ResultCode)
      else
        Exec(ExpandConstant('{cmd}'), '/c sc delete iedb', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
    end;

    // Remove the install dir itself if it is now empty.
    // [UninstallDelete] already removed subdirs (front/.tmp/data/logs) and
    // the standard uninstall removed iedb.exe/nssm.exe/iedb.toml. If anything
    // else remains (user dropped their own files in), we leave the folder alone.
    AppDir := ExpandConstant('{app}');
    if DirExists(AppDir) and IsDirEmpty(AppDir) then
      RemoveDir(AppDir);
  end;
end;
