; Stock Oracle — Inno Setup Installer Script
; Run after BUILD.bat: iscc installer.iss
; Requires: Inno Setup 6+ (https://jrsoftware.org/isinfo.php)

#define MyAppName "Stock Oracle"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "James Cupps"
#define MyAppExeName "StockOracle.exe"

[Setup]
AppId={{A7B3C4D5-E6F7-8901-2345-6789ABCDEF01}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
InfoBeforeFile=installer_readme.txt
OutputDir=installer_output
OutputBaseFilename=StockOracle_Setup_{#MyAppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
DisableProgramGroupPage=yes
UninstallDisplayName={#MyAppName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checked
Name: "startupicon"; Description: "Start Stock Oracle when Windows starts"; GroupDescription: "Startup:"

[Files]
Source: "dist\StockOracle\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon
Name: "{userstartup}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: startupicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch Stock Oracle"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\stock_oracle\cache"

[Code]
procedure CurPageChanged(CurPageID: Integer);
begin
  if CurPageID = wpFinished then
  begin
    WizardForm.FinishedLabel.Caption :=
      'Stock Oracle has been installed successfully!' + #13#10 + #13#10 +
      'On first launch, a setup wizard will walk you through:' + #13#10 +
      '  - Connecting free API keys for real-time data' + #13#10 +
      '  - Setting up your stock watchlist' + #13#10 +
      '  - Training the machine learning model' + #13#10 + #13#10 +
      'Click the Help button inside the app anytime for a full guide.' + #13#10 + #13#10 +
      'Your data is stored in: ' + ExpandConstant('{userappdata}\StockOracle');
  end;
end;
