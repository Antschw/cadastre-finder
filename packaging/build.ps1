<#
.SYNOPSIS
    Script de build complet pour le packaging Windows portable de Cadastre Finder.

.DESCRIPTION
    Produit un dossier dist/cadastre-finder-windows/ contenant :
      - cadastre-finder.exe      (launcher Tauri / WebView2)
      - backend/                 (bundle PyInstaller : FastAPI + Angular statique)
      - data/cadastre.duckdb     (base de données — copie depuis data/processed/)

.PREREQUISITES
    - Python >=3.12 avec uv et le projet installé
    - Node.js >=20 et npm
    - Rust (stable) + cargo tauri-cli v2
    - PyInstaller : uv pip install pyinstaller

.USAGE
    # Depuis la racine du projet :
    pwsh packaging/build.ps1

    # Pour skipper les étapes déjà faites :
    pwsh packaging/build.ps1 -SkipAngular -SkipPyInstaller
#>

param(
    [switch]$SkipAngular,
    [switch]$SkipPyInstaller,
    [switch]$SkipTauri
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DistDir = Join-Path $Root "dist" "cadastre-finder-windows"

Write-Host "=== Build Cadastre Finder (Windows portable) ===" -ForegroundColor Cyan
Write-Host "Racine projet : $Root"
Write-Host "Sortie        : $DistDir"

# ─── Étape 1 : Build Angular ──────────────────────────────────────────────────
if (-not $SkipAngular) {
    Write-Host "`n[1/3] Build Angular..." -ForegroundColor Yellow
    Set-Location (Join-Path $Root "frontend")
    npm ci
    npx ng build --configuration=production
    Set-Location $Root

    $FrontendDist = Join-Path $Root "frontend" "dist" "frontend" "browser"
    if (-not (Test-Path $FrontendDist)) {
        Write-Error "Build Angular échoué : $FrontendDist introuvable"
    }
    Write-Host "  Angular OK -> $FrontendDist" -ForegroundColor Green
} else {
    Write-Host "[1/3] Angular skippé." -ForegroundColor Gray
}

# ─── Étape 2 : Bundle PyInstaller ────────────────────────────────────────────
if (-not $SkipPyInstaller) {
    Write-Host "`n[2/3] Bundle PyInstaller..." -ForegroundColor Yellow
    Set-Location $Root

    # Installer pyinstaller dans l'environnement uv si absent
    uv pip install pyinstaller --quiet

    # Nettoyer les builds précédents
    Remove-Item -Recurse -Force (Join-Path $Root "dist" "backend") -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force (Join-Path $Root "build") -ErrorAction SilentlyContinue

    uv run pyinstaller packaging/server.spec --noconfirm

    $BackendDist = Join-Path $Root "dist" "backend"
    if (-not (Test-Path (Join-Path $BackendDist "server.exe"))) {
        Write-Error "PyInstaller échoué : server.exe introuvable dans $BackendDist"
    }
    Write-Host "  PyInstaller OK -> $BackendDist" -ForegroundColor Green
} else {
    Write-Host "[2/3] PyInstaller skippé." -ForegroundColor Gray
}

# ─── Étape 3 : Build Tauri ───────────────────────────────────────────────────
if (-not $SkipTauri) {
    Write-Host "`n[3/3] Build Tauri..." -ForegroundColor Yellow
    Set-Location (Join-Path $Root "packaging" "tauri-app")

    # Vérifier cargo-tauri
    if (-not (Get-Command "cargo-tauri" -ErrorAction SilentlyContinue)) {
        Write-Host "  Installation de tauri-cli..." -ForegroundColor Gray
        cargo install tauri-cli --version "^2"
    }

    # Générer les icônes si absentes (requis par Tauri pour Windows)
    $IconsDir = Join-Path $Root "packaging" "tauri-app" "icons"
    if (-not (Test-Path (Join-Path $IconsDir "icon.ico"))) {
        Write-Host "  Génération des icônes placeholder..." -ForegroundColor Gray
        New-Item -ItemType Directory -Force $IconsDir | Out-Null

        # Créer un PNG 512x512 simple avec System.Drawing
        Add-Type -AssemblyName System.Drawing
        $bmp = New-Object System.Drawing.Bitmap(512, 512)
        $g = [System.Drawing.Graphics]::FromImage($bmp)
        $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
        $g.FillRectangle([System.Drawing.Brushes]::SteelBlue, 0, 0, 512, 512)
        $font = New-Object System.Drawing.Font("Arial", 180, [System.Drawing.FontStyle]::Bold)
        $brush = [System.Drawing.Brushes]::White
        $sf = New-Object System.Drawing.StringFormat
        $sf.Alignment = [System.Drawing.StringAlignment]::Center
        $sf.LineAlignment = [System.Drawing.StringAlignment]::Center
        $g.DrawString("CF", $font, $brush, [System.Drawing.RectangleF]::new(0, 0, 512, 512), $sf)
        $g.Dispose()
        $srcPng = Join-Path $IconsDir "app-icon.png"
        $bmp.Save($srcPng, [System.Drawing.Imaging.ImageFormat]::Png)
        $bmp.Dispose()

        # Générer tous les formats d'icônes avec cargo tauri icon
        cargo tauri icon $srcPng --output $IconsDir
    }

    cargo tauri build --no-bundle
    Set-Location $Root

    $TauriExe = Join-Path $Root "packaging" "tauri-app" "target" "release" "cadastre-finder.exe"
    if (-not (Test-Path $TauriExe)) {
        Write-Error "Build Tauri échoué : $TauriExe introuvable"
    }
    Write-Host "  Tauri OK -> $TauriExe" -ForegroundColor Green
} else {
    Write-Host "[3/3] Tauri skippé." -ForegroundColor Gray
}

# ─── Assemblage du dossier portable ──────────────────────────────────────────
Write-Host "`n[Assemblage] $DistDir" -ForegroundColor Yellow
Remove-Item -Recurse -Force $DistDir -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $DistDir | Out-Null

# Launcher Tauri
Copy-Item (Join-Path $Root "packaging" "tauri-app" "target" "release" "cadastre-finder.exe") $DistDir

# Backend PyInstaller
Copy-Item -Recurse (Join-Path $Root "dist" "backend") (Join-Path $DistDir "backend")

# Base de données (reproduit la structure data/processed/ attendue par config.py)
$DbSrc = Join-Path $Root "data" "processed" "cadastre.duckdb"
$DataDst = Join-Path $DistDir "data" "processed"
New-Item -ItemType Directory -Force $DataDst | Out-Null
if (Test-Path $DbSrc) {
    Write-Host "  Copie de la base de données (peut prendre quelques minutes)..." -ForegroundColor Gray
    Copy-Item $DbSrc $DataDst
    Write-Host "  Base de données copiée." -ForegroundColor Green
} else {
    Write-Warning "  cadastre.duckdb introuvable ($DbSrc). Copiez-la manuellement dans $DataDst"
}

# ─── Résultat ─────────────────────────────────────────────────────────────────
$SizeMB = [math]::Round((Get-ChildItem $DistDir -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB, 1)
Write-Host "`n=== BUILD TERMINÉ ===" -ForegroundColor Cyan
Write-Host "Dossier : $DistDir"
Write-Host "Taille  : ${SizeMB} MB"
Write-Host ""
Write-Host "Pour distribuer : copiez tout le dossier '$DistDir' sur la machine cible."
Write-Host "Pour lancer     : double-cliquez sur cadastre-finder.exe"
