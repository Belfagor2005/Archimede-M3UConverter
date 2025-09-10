#!/bin/bash
## setup command=wget -q --no-check-certificate https://raw.githubusercontent.com/Belfagor2005/Archimede-M3UConverter/main/installer.sh -O - | /bin/sh

##############################################################
##                  Configurazione Versione                 ##
##############################################################
version='2.1'
changelog='\nFixed EPG ON CONVERSION TV'

##############################################################
##               Variabili Percorsi Temporanei              ##
##############################################################
TMPPATH=/tmp/Archimede-M3UConverter-main
FILEPATH=/tmp/main.tar.gz

##############################################################
##          Determinazione Percorso Installazione           ##
##############################################################
if [ ! -d /usr/lib64 ]; then
    PLUGINPATH=/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter
else
    PLUGINPATH=/usr/lib64/enigma2/python/Plugins/Extensions/M3UConverter
fi

##############################################################
##              Controllo Tipo Sistema/Distro               ##
##############################################################
if [ -f /var/lib/dpkg/status ]; then
    STATUS=/var/lib/dpkg/status
    OSTYPE=DreamOs
else
    STATUS=/var/lib/opkg/status
    OSTYPE=Dream
fi

echo ""
echo "##############################################################"
echo "##           Inizio Installazione Archimede M3UConverter    ##"
echo "##                     Versione $version                    ##"
echo "##############################################################"
echo ""

##############################################################
##            Controllo e Installazione Dipendenze          ##
##############################################################

# Controllo presenza wget
if ! command -v wget >/dev/null 2>&1; then
    echo "⚠ Installazione wget in corso..."
    if [ "$OSTYPE" = "DreamOs" ]; then
        apt-get update && apt-get install -y wget || { echo "Failed to install wget"; exit 1; }
    else
        opkg update && opkg install wget || { echo "Failed to install wget"; exit 1; }
    fi
else
    echo "✔ wget è già installato"
fi

# Determinazione versione Python
if python --version 2>&1 | grep -q '^Python 3\.'; then
    echo "✔ Immagine Python3 rilevata"
    PYTHON=PY3
    Packagesix=python3-six
    Packagerequests=python3-requests
else
    echo "✔ Immagine Python2 rilevata"
    PYTHON=PY2
    Packagerequests=python-requests
fi

# Installazione dipendenze Python
echo ""
echo "##############################################################"
echo "##          Verifica Dipendenze Python                     ##"
echo "##############################################################"

if [ "$PYTHON" = "PY3" ] && ! grep -qs "Package: $Packagesix" "$STATUS"; then
    echo "⚠ Installazione python3-six in corso..."
    opkg update && opkg install python3-six || { echo "Failed to install python3-six"; exit 1; }
fi

if ! grep -qs "Package: $Packagerequests" "$STATUS"; then
    echo "⚠ Installazione $Packagerequests in corso..."
    if [ "$OSTYPE" = "DreamOs" ]; then
        apt-get update && apt-get install -y "$Packagerequests" || { echo "Failed to install $Packagerequests"; exit 1; }
    else
        opkg update && opkg install "$Packagerequests" || { echo "Failed to install $Packagerequests"; exit 1; }
    fi
fi

##############################################################
##           Pulizia File e Cartelle Temporanee             ##
##############################################################
echo ""
echo "##############################################################"
echo "##           Pulizia File Temporanei                      ##"
echo "##############################################################"

[ -r "$TMPPATH" ] && rm -rf "$TMPPATH" > /dev/null 2>&1
[ -r "$FILEPATH" ] && rm -f "$FILEPATH" > /dev/null 2>&1
[ -r "$PLUGINPATH" ] && rm -rf "$PLUGINPATH" > /dev/null 2>&1

##############################################################
##            Download e Installazione Plugin               ##
##############################################################
echo ""
echo "##############################################################"
echo "##           Download Archimede M3UConverter               ##"
echo "##############################################################"

mkdir -p "$TMPPATH"
cd "$TMPPATH" || exit 1

set -e
if [ -f /var/lib/dpkg/status ]; then
    echo "# Immagine OE2.5/2.6 rilevata #"
else
    echo "# Immagine OE2.0 rilevata #"
fi

echo ""
echo "⚠ Download plugin in corso..."
wget --no-check-certificate -q 'https://github.com/Belfagor2005/Archimede-M3UConverter/archive/refs/heads/main.tar.gz' -O "$FILEPATH" || { echo "Download failed"; exit 1; }
echo "✔ Download completato!"

echo ""
echo "⚠ Estrazione archivio in corso..."
tar -xzf "$FILEPATH" -C /tmp/ || { echo "Extraction failed"; exit 1; }
echo "✔ Estrazione completata!"

echo ""
echo "⚠ Installazione file in corso..."
cp -rf /tmp/Archimede-M3UConverter-main/usr/ / || { echo "Copy failed"; exit 1; }
set +e

##############################################################
##            Verifica Installazione Completata             ##
##############################################################
echo ""
echo "##############################################################"
echo "##           Verifica Installazione                       ##"
echo "##############################################################"

if [ -d "$PLUGINPATH" ]; then
    echo "✔ Plugin installato correttamente in: $PLUGINPATH"
else
    echo "✖ Errore! Installazione fallita!"
    rm -rf "$TMPPATH" "$FILEPATH" > /dev/null 2>&1
    exit 1
fi

##############################################################
##               Pulizia Finale e Riavvio                   ##
##############################################################
echo ""
echo "⚠ Pulizia file temporanei in corso..."
rm -rf "$TMPPATH" "$FILEPATH" > /dev/null 2>&1
sync

##############################################################
##               Informazioni di Sistema                    ##
##############################################################
FILE="/etc/image-version"
box_type=$(head -n 1 /etc/hostname 2>/dev/null || echo "Sconosciuto")
distro_value=$(grep '^distro=' "$FILE" 2>/dev/null | awk -F '=' '{print $2}' || echo "Sconosciuto")
distro_version=$(grep '^version=' "$FILE" 2>/dev/null | awk -F '=' '{print $2}' || echo "Sconosciuto")
python_vers=$(python --version 2>&1 || echo "Sconosciuto")

echo ""
echo "##############################################################"
echo "#           INSTALLATO CON SUCCESSO!                        #"
echo "#                Sviluppato da LULULLA                      #"
echo "#               https://corvoboys.org                       #"
echo "##############################################################"
echo "#           Il dispositivo verrà RIAVVIATO                  #"
echo "##############################################################"
echo "^^^^^^^^^^ Informazioni di debug:"
echo "BOX MODEL: $box_type"
echo "SISTEMA: $OSTYPE"
echo "PYTHON: $python_vers"
echo "NOME IMMAGINE: $distro_value"
echo "VERSIONE IMMAGINE: $distro_version"
echo "##############################################################"

sleep 5
echo ""
echo "⚠ Riavvio in corso..."
killall -9 enigma2
exit 0
