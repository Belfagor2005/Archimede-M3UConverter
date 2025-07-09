# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 1.7                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "21:50 - 20250527"                    #
#                                                       #
#  Credits:                                             #
#  - Original concept by Lululla                        #
#  Usage of this code without proper attribution        #
#  is strictly prohibited.                              #
#  For modifications and redistribution,                #
#  please maintain this credit header.                  #
#########################################################
"""
__author__ = "Lululla"

# Standard library
import codecs
import json
import shutil
import unicodedata
from gettext import gettext as _
from os import access, W_OK, listdir, makedirs, remove, replace, chmod, fsync, system
from os.path import dirname, exists, isdir, isfile, join, normpath
from re import sub, findall, DOTALL, search
from threading import Lock
from time import strftime
from urllib.parse import urlparse, unquote

# Third-party libraries
from twisted.internet import threads

# Enigma2 core
from enigma import eDVBDB, eServiceReference, getDesktop, eTimer

# Enigma2 components
from Components.ActionMap import ActionMap
from Components.FileList import FileList
from Components.Label import Label
from Components.MenuList import MenuList
from Components.Sources.Progress import Progress
from Components.Sources.StaticText import StaticText
from Components.config import config, ConfigSelection, ConfigSubsection, ConfigYesNo, ConfigNumber

# Enigma2 screens
from Screens.MessageBox import MessageBox
from Screens.Screen import Screen
from Screens.Setup import Setup

# Enigma2 tools
from Tools.Directories import fileExists, resolveFilename, defaultRecordingLocation, SCOPE_MEDIA


try:
    from Components.AVSwitch import AVSwitch
except ImportError:
    from Components.AVSwitch import eAVControl as AVSwitch


class AspectManager:
    def __init__(self):
        self.init_aspect = self.get_current_aspect()
        print("[INFO] Initial aspect ratio:", self.init_aspect)

    def get_current_aspect(self):
        """Returns the current aspect ratio of the device."""
        try:
            return int(AVSwitch().getAspectRatioSetting())
        except Exception as e:
            print("[ERROR] Failed to get aspect ratio:", str(e))
            return 0

    def restore_aspect(self):
        """Restores the original aspect ratio when the plugin exits."""
        try:
            print("[INFO] Restoring aspect ratio to:", self.init_aspect)
            AVSwitch().setAspectRatio(self.init_aspect)
        except Exception as e:
            print("[ERROR] Failed to restore aspect ratio:", str(e))


# for my friend Archimede
currversion = '1.7'
last_date = "20250709"
title_plug = _("Archimede Universal Converter v.%s by Lululla") % currversion
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2

aspect_manager = AspectManager()
screenwidth = getDesktop(0).size()
screen_width = screenwidth.width()


# add lululla for debug
class ColoredLogger:
    LEVELS = {
        "DEBUG": ("\033[92m", "[DEBUG]"),       # green
        "INFO": ("\033[97m", "[INFO] "),        # white
        "WARNING": ("\033[93m", "[WARN] "),     # yellow
        "ERROR": ("\033[91m", "[ERROR]"),       # red
    }
    END = "\033[0m"
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, log_file=None, clear_on_start=True):
        if not hasattr(self, '_initialized'):  # Previene reinizializzazione
            self.log_file = log_file
            if self.log_file and clear_on_start:
                try:
                    remove(self.log_file)
                except Exception:
                    pass
            self._initialized = True

    def log(self, level, message):
        """Base method for all log levels"""
        prefix, label = self.LEVELS.get(level.upper(), ("", "[LOG] "))
        timestamp = strftime("%Y-%m-%d %H:%M:%S")

        # Console formatting with colors
        console_msg = f"{timestamp} {label} {prefix}{message}{self.END}"

        # File formatting without colors
        file_msg = f"{timestamp} {label} {message}"

        print(console_msg)

        if self.log_file:
            try:
                with open(self.log_file, "a") as f:
                    f.write(file_msg + "\n")
            except Exception:
                print(f"{timestamp} [ERROR] Failed to write to log file")

    # Standard methods for various log levels
    def debug(self, message):
        self.log("DEBUG", message)

    def info(self, message):
        self.log("INFO", message)

    def warning(self, message):
        self.log("WARNING", message)

    def error(self, message):
        self.log("ERROR", message)

    # Additional methods for compatibility
    def exception(self, message):
        self.log("ERROR", f"EXCEPTION: {message}")

    def critical(self, message):
        self.log("ERROR", f"CRITICAL: {message}")


# Global singleton Instance
logger = ColoredLogger(log_file="/tmp/m3u_converter.log")

# Test
if __name__ == "__main__":
    logger.debug("Test debug message")
    logger.info("Test info message")
    logger.warning("Test warning message")
    logger.error("Test error message")
    try:
        1 / 0
    except Exception as e:
        logger.exception(f"Divisione per zero {str(e)}")

print(f"Logger type: {type(logger)}")  # Debug
print(f"Has debug method: {hasattr(logger, 'debug')}")  # Debug
logger.log("INFO", f"START PLUGIN {str(title_plug)}")


# Ensure movie path
def defaultMoviePath():
    result = config.usage.default_path.value
    if not result.endswith("/"):
        result += "/"
    if not isdir(result):
        return defaultRecordingLocation(config.usage.default_path.value)
    return result


def get_mounted_devices():
    """Recovers all possible mounted devices with write permissions"""
    from Components.Harddisk import harddiskmanager
    
    # Complete list of all possible mount paths
    common_mount_points = [
        # Standard Enigma2 paths
        "/media/hdd",           # Main internal HDD
        "/media/usb",           # Main USB
        "/media/usb1",          # Secondary USB
        "/media/usb2",          # Tertiary USB
        "/media/ba",            # Barry Allen
        "/media/nas",           # NAS mount
        "/media/net",           # Network mount
        "/media/mmc",           # SD/MMC cards
        "/media/cf",            # Compact Flash
        "/tmp",                 # Temporary

        # Other common paths
        "/autofs",              # Auto-mount
        "/hdd",                 # Legacy systems
        "/usr",                 # User space
        "/var",                 # Various
        "/mnt",                 # Generic mount point
        "/media/mmcblk0p1",     # Specific SD card
        "/media/mmcblk1p1"      # Second SD card
    ]
    devices = []
    
    try:
        # First, check standard paths
        for path in common_mount_points:
            if isdir(path) and access(path, W_OK):
                desc = _("Storage")
                if "hdd" in path:
                    desc = _("Internal HDD")
                elif "usb" in path:
                    desc = _("USB Drive")
                elif "nas" in path or "net" in path:
                    desc = _("Network Storage")
                devices.append((path + "/", desc))

        # Then add all partitions mounted by the system
        for p in harddiskmanager.getMountedPartitions():
            if p.mountpoint and p.mountpoint not in [d[0] for d in devices]:
                if isdir(p.mountpoint) and access(p.mountpoint, W_OK):
                    devices.append((p.mountpoint + "/", p.description or _("Disk")))

        # Add any network shares
        net_dir = resolveFilename(SCOPE_MEDIA, "net")
        if isdir(net_dir):
            for share in listdir(net_dir):
                share_path = join(net_dir, share) + "/"
                if isdir(share_path) and share_path not in [d[0] for d in devices]:
                    devices.append((share_path, _("Network Share")))

    except Exception as e:
        logger.error(f"Error scanning devices: {str(e)}")
        # Fallback to base paths
        devices = [
            ("/media/hdd/", _("Internal HDD")),
            ("/media/usb/", _("USB Drive")),
            ("/tmp/", _("Temporary Storage"))
        ]

    # Remove duplicates while preserving order
    unique_devices = []
    seen_paths = set()
    for path, desc in devices:
        if path not in seen_paths:
            seen_paths.add(path)
            unique_devices.append((path, desc))

    return unique_devices


def update_mounts():
    """Update the list of mounted devices and update config choices"""
    mounts = get_mounted_devices()
    if not mounts:
        default_path = defaultMoviePath()
        mounts = [(default_path, default_path)]
    config.plugins.m3uconverter.lastdir.setChoices(mounts, default=mounts[0][0])
    config.plugins.m3uconverter.lastdir.save()


# Init Config
config.plugins.m3uconverter = ConfigSubsection()
default_dir = config.movielist.last_videodir.value if isdir(config.movielist.last_videodir.value) else defaultMoviePath()
config.plugins.m3uconverter.lastdir = ConfigSelection(default=default_dir, choices=[])
config.plugins.m3uconverter.bouquet_position = ConfigSelection(
    default="bottom",
    choices=[("top", _("Top")), ("bottom", _("Bottom"))]
)
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)
config.plugins.m3uconverter.filter_dead_channels = ConfigYesNo(default=False)
config.plugins.m3uconverter.auto_reload = ConfigYesNo(default=True)
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)
config.plugins.m3uconverter.max_backups = ConfigNumber(default=3)
update_mounts()


def create_backup():
    """Create a backup of bouquets.tv only"""
    from shutil import copy2
    src = "/etc/enigma2/bouquets.tv"
    dst = "/etc/enigma2/bouquets.tv.bak"
    if exists(src):
        copy2(src, dst)


def reload_services():
    """Reload the list of services"""
    eDVBDB.getInstance().reloadServicelist()
    eDVBDB.getInstance().reloadBouquets()


def transliterate(text):
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode('ascii', 'ignore').decode('ascii')


def clean_group_name(name):
    return name.encode("ascii", "ignore").decode().replace(" ", "_").replace("/", "_").replace(":", "_")[:50]


class CoreConverter:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        if not self.__initialized:
            self.backup_dir = "/tmp/archimede_backup/"
            self.log_file = "/tmp/archimede_converter.log"
            self.__create_dirs()
            self.__initialized = True

    def __create_dirs(self):
        """Create necessary directories if they don't exist"""
        try:
            makedirs(self.backup_dir, exist_ok=True)
        except Exception as e:
            print(f"Error creating directories: {str(e)}")

    def safe_convert(self, func, *args, **kwargs):
        """Performs a conversion with automatic backup"""
        try:
            self._create_backup()
            result = func(*args, **kwargs)
            self._log_success(func.__name__)
            return result
        except Exception as e:
            self._log_error(e)
            self._restore_backup()
            raise RuntimeError(f"Conversion failed (restored backup). Error: {str(e)}")

    def _create_backup(self):
        """Create a backup of the existing bouquets"""
        try:
            if not exists("/etc/enigma2/bouquets.tv"):
                return

            timestamp = strftime("%Y%m%d_%H%M%S")
            backup_file = join(self.backup_dir, f"bouquets_{timestamp}.tv")
            shutil.copy2("/etc/enigma2/bouquets.tv", backup_file)
        except Exception as e:
            raise RuntimeError(f"Backup failed: {str(e)}")

    def _restore_backup(self):
        """Restore the most recent available backup"""
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            if backups:
                latest = join(self.backup_dir, backups[-1])
                shutil.copy2(latest, "/etc/enigma2/bouquets.tv")
        except Exception as e:
            raise RuntimeError(f"Restore failed: {str(e)}")

    def _log_success(self, operation):
        """Log a successful operation"""
        msg = f"{strftime('%Y-%m-%d %H:%M:%S')} [SUCCESS] {operation}"
        self.__write_log(msg)

    def _log_error(self, error):
        """Log an error"""
        msg = f"{strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {str(error)}"
        self.__write_log(msg)

    def __write_log(self, message):
        """Write to the log file"""
        try:
            with open(self.log_file, "a") as f:
                f.write(message + "\n")
        except Exception:
            print(f"Fallback log: {message}")

    def filter_channels(self, channels, filter_type="all"):
        """Filter channels by type"""
        if not channels:
            return []

        if filter_type == "working":
            return [ch for ch in channels if self._is_url_alive(ch.get("url", ""))]
        return channels

    def _is_url_alive(self, url, timeout=5):
        """Check if a URL is reachable"""
        if not url:
            return False

        try:
            cmd = f"curl --max-time {timeout} --head --silent --fail --output /dev/null {url}"
            return system(cmd) == 0
        except Exception:
            return False

    def cleanup_old_backups(self, max_backups=5):
        """Keep only the latest N backups"""
        try:
            backups = sorted([f for f in listdir(self.backup_dir)
                              if f.startswith("bouquets_") and f.endswith(".tv")])

            for old_backup in backups[:-max_backups]:
                remove(join(self.backup_dir, old_backup))
        except Exception as e:
            self._log_error(f"Cleanup failed: {str(e)}")


core_converter = CoreConverter()


class M3UFileBrowser(Screen):
    def __init__(self, session, startdir="/etc/enigma2", matchingPattern=r"(?i)^.*\.tv$", conversion_type=None):
        Screen.__init__(self, session)
        self.skinName = "FileBrowser"
        self.conversion_type = conversion_type
        self["filelist"] = FileList(
            startdir,
            matchingPattern=matchingPattern,
            showDirectories=True,
            showFiles=True,
            useServiceRef=False
        )

        self["actions"] = ActionMap(["OkCancelActions", "ColorActions"], {
            "ok": self.ok_pressed,
            "green": self.ok_pressed,
            "cancel": self.close
        }, -1)
        if self.conversion_type == "tv_to_m3u":
            self.onShown.append(self._filter_list)  # Filter after screen is shown

    def _filter_list(self):
        """Filter list to show only directories and .tv files containing 'http'"""
        filtered = []
        for entry in self["filelist"].list:
            if not entry or not isinstance(entry[0], tuple):
                continue  # Skip invalid entries

            file_data = entry[0]
            path = None
            is_dir = False

            # Handle different tuple formats
            if len(file_data) >= 2:
                # Standard format: (path, is_dir, ...)
                path = file_data[0]
                is_dir = file_data[1]

            elif len(file_data) == 1 and isinstance(file_data[0], str):
                # Special case for parent directory: ('..',)
                path = file_data[0]
                is_dir = True
            else:
                logger.log("DEBUG", f"Skipping invalid entry: {file_data}")
                continue

            # Handle special cases like parent directory
            if path == ".." or is_dir:
                filtered.append(entry)
            else:
                # Only include .tv files with HTTP content
                if path and path.lower().endswith(".tv") and self._contains_http(path):
                    filtered.append(entry)

        self["filelist"].list = filtered
        self["filelist"].l.setList(filtered)

    def _contains_http(self, filename):
        """Check if file contains 'http' (case-insensitive) with full path"""
        try:
            # Get the current directory to form the full path
            current_dir = self["filelist"].getCurrentDirectory()
            full_path = join(current_dir, filename)

            with open(full_path, "r") as f:
                return any("http" in line.lower() for line in f)
        except Exception as e:
            logger.log("ERROR", f"Error reading {full_path}: {str(e)}")
            return False

    def ok_pressed(self):
        selection = self["filelist"].getCurrent()
        if not selection or not isinstance(selection, list) or not isinstance(selection[0], tuple):
            logger.log("ERROR", f"Invalid selection format: {selection}")
            return

        file_data = selection[0]
        path = file_data[0]
        is_dir = file_data[1]
        dir_icon = None
        logger.log("INFO", f"file_data: {file_data}, path: {path}, is_dir: {is_dir}")
        try:
            if dir_icon in (ICON_STORAGE, ICON_PARENT, ICON_CURRENT):
                self["filelist"].changeDir(path)
                if self.conversion_type == "tv_to_m3u":
                    self._filter_list()
            elif is_dir:
                self["filelist"].changeDir(path)
                if self.conversion_type == "tv_to_m3u":
                    self._filter_list()
            else:
                current_dir = self["filelist"].getCurrentDirectory()
                full_path = join(current_dir, path)
                logger.log("INFO", f"Selected full file path: {full_path}")
                self.close(full_path)
        except Exception as e:
            logger.log("ERROR", f"ok_pressed error: {str(e)}")


class ConversionSelector(Screen):

    skin = """
    <screen name="ConversionSelector" position="center,center" size="1280,720" title="Archimede Universal Converter - Select Type" flags="wfNoBorder">
        <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
        <widget name="list" position="20,20" size="1240,559" itemHeight="50" font="Regular;34" scrollbarMode="showNever" />
        <widget name="status" position="20,605" size="1240,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
        <eLabel backgroundColor="red" cornerRadius="3" position="50,700" size="300,6" zPosition="11" />
        <eLabel backgroundColor="green" cornerRadius="3" position="347,700" size="300,6" zPosition="11" />
        <eLabel backgroundColor="yellow" cornerRadius="3" position="647,700" size="300,6" zPosition="11" />
        <!--
        <eLabel backgroundColor="blue" cornerRadius="3" position="882,720" size="200,6" zPosition="11" />
        -->
        <widget source="key_red" render="Label" position="50,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
        <widget source="key_green" render="Label" position="350,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
        <widget source="key_yellow" render="Label" position="650,660" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
        <!--
        <widget source="key_blue" render="Label" position="880,685" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white"/>
        -->
    </screen>"""

    def __init__(self, session):
        Screen.__init__(self, session)
        self.session = session
        self.skinName = "ConversionSelector"
        self.is_modal = True
        self.setTitle(title_plug)
        self.menu = [
            (_("M3U ➔ Enigma2 Bouquets"), "m3u_to_tv", "m3u"),
            (_("Enigma2 Bouquets ➔ M3U"), "tv_to_m3u", "tv"),
            (_("JSON ➔ Enigma2 Bouquets"), "json_to_tv", "json"),
            (_("XSPF ➔ M3U Playlist"), "xspf_to_m3u", "xspf"),
            (_("Remove M3U Bouquets"), "purge_m3u_bouquets", None)
        ]
        self["list"] = MenuList([(x[0], x[1]) for x in self.menu])

        self["status"] = Label(_("We're ready: what do you want to do?"))
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions"], {
            # "red": lambda: self.close(None),
            "red": self.close,
            "green": self.select_item,
            "ok": self.select_item,
            "yellow": self.purge_m3u_bouquets,
            # "cancel": lambda: self.close(None)
            "cancel": self.close
        })
        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Select"))
        self["key_yellow"] = StaticText(_("Remove Bouquets"))

    def purge_m3u_bouquets(self, directory="/etc/enigma2", pattern="_m3ubouquet.tv"):
        """Remove all bouquet files created by M3UConverter"""
        create_backup()
        removed_files = []

        # Remove matching bouquet files
        for f in listdir(directory):
            file_path = join(directory, f)
            if isfile(file_path) and search(pattern, f):
                try:
                    remove(file_path)
                    removed_files.append(f)
                except Exception as e:
                    logger.log("ERROR", f"Failed to remove: {str(f)} Error {str(e)}")

        # Remove matching lines from bouquets.tv
        bouquets_file = join(directory, "bouquets.tv")
        if exists(bouquets_file):
            with open(bouquets_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            with open(bouquets_file, "w", encoding="utf-8") as f:
                for line in lines:
                    if not search(pattern, line):
                        f.write(line)

        if removed_files:
            message = "Removed %d bouquet(s):\n%s" % (
                len(removed_files),
                "\n".join(removed_files)
            )
        else:
            message = "No M3UConverter bouquets found to remove."

        self.session.open(MessageBox, message, MessageBox.TYPE_INFO)

    def selectionMade(self):
        selection = self["list"].getCurrent()
        if selection:
            conversion_type = selection[1]
            if conversion_type == "purge_m3u_bouquets":
                self.purge_m3u_bouquets()
            else:
                self.openFileBrowser(conversion_type)

    def openFileBrowser(self, conversion_type):
        patterns = {
            "m3u_to_tv": r"(?i)^.*\.(m3u|m3u8)$",
            "tv_to_m3u": r"(?i)^.*\.tv$",
            "json_to_tv": r"(?i)^.*\.json$",
            "xspf_to_m3u": r"(?i)^.*\.xspf$"
        }
        start_dir = "/media/hdd" if isdir("/media/hdd") else "/tmp"

        self.session.openWithCallback(
            lambda res: self.fileSelected(res, conversion_type),
            M3UFileBrowser,
            startdir=start_dir,
            matchingPattern=patterns.get(conversion_type, r".*"),
            title=self._get_browser_title(conversion_type)  # Aggiunto titolo al browser
        )

    def _get_browser_title(self, conversion_type):
        """Restituisce il titolo appropriato per il file browser"""
        titles = {
            "m3u_to_tv": _("Select M3U file to convert"),
            "tv_to_m3u": _("Select bouquet to convert"),
            "json_to_tv": _("Select JSON file to convert"),
            "xspf_to_m3u": _("Select XSPF playlist to convert")
        }
        return titles.get(conversion_type, _("Select file"))

    def fileSelected(self, res, conversion_type):
        if res:
            title_map = {
                "m3u_to_tv": _("M3U to Enigma2 Bouquet Conversion"),
                "tv_to_m3u": _("Enigma2 Bouquet to M3U Conversion"),
                "json_to_tv": _("JSON to Enigma2 Bouquet Conversion"),
                "xspf_to_m3u": _("XSPF to M3U Playlist Conversion")
            }
            
            converter = UniversalConverter(
                session=self.session,
                conversion_type=conversion_type,
                selected_file=res
            )
            
            converter.setTitle(title_map.get(conversion_type, title_plug))
            
            if conversion_type == "tv_to_m3u":
                converter["key_green"].setText(_("Convert to M3U"))
                converter["status"].setText(_("TV bouquet loaded. Press Green to convert to M3U"))
            elif conversion_type == "m3u_to_tv":
                converter["key_green"].setText(_("Convert to Bouquet"))
                converter["status"].setText(_("M3U file loaded. Press Green to convert to bouquet"))
            
            self.session.open(converter)

    def convert_json_to_tv(self, filepath):
        converter = UniversalConverter(self.session, "json_to_tv")
        converter.selected_file = filepath
        converter.convert_json_to_tv()

    def convert_xspf_to_m3u(self, filepath):
        converter = UniversalConverter(self.session, "xspf_to_m3u")
        converter.selected_file = filepath
        converter.convert_xspf_to_m3u()

    def select_item(self):
        selection = self["list"].getCurrent()
        if not selection:
            return
            
        if selection[1] == "purge_m3u_bouquets":
            self.purge_m3u_bouquets()
            return
            
        self.openFileBrowser(selection[1])


class UniversalConverter(Screen):
    if screen_width > 1280:

        skin = """
        <screen position="-25,-20" size="1920,1080" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
            <widget name="list" position="65,70" size="1122,797" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="65,920" size="1127,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <eLabel backgroundColor="red" cornerRadius="3" position="65,1025" size="300,6" zPosition="11" />
            <eLabel backgroundColor="green" cornerRadius="3" position="366,1025" size="300,6" zPosition="11" />
            <eLabel backgroundColor="yellow" cornerRadius="3" position="666,1025" size="300,6" zPosition="11" />
            <eLabel backgroundColor="blue" cornerRadius="3" position="966,1025" size="300,6" zPosition="11" />
            <widget source="key_red" render="Label" position="65,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="key_green" render="Label" position="365,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="#00ff00" conditional="key_green" />
            <!--
            <widget source="key_green" render="Label" position="365,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            -->
            <widget source="key_yellow" render="Label" position="664,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="key_blue" render="Label" position="967,985" size="300,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="65,880" size="1125,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <widget source="progress_text" render="Label" position="65,880" size="1124,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1200,820" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1200,875" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1200,930" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="1220,125" size="640,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="1220,166" zPosition="20" size="640,360" backgroundColor="transparent" transparent="0" cornerRadius="14" />
        </screen>"""

    else:
        skin = """
        <screen position="center,center" size="1280,720" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="23,608" size="1185,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <eLabel backgroundColor="red" cornerRadius="3" position="20,700" size="275,6" zPosition="11" />
            <eLabel backgroundColor="green" cornerRadius="3" position="295,700" size="275,6" zPosition="11" />
            <eLabel backgroundColor="yellow" cornerRadius="3" position="570,700" size="275,6" zPosition="11" />
            <eLabel backgroundColor="blue" cornerRadius="3" position="845,700" size="275,6" zPosition="11" />
            <widget source="key_red" render="Label" position="20,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="key_green" render="Label" position="295,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="key_yellow" render="Label" position="570,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="key_blue" render="Label" position="845,660" size="275,40" zPosition="1" font="Regular;28" halign="center" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="25,582" size="1180,30" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <widget source="progress_text" render="Label" position="24,582" size="1180,30" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1121,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1175,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1230,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular; 28" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
        </screen>"""

    def __init__(self, session, conversion_type, selected_file=None):
        Screen.__init__(self, session)
        self.conversion_type = conversion_type
        self.m3u_list = []
        self.bouquet_list = []
        self.converter = core_converter
        self.selected_file = selected_file
        self.progress = None
        self.progress_source = Progress()
        base_path = config.plugins.m3uconverter.lastdir.value
        self.full_path = base_path
        self["list"] = MenuList([])
        self["Title"] = Label(title_plug)
        
        self["status"] = Label(_("Ready"))
        self["key_red"] = StaticText(_("Open File"))
        self["key_green"] = Label()
        self["key_yellow"] = StaticText(_("Filter"))
        self["key_blue"] = StaticText(_("Tools"))
        self["progress_source"] = self.progress_source
        self["progress_text"] = StaticText("")
        self["progress_source"].setValue(0)
        self.setTitle(title_plug)
        self.initialservice = self.session.nav.getCurrentlyPlayingServiceReference()

        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MediaPlayerActions", "MenuActions"], {
            "red": self.open_file,              # Open file
            "green": self.start_conversion,     # Start conversion
            "yellow": self.toggle_filter,       # Enable/disable filter
            "blue": self.show_tools_menu,       # Tools menu
            "menu": self.open_settings,         # Settings
            "ok": self.key_ok,                  # Play channel
            "cancel": self.keyClose,            # Close
            "stop": self.stop_player            # Stop playback
        }, -1)

        self.file_loaded = False if selected_file is None else True
        self.update_green_button()

        if self.conversion_type == "m3u_to_tv":
            # self.init_m3u_converter()
            pass
        else:
            self.conversion_type = "tv_to_m3u"
            self.init_tv_converter()

        self["status"] = Label(_("Ready: Select the file from the %s you configured in settings.") % self.full_path)

    def update_green_button(self):
        if not hasattr(self, 'conversion_type'):
            return
            
        status_messages = {
            "tv_to_m3u": (_("Convert to M3U"), _("TV bouquet loaded. Press Green to convert to M3U")),
            "m3u_to_tv": (_("Convert to Bouquet"), _("M3U file loaded. Press Green to convert to bouquet")),
            "json_to_tv": (_("Convert JSON to Bouquet"), _("JSON file loaded. Press Green to convert")),
            "xspf_to_m3u": (_("Convert XSPF to M3U"), _("XSPF playlist loaded. Ready to convert"))
        }

        if self.file_loaded and self.m3u_list:
            label, status = status_messages.get(self.conversion_type, (_("Convert"), _("Ready to convert")))
            self["key_green"].setText(label)
            self["status"].setText(status)
        else:
            self["key_green"].setText("")
            if not self.file_loaded:
                self["status"].setText(_("Please select a file first"))
            elif not self.m3u_list:
                self["status"].setText(_("No valid channels found in file"))

    # optional
    def create_manual_backup(self):
        try:
            self.converter._create_backup()
            self.session.open(MessageBox, _("Backup created successfully!"), MessageBox.TYPE_INFO)
        except Exception as e:
            self.session.open(MessageBox, _(f"Backup failed: {str(e)}"), MessageBox.TYPE_ERROR)

    def init_m3u_converter(self):
        self.onShown.append(self.delayed_file_browser)

    def delayed_file_browser(self):
        try:
            self.onShown.remove(self.delayed_file_browser)
        except ValueError:
            pass
        # from twisted.internet import reactor
        # reactor.callLater(0.2, self.open_file_browser)

    def init_tv_converter(self):
        self.update_path_tv()

    def update_path_tv(self):
        try:
            if not exists("/etc/enigma2"):
                raise OSError("Bouquets path not found")

            if not access("/etc/enigma2", W_OK):
                logger.log("WARNING", "Read-only bouquets path /etc/enigma2")

        except Exception as e:
            logger.log("ERROR", f"TV path error: {str(e)}")
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

    def toggle_filter(self):
        """Enable/disable filtering of non-working channels"""
        config.plugins.m3uconverter.filter_dead_channels.value = \
            not config.plugins.m3uconverter.filter_dead_channels.value
        config.plugins.m3uconverter.filter_dead_channels.save()

        if self.m3u_list:
            self.parse_file()

        # Save previous status
        self._previous_status = self["status"].getText()

        # Show temporary filter status
        status = _("Filter: %s") % (_("ON") if config.plugins.m3uconverter.filter_dead_channels.value else _("OFF"))
        self["status"].setText(status)

        # Restore previous status after 6 seconds
        self.status_timer = eTimer()
        self.status_timer.callback.append(self._restore_status_text)
        self.status_timer.start(6000)  # milliseconds

    def _restore_status_text(self):
        """Restore the previous status text"""
        if hasattr(self, "_previous_status"):
            self["status"].setText(self._previous_status)

        if hasattr(self, "status_timer"):
            self.status_timer.stop()
            self.status_timer.callback.remove(self._restore_status_text)

    def show_tools_menu(self):
        """Additional tools menu"""
        from Screens.ChoiceBox import ChoiceBox
        menu = [
            (_("Create Backup"), "backup"),
            (_("Reload Services"), "reload"),
            (_("Plugin Info"), "info")
        ]
        
        def tool_selected(choice):
            if choice:
                if choice[1] == "backup":
                    self.create_manual_backup()
                elif choice[1] == "reload":
                    reload_services()
                    self["status"].setText(_("Services reloaded"))
                elif choice[1] == "info":
                    self.show_plugin_info()
        
        self.session.openWithCallback(
            tool_selected,
            ChoiceBox,
            title=_("Tools Menu"),
            list=menu
        )

    def _safe_show_summary(self):
        """Wrapper sicuro per show_summary con gestione errori"""
        try:
            if not self.m3u_list:
                self["status"].setText(_("No channels loaded!"))
                return

            if hasattr(self.converter, 'filter_channels'):
                valid_channels = self.converter.filter_channels(
                    self.m3u_list,
                    "working" if config.plugins.m3uconverter.filter_dead_channels.value else "all"
                )
            else:
                valid_channels = self.m3u_list

            self.show_summary(valid_channels)
        except Exception as e:
            logger.error(f"Show summary failed: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error preparing summary:\n%s") % str(e),
                MessageBox.TYPE_ERROR
            )

    def show_summary(self, channels):
        """Show summary with valid channels"""
        try:
            summary = [
                _("=== Conversion Summary ==="),
                _("Total channels: %d") % len(channels),
                _("Valid channels: %d") % len([c for c in channels if c.get('url')]),
                _("Groups detected: %d") % len(set(c.get('group', '') for c in channels)),
                "",
                _("Press OK to confirm or Cancel to abort.")
            ]

            # Show preview of first 5 channels
            sample = [c['name'] for c in channels[:5]]
            if len(channels) > 5:
                sample.append("...")
            summary.extend(["", _("Sample channels:")] + sample)

            self.session.openWithCallback(
                self._start_conversion_if_confirmed,
                MessageBox,
                "\n".join(summary),
                MessageBox.TYPE_YESNO
            )
        except Exception as e:
            raise RuntimeError(_("Summary generation error: %s") % str(e))

    def _start_conversion_if_confirmed(self, confirmed):
        if confirmed:
            self.start_conversion()

    def parse_file(self):
        """Main router for the different parsers"""
        if not self.selected_file:
            self.open_file()
            return

        ext = self.selected_file.lower().split('.')[-1]

        if ext in ('m3u', 'm3u8'):
            self.parse_m3u()
        elif ext == 'json':
            self.parse_json()
        elif ext == 'xspf':
            self.parse_xspf()
        else:
            self.show_error(_("Unsupported file type"))

    def open_file_browser(self):
        path = config.plugins.m3uconverter.lastdir.value
        if not path or not isdir(path):
            path = "/media/hdd" if isdir("/media/hdd") else "/tmp"

        pattern = r"(?i)^.*\.tv$" if self.conversion_type != "m3u_to_tv" else r"(?i)^.*\.m3u$"
        try:
            self.session.openWithCallback(
                self.file_selected,
                M3UFileBrowser,
                path,
                matchingPattern=pattern,
                conversion_type=self.conversion_type
            )
        except Exception as e:
            logger.log("ERROR", f"Error opening file browser: {str(e)}")
            self.session.open(MessageBox, _("Unable to open file browser."), MessageBox.TYPE_ERROR)

    def start_conversion(self):
        self.converter.cleanup_old_backups(max_backups=3)
        if self.conversion_type == "m3u_to_tv":
            self.update_path()
            self.convert_m3u_to_tv()
        else:
            self.convert_tv_to_m3u()

    def convert_m3u_to_tv(self):

        def _real_conversion_task():
            try:
                groups = {}
                total_channels = len(self.m3u_list)

                # Phase 1: Channel Grouping
                for idx, channel in enumerate(self.m3u_list):
                    if not channel.get('url'):  # Skip channels without URLs
                        continue

                    group = channel.get('group', 'Default')
                    groups.setdefault(group, []).append(channel)
                    progress = (idx + 1) / total_channels * 100
                    # self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (channel['name'], progress))
                    name = str(channel.get("name") or "--")
                    self.update_progress(idx + 1, _("Processing: %s (%d%%)") % (name, progress))

                # Phase 2: Writing bouquets
                for group_idx, (group, channels) in enumerate(groups.items()):
                    self.write_group_bouquet(group, channels)
                    self.update_progress(
                        total_channels + group_idx,
                        _("Creating bouquet: %s") % group
                    )

                # Phase 3: Main bouquet update
                self.update_main_bouquet(groups.keys())

                return (True, total_channels)
            except Exception as e:
                logger.exception(f"Error during real conversion {str(e)}")
                raise

        def conversion_task():
            try:
                return self.converter.safe_convert(_real_conversion_task)
            except Exception as e:
                logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def update_path(self):
        """Update path with all possible device locations"""
        try:
            if self.conversion_type == "tv_to_m3u":
                self.full_path = "/etc/enigma2"
                if not isdir(self.full_path):
                    self.full_path = "/tmp"
                return
            
            potential_paths = [
                config.plugins.m3uconverter.lastdir.value,
                "/media/hdd/movie",
                "/media/hdd",
                "/media/usb/movie",
                "/media/usb",
                "/media/nas/movie",
                "/media/nas",
                "/media/ba/movie",
                "/media/ba",
                "/media/net/movie",
                "/media/net",
                "/hdd/movie",
                "/hdd",
                "/autofs/movie",
                "/autofs",
                "/tmp"
            ]
            
            for path in potential_paths:
                if path and isdir(path):
                    self.full_path = path
                    break
            else:
                self.full_path = "/tmp"  # Fallback
            
            if self.full_path.split('/')[-1] != "movie" and self.full_path != "/tmp":
                movie_path = join(self.full_path, "movie")
                if not isdir(movie_path):
                    makedirs(movie_path, exist_ok=True)
                    chmod(movie_path, 0o755)
                self.full_path = movie_path
            
            logger.info(f"Selected storage path: {self.full_path}")

        except Exception as e:
            logger.error(f"Path update failed: {str(e)}")
            self.full_path = "/tmp"

    def open_file(self):
        logger.log("DEBUG", f"Path self.conversion_type: {self.conversion_type}")

        if self.conversion_type == "m3u_to_tv":
            self.open_file_browser()
        else:
            try:
                self.update_path()
                logger.log("DEBUG", "Conversion type: %s, Full path: %s" % (self.conversion_type, self.full_path))
                pattern = r"(?i)^.*\.tv$" if self.conversion_type == "tv_to_m3u" else r"(?i)^.*\.m3u$"
                logger.log("DEBUG", "Using pattern: %s" % pattern)
                self.session.openWithCallback(
                    self.file_selected,
                    M3UFileBrowser,
                    self.full_path,
                    matchingPattern=pattern,
                    conversion_type=self.conversion_type
                )
            except Exception as e:
                logger.log("ERROR", "Browser error: %s" % str(e))
                self.session.open(
                    MessageBox,
                    _("Error browser file:\n%s") % str(e),
                    MessageBox.TYPE_ERROR
                )

    def file_selected(self, res=None):
        logger.log("INFO", f"Callback file_selected: {str(res)}")

        if not res:
            self["status"].setText(_("No files selected"))
            return

        # Directly use the full path
        if isinstance(res, (tuple, list)):
            if len(res) == 0:
                self["status"].setText(_("No files selected"))
                return
            selected_path = res[0]
        elif isinstance(res, str):
            selected_path = res
        else:
            self["status"].setText(_("Invalid selection"))
            return

        if not fileExists(selected_path):
            self["status"].setText(_("Selected file does not exist"))
            return

        try:
            selected_path = normpath(selected_path)
            self.file_loaded = True
            
            # Validate extension based on conversion type
            ext = selected_path.lower().split('.')[-1]
            valid_extensions = {
                "m3u_to_tv": ["m3u", "m3u8"],
                "tv_to_m3u": ["tv"],
                "json_to_tv": ["json"],
                "xspf_to_m3u": ["xspf"]
            }.get(self.conversion_type, [])

            if ext not in valid_extensions:
                raise ValueError(_("Invalid file type for %s conversion") % self.conversion_type)

            # Save current directory
            config_dir = dirname(selected_path)
            if isdir(config_dir):
                config.plugins.m3uconverter.lastdir.value = config_dir
                config.plugins.m3uconverter.lastdir.save()

            self.selected_file = selected_path
            self.update_green_button()

            # Parse selected file based on conversion type
            parser_methods = {
                "m3u_to_tv": self.parse_m3u,
                "tv_to_m3u": self.parse_tv,
                "json_to_tv": self.parse_json,
                "xspf_to_m3u": self.parse_xspf
            }
            
            if self.conversion_type in parser_methods:
                parser_methods[self.conversion_type](selected_path)
            else:
                raise ValueError(_("Unsupported conversion type"))

        except Exception as e:
            logger.log("ERROR", f"Error file selected: {str(e)}", exc_info=True)
            self.file_loaded = False
            self.update_green_button()
            self["status"].setText(_("Error: %s") % str(e))
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR)

    def parse_m3u(self, filename=None):
        """Analyze M3U files with advanced attribute management"""
        try:
            logger.log("INFO", f"Parsing M3U: {filename}")
            with open(filename, 'r', encoding='utf-8', errors='replace') as f:
                data = f.read()

            self.m3u_list = []
            self.filename = filename

            # 1. Initial backup
            if config.plugins.m3uconverter.backup_enable.value:
                create_backup()
            channels = self._parse_m3u_content(data)
            # Advanced Attribute Mapping
            for channel in channels:
                self.m3u_list.append({
                    'name': channel.get('title', ''),
                    'group': channel.get('group-title', ''),
                    'tvg_name': channel.get('tvg-name', ''),
                    'logo': channel.get('tvg-logo', ''),
                    'url': self.process_url(channel.get('uri', '')),
                    'duration': channel.get('length', ''),
                    'user_agent': channel.get('user_agent', ''),
                    'program_id': channel.get('program-id', '')
                })

            # UI Update
            display_list = []
            for c in self.m3u_list:
                group = c['group']
                if group:
                    name = sub(r'\[.*?\]', '', c['name']).strip()

                else:
                    name = sub(r'\[.*?\]', '', c['name']).strip()

                if group:
                    display_list.append(group + " - " + name)
                else:
                    display_list.append(name)

            self["list"].setList(display_list)
            self["status"].setText(_("Loaded %d channels. Are you ready to convert? Press Green to proceed.") % len(self.m3u_list))

        except Exception as e:
            logger.log("ERROR", f"Error Parsing M3U: {str(e)}")
            self.session.open(
                MessageBox,
                _("Invalid file format:\n%s") % str(e),
                MessageBox.TYPE_ERROR
            )

    def update_channel_list(self):
        """Aggiorna la UI con la lista canali"""
        display_list = []
        for idx, channel in enumerate(self.m3u_list[:200]):  # Mostra max 200 elementi
            name = sub(r'\[.*?\]', '', channel.get('name', '')).strip()
            group = channel.get('group', '')
            display_list.append(f"{idx + 1: 03d}. {group + ' - ' if group else ''}{name}")

        self["list"].setList(display_list)
        self["status"].setText(_("Loaded %d channels") % len(self.m3u_list))

    def _parse_m3u_content(self, data):
        """Advanced parser for M3U content"""

        def get_attributes(txt, first_key_as_length=False):
            attribs = {}
            current_key = ''
            current_value = ''
            parse_state = 0  # 0=key, 1=value
            txt = txt.strip()

            for char in txt:
                if parse_state == 0:
                    if char == '=':
                        parse_state = 1
                        if first_key_as_length and not attribs:
                            attribs['length'] = current_key.strip()
                            current_key = ''
                        else:
                            current_key = current_key.strip()
                    else:
                        current_key += char
                elif parse_state == 1:
                    if char == '"':
                        if current_value:
                            attribs[current_key] = current_value
                            current_key = ''
                            current_value = ''
                            parse_state = 0
                        else:
                            parse_state = 2
                    else:
                        current_value += char
                elif parse_state == 2:
                    if char == '"':
                        attribs[current_key] = current_value
                        current_key = ''
                        current_value = ''
                        parse_state = 0
                    else:
                        current_value += char

            return attribs

        entries = []
        current_params = {}
        data = data.replace("\r\n", "\n").replace("\r", "\n").split("\n")

        for line in data:
            line = line.strip()
            if not line:
                continue

            if line.startswith('#EXTINF:'):
                current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                parts = line[8:].split(',', 1)
                if len(parts) > 1:
                    current_params['title'] = parts[1].strip()
                attribs = get_attributes(parts[0], first_key_as_length=True)
                current_params.update(attribs)

            elif line.startswith('#EXTGRP:'):
                current_params['group-title'] = line[8:].strip()

            elif line.startswith('#EXTVLCOPT:'):
                opts = line[11:].split('=', 1)
                if len(opts) == 2:
                    key = opts[0].lower().strip()
                    value = opts[1].strip()
                    if key == 'http-user-agent':
                        current_params['user_agent'] = value
                    elif key == 'program':
                        current_params['program-id'] = value

            elif line.startswith('#'):
                continue

            else:
                if current_params.get('title'):
                    current_params['uri'] = line.strip()
                    entries.append(current_params)
                    current_params = {}

        return entries

    def process_url(self, url):
        """Process URLs based on settings"""
        url = url.replace(":", "%3a")
        if config.plugins.m3uconverter.hls_convert.value:
            if any(url.lower().endswith(x) for x in ('.m3u8', '.stream')):
                url = f"hls://{url}"
        return url

    def convert_hls(self, url):
        """Convert HLS URLs"""
        parsed = urlparse(url)
        if parsed.path.lower().endswith(('.m3u8', '.stream')):
            return f"hls://{url}"
        return url

    def update_main_bouquet(self, groups):
        """Update the main bouquet file with generated group bouquets"""
        main_file = "/etc/enigma2/bouquets.tv"
        existing = []

        if exists(main_file):
            with open(main_file, "r", encoding="utf-8") as f:
                existing = f.readlines()

        if config.plugins.m3uconverter.backup_enable.value:
            create_backup()

        new_lines = []
        for group in groups:
            safe_name = self.get_safe_filename(group)
            bouquet_path = "userbouquet." + safe_name + ".tv"
            line_to_add = '#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "' + bouquet_path + '" ORDER BY bouquet\n'

            if line_to_add not in existing and line_to_add not in new_lines:
                new_lines.append(line_to_add)

        # Decide if add at top or bottom based on config option
        if config.plugins.m3uconverter.bouquet_position.value == "top":
            new_content = new_lines + existing
        else:
            new_content = existing + new_lines

        with open(main_file, "w", encoding="utf-8") as f:
            f.writelines(new_content)

        if config.plugins.m3uconverter.auto_reload.value:
            reload_services()

    def write_group_bouquet(self, group, channels):
        """
        Writes a bouquet file for a single group safely and efficiently,
        handling cirillic characters by transliterating names and saving
        the file in latin-1 encoding for Enigma2 compatibility.
        """
        try:
            safe_name = self.get_safe_filename(group)
            filename = join("/etc/enigma2", "userbouquet." + safe_name + ".tv")
            temp_file = filename + ".tmp"

            # Transliterate group name and provide fallback
            name_bouquet = transliterate(group).capitalize()
            if not name_bouquet.strip():
                match = search(r"([^/\\]+)\.m3u$", self.selected_file, flags=DOTALL)
                if match:
                    name_bouquet = transliterate(match.group(1)).capitalize()
                else:
                    name_bouquet = "Unnamed Group"

            # Add marker and main bouquet name
            markera = "#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | M3UConverter | ---"
            markerb = "#DESCRIPTION --- | M3UConverter | ---"

            with open(temp_file, "w", encoding="latin-1", errors="replace") as f:
                f.write("#NAME " + name_bouquet + "\n")
                f.write("#NAME {}\n".format(name_bouquet if name_bouquet else group))
                f.write(markera + "\n")
                f.write(markerb + "\n")
                for idx, ch in enumerate(channels, 1):
                    f.write("#SERVICE 4097:0:1:0:0:0:0:0:0:0:" + ch["url"] + "\n")
                    desc = transliterate(ch["name"])
                    f.write("#DESCRIPTION " + desc + "\n")

                    if idx % 50 == 0:
                        f.flush()
                        fsync(f.fileno())

            replace(temp_file, filename)
            chmod(filename, 0o644)

        except Exception as e:
            if exists(temp_file):
                try:
                    remove(temp_file)
                except Exception as cleanup_error:
                    logger.log("ERROR", f"Cleanup error: {str(cleanup_error)}")

            logger.log("ERROR", f"Failed to write bouquet {str(group)} : {str(e)}")
            raise RuntimeError("Bouquet creation failed for " + group) from e

    def get_safe_filename(self, group):
        """Generate a secure file name for bouquets with a suffix for identification"""
        normalized = unicodedata.normalize("NFKD", group)
        safe_name = normalized.encode('ascii', 'ignore').decode('ascii')  # Remove accents and non-ASCII
        safe_name = sub(r'[^a-z0-9_-]', '_', safe_name.lower())           # Replace unwanted chars
        safe_name = sub(r'_+', '_', safe_name).strip('_')                 # Normalize underscores

        # Add a suffix to mark the bouquet as created by M3UConverter
        suffix = "_m3ubouquet"
        base_name = safe_name[:50 - len(suffix)] if len(safe_name) > 50 - len(suffix) else safe_name

        return base_name + suffix if base_name else "my_m3uconverter" + suffix

    # TV to M3U Conversion Methods
    def parse_tv(self, filename):
        try:
            channels = []
            with codecs.open(filename, "r", encoding="utf-8") as f:
                content = f.read()
                # More robust pattern that handles different spacing and optional fields
                pattern = r'#SERVICE\s(?:4097|5002):[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:(.*?)\s*\n#DESCRIPTION\s*(.*?)\s*\n'
                matches = findall(pattern, content, DOTALL)

                for service, name in matches:
                    # URL decoding and filtering HTTP/HTTPS/HLS streams
                    url = unquote(service.strip())
                    if any(url.startswith(proto) for proto in ('http://', 'https://', 'hls://')):
                        channels.append((name.strip(), url))

            if not channels:
                raise ValueError(_("No IPTV channels found in the bouquet"))

            self.m3u_list = channels
            self["list"].setList([c[0] for c in channels])
            self["status"].setText(_("Loaded %d channels. Are you ready to convert? Press Green to proceed.") % len(channels))
        except Exception as e:
            self.show_error(_("Error parsing bouquet: %s") % str(e))

    def convert_tv_to_m3u(self):

        def _real_tv_to_m3u_conversion():
            try:
                output_file = self._get_output_filename()
                total_items = len(self.m3u_list)

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {currversion}\n')

                    for idx, (name, url) in enumerate(self.m3u_list):
                        f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                        f.write(f'{url}\n')

                        if idx % 10 == 0:
                            progress = (idx + 1) / total_items * 100
                            self.update_progress(
                                idx + 1,
                                _("Exporting: %s (%d%%)") % (name, int(progress))
                            )

                return (True, output_file)
            except IOError as e:
                raise RuntimeError(_("File write error: %s") % str(e))
            except Exception as e:
                raise RuntimeError(_("Conversion error: %s") % str(e))

        def conversion_task():
            try:
                return self.converter.safe_convert(_real_tv_to_m3u_conversion)
            except Exception as e:
                logger.error(f"TV-to-M3U conversion failed: {str(e)}")
                return (False, str(e))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _get_output_filename(self):
        """Generate a unique file name for export"""
        timestamp = strftime("%Y%m%d_%H%M%S")
        return f"/tmp/archimede_export_{timestamp}.m3u"

    def parse_json(self, filename=None):
        file_to_parse = filename or self.selected_file
        try:
            with open(file_to_parse, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.m3u_list = []
            for group in data.get('groups', []):
                for channel in group.get('channels', []):
                    self.m3u_list.append({
                        'name': channel.get('name', ''),
                        'url': channel.get('url', ''),
                        'group': group.get('name', ''),
                        'logo': channel.get('logo', '')
                    })

            self.update_channel_list()
        except Exception as e:
            self.show_error(_("Error parsing JSON: %s") % str(e))

    def parse_xspf(self, filename=None):
        file_to_parse = filename or self.selected_file
        try:
            from xml.etree import ElementTree as ET
            tree = ET.parse(file_to_parse)
            root = tree.getroot()
            ns = {'ns': 'http://xspf.org/ns/0/'}

            self.m3u_list = []
            for track in root.findall('.//ns:track', ns):
                name = track.find('ns:title', ns)
                url = track.find('ns:location', ns)
                if name is not None and url is not None:
                    self.m3u_list.append({
                        'name': name.text,
                        'url': url.text,
                        'group': 'XSPF Import'
                    })

            self.update_channel_list()
        except Exception as e:
            self.show_error(_("Error parsing XSPF: %s") % str(e))

    def convert_json_to_tv(self):
        """New method for converting from JSON to bouquet"""
        def _json_conversion_task():
            try:
                with open(self.selected_file, 'r') as f:
                    data = json.load(f)

                channels = []
                for group in data.get('groups', []):
                    for channel in group.get('channels', []):
                        channels.append({
                            'name': channel.get('name', ''),
                            'url': channel.get('url', ''),
                            'group': group.get('name', '')
                        })

                self.m3u_list = channels
                return self.convert_m3u_to_tv()

            except json.JSONDecodeError:
                raise RuntimeError(_("Invalid JSON file"))
            except Exception as e:
                raise RuntimeError(_("JSON processing error: %s") % str(e))

        threads.deferToThread(
            self.converter.safe_convert(_json_conversion_task)
        ).addBoth(self.conversion_finished)

    def convert_xspf_to_m3u(self):
        """Convert XSPF (XML) to M3U"""
        def _xspf_conversion():
            try:
                from xml.etree import ElementTree as ET
                tree = ET.parse(self.selected_file)
                root = tree.getroot()

                output_file = self._get_output_filename()
                ns = {'ns': 'http://xspf.org/ns/0/'}

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')

                    for track in root.findall('.//ns:track', ns):
                        title = track.find('ns:title', ns)
                        location = track.find('ns:location', ns)

                        if title is not None and location is not None:
                            f.write(f'#EXTINF:-1,{title.text}\n')
                            f.write(f'{location.text}\n')

                return (True, output_file)
            except ET.ParseError:
                raise RuntimeError(_("Invalid XSPF file"))
            except Exception as e:
                raise RuntimeError(_("XSPF conversion error: %s") % str(e))

        threads.deferToThread(
            self.converter.safe_convert(_xspf_conversion)
        ).addBoth(self.conversion_finished)

    def conversion_finished(self, result):
        self["progress_source"].setValue(0)
        success, data = result
        msg = ''
        if success:
            msg = _("Successfully converted %d items") % data if isinstance(data, int) else _("File saved to: %s") % data
            self.show_info(msg)
            self["progress_source"].setValue(0)
        else:
            msg = _("Conversion failed: %s") % data
            self.show_error(msg)
            self["progress_source"].setValue(0)

        self["status"].setText(msg)
        self["progress_text"].setText("")

    def update_progress(self, value, text):
        from twisted.internet import reactor
        reactor.callFromThread(self._update_progress_ui, value, text)

    def _update_progress_ui(self, value, text):
        self.progress_source.setRange(len(self.m3u_list) if self.m3u_list else 100)
        self.progress_source.setValue(value)
        self["progress_text"].setText(text)

    def open_settings(self):
        self.session.open(M3UConverterSettings)

    def key_ok(self):
        index = self["list"].getSelectedIndex()
        if index < 0 or index >= len(self.m3u_list):
            self["status"].setText(_("No item selected"))
            return

        item = self.m3u_list[index]

        if self.conversion_type == "tv_to_m3u":
            # If loaded from .tv (tuple)
            if isinstance(item, tuple) and len(item) == 2:
                name, url = item
                url = unquote(url.strip())
            else:
                self["status"].setText(_("Invalid .tv item format"))
                return
        else:
            # Default: assume it's from .m3u (dict)
            if isinstance(item, dict):
                name = item.get("name", "")
                url = item.get("url", "")
                url = unquote(url.strip())
            else:
                self["status"].setText(_("Invalid .m3u item format"))
                return

        self["status"].setText(_("Playing: %s") % name)
        self.start_player(name, url)

    def start_player(self, name, url):
        stream = eServiceReference(4097, 0, url)
        stream.setName(name)
        logger.log("DEBUG", f"Extracted video URL: {str(stream)}")
        self.session.nav.stopService()
        self.session.nav.playService(stream)

    def stop_player(self):
        self.session.nav.stopService()
        self.session.nav.playService(self.initialservice)
        self["status"].setText(_("Ready"))

    def on_movieplayer_exit(self, result=None):
        self.session.nav.stopService()
        self.session.nav.playService(self.initialservice)
        aspect_manager.restore_aspect()
        self.close()

    def keyClose(self, result=None):
        try:
            self.session.nav.stopService()
            self.session.nav.playService(self.initialservice)
            aspect_manager.restore_aspect()
            self.close()
        except Exception as e:
            print('error:', str(e))

    def show_plugin_info(self):
        """Show plugin information and credits"""
        info = [
            f"Archimede Universal Converter v.{currversion}",
            _("Author: Lululla"),
            _("License: CC BY-NC-SA 4.0"),
            _("Developed for Enigma2"),
            _(f"Last modified: {last_date}"),
            "",
            _("Press OK to close")
        ]
        self.session.open(
            MessageBox,
            "\n".join(info),
            MessageBox.TYPE_INFO
        )

    def show_info(self, message):
        self.session.open(MessageBox, message, MessageBox.TYPE_INFO)
        self["status"].setText(message)

    def show_error(self, message):
        self.session.open(MessageBox, message, MessageBox.TYPE_ERROR)
        self["status"].setText(message)


class M3UConverterSettings(Setup):
    def __init__(self, session, parent=None):
        Setup.__init__(self, session, setup="M3UConverterSettings", plugin="Extensions/M3UConverter")
        self.parent = parent

    def keySave(self):
        Setup.keySave(self)


def main(session, **kwargs):
    core_converter.cleanup_old_backups(config.plugins.m3uconverter.max_backups.value)

    def on_conversion_selected(conversion_type=None):
        if conversion_type:
            session.openWithCallback(
                lambda res: session.open(UniversalConverter, conversion_type=conversion_type, selected_file=res),
                M3UFileBrowser,
                startdir="/media/hdd" if isdir("/media/hdd") else "/tmp",
                matchingPattern=get_pattern_for_type(conversion_type),
                conversion_type=conversion_type
            )

    session.openWithCallback(on_conversion_selected, ConversionSelector)


def get_pattern_for_type(conversion_type):
    patterns = {
        "m3u_to_tv": r"(?i)^.*\.(m3u|m3u8)$",
        "tv_to_m3u": r"(?i)^.*\.tv$",
        "json_to_tv": r"(?i)^.*\.json$",
        "xspf_to_m3u": r"(?i)^.*\.xspf$"
    }
    return patterns.get(conversion_type, r".*")


def Plugins(**kwargs):
    from Plugins.Plugin import PluginDescriptor
    return [PluginDescriptor(
        name=_("Universal Converter"),
        description=_("Convert between M3U and Enigma2 bouquets"),
        where=PluginDescriptor.WHERE_PLUGINMENU,
        icon="plugin.png",
        fnc=main)
    ]
