# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 3.0                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "20:05 - 20251102"                    #
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

# ======================== IMPORTS ========================
# 🧠 STANDARD LIBRARIES (Python built-ins)
import unicodedata
from re import IGNORECASE, sub
from os import access, remove, W_OK
from os.path import dirname, exists, isdir, join

# 📺 ENIGMA2 CORE
from enigma import eTimer
try:
    from enigma import AVSwitch
except ImportError:
    from Components.AVSwitch import AVSwitch

# 🧩 ENIGMA2 COMPONENTS
from Components.ActionMap import ActionMap
from Components.FileList import FileList
from Components.config import config
from Screens.Screen import Screen
from Tools.Directories import defaultRecordingLocation

# 🧱 LOCAL MODULES
from . import _
from .Logger_clr import get_logger
from .version import CURRENT_VERSION


# ==================== UTILITY FUNCTIONS A ====================
def get_best_storage_path():
    """Find writable storage from mount points"""
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.readlines()

        for mount in mounts:
            if '/media/' in mount:
                parts = mount.split()
                mount_point = parts[1]
                try:
                    test_file = join(mount_point, "test.tmp")
                    with open(test_file, 'w') as f:
                        f.write("test")
                    remove(test_file)
                    print(f"✅ Mount OK: {mount_point}")
                    return mount_point
                except:
                    continue
    except:
        pass

    return "/tmp/"


PLUGIN_TITLE = _("Archimede Universal Converter v.%s by Lululla") % CURRENT_VERSION
PLUGIN_PATH = dirname(__file__)
BASE_STORAGE_PATH = get_best_storage_path()
ARCHIMEDE_M3U_PATH = join(BASE_STORAGE_PATH, "movie")
ARCHIMEDE_CONVERTER_PATH = join(BASE_STORAGE_PATH, "archimede_converter")
LOG_DIR = ARCHIMEDE_CONVERTER_PATH
DEBUG_DIR = join(ARCHIMEDE_CONVERTER_PATH, "debug")

# ==================== LOGGER ====================
logger = get_logger(
    log_path=LOG_DIR,
    plugin_name="M3U_CONVERTER",
    clear_on_start=True,
    max_size_mb=0.5
)


# ==================== UTILITY FUNCTIONS B ====================

def get_mounted_devices():
    """Get list of mounted and writable devices."""
    basic_paths = [
        ("/media/hdd/", _("HDD Drive")),
        ("/media/usb/", _("USB Drive")),
        ("/media/ba/", _("Barry Allen")),
        ("/media/net/", _("Network Storage")),
        ("/tmp/", _("Temporary"))
    ]

    # Check which paths exist and are writable
    valid_devices = []
    for path, desc in basic_paths:
        if isdir(path) and access(path, W_OK):
            valid_devices.append((path, desc))

    # Add additional USB devices if available (usb1, usb2...)
    for i in range(1, 4):
        usb_path = "/media/usb%d/" % i
        if isdir(usb_path) and access(usb_path, W_OK):
            valid_devices.append((usb_path, _("USB Drive") + " %d" % i))

    return valid_devices


def default_movie_path():
    """Get default movie path from Enigma2 configuration."""
    result = config.usage.default_path.value
    if not result.endswith("/"):
        result += "/"
    if not isdir(result):
        return defaultRecordingLocation(config.usage.default_path.value)
    return result


def update_mounts_configuration():
    """Update the list of mounted devices and update config choices."""
    mounts = get_mounted_devices()
    if not mounts:
        default_path = default_movie_path()
        mounts = [(default_path, default_path)]
    config.plugins.m3uconverter.lastdir.setChoices(mounts, default=mounts[0][0])
    config.plugins.m3uconverter.lastdir.save()


def clean_group_name(name):
    """Clean group names preserving accented characters."""
    if not name:
        return "Default"

    cleaned = name.strip()
    cleaned = sub(r'^\s*\|[A-Z]+\|\s*', '', cleaned)
    cleaned = sub(r'^\s*[A-Z]{2}:\s*', '', cleaned)
    cleaned = sub(r'^\s*(IT|UK|FR|DE|ES|NL|PL|GR|CZ|HU|RO|SE|NO|DK|FI|NOW)\s+', '', cleaned, flags=IGNORECASE)
    cleaned = sub(r'[^\w\s\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', '', cleaned)
    cleaned = ' '.join(cleaned.split())

    if len(cleaned) > 40:
        cleaned = cleaned[:40]

    return cleaned or "Default"


def transliterate_text(text):
    """Convert accented characters to ASCII equivalents."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode('ascii', 'ignore').decode('ascii')


def create_bouquets_backup():
    """Create a backup of bouquets.tv only."""
    from shutil import copy2
    src = "/etc/enigma2/bouquets.tv"
    dst = "/etc/enigma2/bouquets.tv.bak"
    if exists(src):
        copy2(src, dst)


def _reload_services_after_delay(delay=4000):
    """Reload Enigma2 bouquets and service lists"""
    try:
        def do_reload():
            try:
                from enigma import eDVBDB
                db = eDVBDB.getInstance()
                db.reloadServicelist()
                db.reloadBouquets()
            except Exception as e:
                logger.error(f"Service reload error: {str(e)}")

        reload_timer = eTimer()
        reload_timer.callback.append(do_reload)
        reload_timer.start(delay, True)

    except Exception as e:
        logger.error(f"Error setting up service reload: {str(e)}")
        try:
            system("wget -qO - http://127.0.0.1/web/servicelistreload > /dev/null 2>&1")
            logger.info("Bouquets reloaded via web interface")
            return True
        except:
            logger.error("All reload methods failed")
            return False

class M3UFileBrowser(Screen):
    """File browser screen for selecting M3U, TV, JSON, and XSPF files."""

    def __init__(self, session, startdir="/etc/enigma2",
                 matchingPattern=r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$",
                 conversion_type=None, title=None):

        Screen.__init__(self, session)
        if title:
            self.setTitle(title)
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
            "ok": self._on_ok_pressed,
            "green": self._on_ok_pressed,
            "cancel": self.close
        }, -1)
        if self.conversion_type == "tv_to_m3u" or self.conversion_type == "tv_to_tv":
            self._filter_file_list()

    def _filter_file_list(self):
        """Filter list to show only directories and .tv files containing 'http'."""
        filtered_entries = []
        for entry in self["filelist"].list:
            if not entry or not isinstance(entry[0], tuple):
                continue

            file_data = entry[0]
            path = None
            is_directory = False

            if len(file_data) >= 2:
                path = file_data[0]
                is_directory = file_data[1]

            elif len(file_data) == 1 and isinstance(file_data[0], str):
                path = file_data[0]
                is_directory = True
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Skipping invalid entry: {file_data}")
                continue

            if path == ".." or is_directory:
                filtered_entries.append(entry)
            else:
                if path and path.lower().endswith(".tv") and self._file_contains_http(path):
                    filtered_entries.append(entry)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"✅ Added TV file: {path}")

        self["filelist"].list = filtered_entries
        self["filelist"].l.setList(filtered_entries)

    def _file_contains_http(self, filename):
        """Check if file contains 'http' (case-insensitive)."""
        try:
            current_directory = self["filelist"].getCurrentDirectory()
            full_path = join(current_directory, filename)

            with open(full_path, "r") as f:
                return any("http" in line.lower() for line in f)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error reading {full_path}: {str(e)}")
            return False

    def _on_ok_pressed(self):
        """Handle OK button press for file selection."""
        selection = self["filelist"].getCurrent()
        if not selection or not isinstance(selection, list) or not isinstance(selection[0], tuple):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Invalid selection format: {selection}")
            return

        file_data = selection[0]
        path = file_data[0]
        is_directory = file_data[1]

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"file_data: {file_data}, path: {path}, is_directory: {is_directory}")
        try:
            if is_directory:
                self["filelist"].changeDir(path)
                if self.conversion_type == "tv_to_m3u" or self.conversion_type == "tv_to_tv":
                    self._filter_file_list()
            else:
                current_directory = self["filelist"].getCurrentDirectory()
                full_path = join(current_directory, path)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Selected full file path: {full_path}")
                self.close(full_path)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error in OK pressed: {str(e)}")

    def close(self, result=None):
        """Close the file browser."""
        try:
            super(M3UFileBrowser, self).close(result)
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error closing browser: {str(e)}")
            super(M3UFileBrowser, self).close(None)


class AspectManager:
    """Manage aspect ratio settings for video playback."""
    def __init__(self):
        self.init_aspect = self.get_current_aspect()
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Initial aspect ratio: {self.init_aspect}")

    def getAspectRatioSetting(self):
        """
        Map Enigma2 config.av.aspectratio string values to integer codes.
        If no mapping exists, fall back to the raw config value.
        """
        aspect_map = {
            "4_3_letterbox": 0,
            "4_3_panscan": 1,
            "16_9": 2,
            "16_9_always": 3,
            "16_10_letterbox": 4,
            "16_10_panscan": 5,
            "16_9_letterbox": 6,
        }

        val = config.av.aspectratio.value
        return aspect_map.get(val, val)

    def set_aspect_for_video(self, aspect=None):
        """Temporarily set an aspect ratio for video playback."""
        try:
            if aspect is None:
                aspect = 2
            print("[INFO] Imposto aspect ratio a:", aspect)
            AVSwitch().setAspectRatio(aspect)
        except Exception as e:
            logger.error(f"Failed to set aspect ratio: {str(e)}")

    def get_current_aspect(self):
        """Get the current aspect ratio setting.
        Returns:
            int: Current aspect ratio setting
        """
        try:
            return self.getAspectRatioSetting()
        except Exception as e:
            logger.error(f"Failed to get aspect ratio: {str(e)}")
            return 0

    def restore_aspect(self):
        """Restores the original aspect ratio when the plugin exits."""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Restoring aspect ratio to: {self.init_aspect}")
            AVSwitch().setAspectRatio(self.init_aspect)
        except Exception as e:
            logger.error(f"Failed to restore aspect ratio: {str(e)}")
